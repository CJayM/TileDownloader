"""Клиент-рабочий: получает задачи с сервера, скачивает тайлы, сдаёт их.

Главный цикл (plan.md §7):
  * досдача остатков локального буфера при старте;
  * получение задачи, скачивание диапазона (N потоков, ретраи) в буфер;
  * сабмит бинарным фреймом; фоновый heartbeat на всё время задачи;
  * повтор при task: null, выход при done.
"""

import argparse
import asyncio
import os
import socket
import sys
import time

import aiohttp

import buffer
import protocol
import utils

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)


class ServerAPI:
    """Тонкая обёртка над aiohttp.ClientSession."""

    def __init__(self, server_url, client_id, session=None):
        self.server_url = server_url.rstrip('/')
        self.client_id = client_id
        self._session = session
        self._owns = session is None

    async def _ensure(self):
        if self._session is None:
            # total=None — сабмит может передавать ~100 МБ долго
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=None))
            self._owns = True
        return self._session

    async def close(self):
        if self._session is not None and self._owns:
            await self._session.close()
            self._session = None

    @staticmethod
    async def _read_json(resp):
        """JSON-ответ; при не-JSON (413 aiohttp, страница ошибки) — словарик
        ошибки, а не исключение: долгий клиент не должен падать с трейсбека,
        а просто счесть задачу несданной и получить её заново."""
        try:
            return await resp.json(content_type=None)
        except (aiohttp.client_exceptions.ContentTypeError, ValueError):
            text = (await resp.text())[:200]
            return {'ok': False, 'error': f'HTTP {resp.status}: {text}',
                    'status': 'error'}

    async def get_task(self):
        session = await self._ensure()
        url = f'{self.server_url}/api/task?client_id={self.client_id}'
        async with session.get(url) as resp:
            return await self._read_json(resp)

    async def submit(self, task_id, zoom, tiles):
        """tiles: список (x, y, ext, data). Кодирует фрейм и отправляет."""
        body = protocol.encode_submit(zoom, tiles)
        session = await self._ensure()
        url = f'{self.server_url}/api/tasks/{task_id}/submit?client_id={self.client_id}'
        async with session.post(url, data=body) as resp:
            return await self._read_json(resp)

    async def heartbeat(self, task_id):
        session = await self._ensure()
        url = f'{self.server_url}/api/tasks/{task_id}/heartbeat?client_id={self.client_id}'
        async with session.post(url) as resp:
            return await self._read_json(resp)


async def download_range(fetcher, zoom, start, end, threads, put_cb,
                         retry_delay=30, sleep=asyncio.sleep, max_retries=3):
    """Параллельная загрузка тайлов [start, end) с ретраями.

    fetcher(x, y, zoom) -> (data_bytes, ext); бросает исключение при сбое.
    put_cb(x, y, data, ext) — запись в буфер (корутина).
    Возвращает (tiles, failures): список (x, y, ext, data) и число сбоев.
    """
    sem = asyncio.Semaphore(threads)
    results = []
    failures = [0]
    lock = asyncio.Lock()

    async def work(idx):
        x, y = utils.get_xy(idx, zoom)
        async with sem:
            data = None
            ext = 'png'
            for attempt in range(max_retries):
                try:
                    data, ext = await fetcher(x, y, zoom)
                    break
                except Exception:
                    data = None
                    if attempt < max_retries - 1:
                        await sleep(retry_delay)
            if data is None:
                async with lock:
                    failures[0] += 1
                return
            await put_cb(x, y, data, ext)
            async with lock:
                results.append((x, y, ext, data))

    await asyncio.gather(*[work(i) for i in range(start, end)])
    return results, failures[0]


async def run(api, buf, fetcher, threads=16, heartbeat_interval=300,
              retry_delay=30, sleep=asyncio.sleep, poll_delay=2,
              once=False, print_fn=print):
    """Главный цикл. Возвращает 0 при done (или --once)."""

    # Досдача остатков буфера прошлой сессии.
    meta = buf.load_meta()
    if meta is not None and buf.count_for_task(meta['task_id']) > 0:
        rows = buf.all_for_task(meta['task_id'])
        tiles = [(x, y, ext, image) for (z, x, y, image, ext) in rows]
        res = await api.submit(meta['task_id'], meta['zoom'], tiles)
        buf.clear_task(meta['task_id'])
        if res.get('status') != 'submitted':
            print_fn(f"Остатки задачи {meta['task_id']}: {res.get('status')} (буфер сброшен)")

    while True:
        res = await api.get_task()
        task = res.get('task')
        if task is None:
            if res.get('done'):
                print_fn('Все тайлы скачаны')
                return 0
            await sleep(poll_delay)
            continue

        task_id = task['task_id']
        zoom = task['zoom']
        start, end = task['start_idx'], task['end_idx']
        buf.save_task_meta(task_id, zoom, start, end - start)
        t0 = time.time()

        # Фоновый heartbeat на всё время задачи (скачивание + сабмит).
        hb_task = None
        if heartbeat_interval > 0:
            async def heartbeat_loop(tid=task_id):
                while True:
                    await sleep(heartbeat_interval)
                    try:
                        await api.heartbeat(tid)
                    except Exception:
                        pass
            hb_task = asyncio.create_task(heartbeat_loop())

        async def put_cb(x, y, data, ext, z=zoom, tid=task_id):
            buf.put(z, x, y, data, ext, tid)

        tiles, failures = await download_range(
            fetcher, zoom, start, end, threads, put_cb,
            retry_delay=retry_delay, sleep=sleep)

        res = await api.submit(task_id, zoom, tiles)

        if hb_task is not None:
            hb_task.cancel()
            try:
                await hb_task
            except asyncio.CancelledError:
                pass

        buf.clear_task(task_id)

        if res.get('status') != 'submitted':
            print_fn(f"Задача {task_id}: {res.get('status')} (тайлы будут выданы снова)")

        elapsed = time.time() - t0
        tps = len(tiles) / elapsed if elapsed > 0 else 0.0
        print_fn(f"Задача {task_id} z{zoom} [{start}:{end}]: "
                 f"{len(tiles)} тайлов, {failures} сбоев, TPS {tps:.0f}")

        if once:
            return 0


def main():
    parser = argparse.ArgumentParser(description='Tile Downloader client')
    parser.add_argument('--server', default='http://127.0.0.1:8080')
    parser.add_argument('--client-id', default=socket.gethostname())
    parser.add_argument('--buffer-dir', default='./buffer')
    parser.add_argument('--threads', type=int, default=16)
    parser.add_argument('--tile-url',
                        default='https://core-sat.maps.yandex.net/tiles?l=sat&x={x}&y={y}&z={z}')
    parser.add_argument('--heartbeat-interval', type=int, default=300)
    parser.add_argument('--retry-delay', type=int, default=30)
    parser.add_argument('--once', action='store_true', help='Одна задача, выход')
    args = parser.parse_args()

    async def fetch_tile(x, y, zoom, session, url_tmpl):
        url = url_tmpl.format(x=x, y=y, z=zoom)
        async with session.get(url) as resp:
            ctype = resp.headers.get('Content-Type', '')
            ext = 'png'
            if 'jpeg' in ctype:
                ext = 'jpg'
            elif 'webp' in ctype:
                ext = 'webp'
            data = await resp.read()
            if not data:
                raise RuntimeError('empty response')
            return data, ext

    async def go():
        session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None))
        api = ServerAPI(args.server, args.client_id, session=session)
        buf = buffer.ClientBuffer(args.buffer_dir, args.client_id)

        async def fetcher(x, y, zoom):
            try:
                return await fetch_tile(x, y, zoom, session, args.tile_url)
            except (aiohttp.ClientError, RuntimeError):
                raise

        try:
            await run(api, buf, fetcher, threads=args.threads,
                      heartbeat_interval=args.heartbeat_interval,
                      retry_delay=args.retry_delay, once=args.once)
        finally:
            await session.close()
            buf.close()

    asyncio.run(go())


if __name__ == '__main__':
    main()
