"""Тесты клиента (client.py) по ТЗ §10.

FakeAPI имитирует сервер; fetcher — источник тайлов с детерминированными
байтами и счётчиком запросов; sleep — инъектируемый (счётный или реальный).
"""

import asyncio
import os
import sys

import pytest

current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

import buffer  # noqa: E402
import client  # noqa: E402
import utils   # noqa: E402


class CountSleep:
    def __init__(self):
        self.count = 0
        self.values = []

    async def __call__(self, seconds):
        self.count += 1
        self.values.append(seconds)
        await asyncio.sleep(0)


class FakeAPI:
    def __init__(self):
        self.task_queue = []
        self.done = False
        self.submitted = {}
        self.heartbeats = []

    def set_tasks(self, *tasks):
        self.task_queue.extend(tasks)

    async def get_task(self):
        if self.task_queue:
            return {'ok': True, 'task': self.task_queue.pop(0), 'done': False}
        return {'ok': True, 'task': None, 'done': self.done}

    async def submit(self, task_id, zoom, tiles):
        self.submitted[task_id] = (zoom, list(tiles))
        return {'ok': True, 'status': 'submitted', 'saved': len(tiles)}

    async def heartbeat(self, task_id):
        self.heartbeats.append(task_id)
        return {'ok': True}


def make_task(task_id, zoom, start, count):
    return {'task_id': task_id, 'zoom': zoom, 'start_idx': start,
            'end_idx': start + count, 'count': count, 'deadline': 0}


def make_fetcher(fail_map=None):
    """fail_map: {(x, y): N} — первые N запросов к тайлу падают."""
    calls = []

    async def fetcher(x, y, zoom):
        calls.append((x, y, zoom))
        if fail_map is not None and (x, y) in fail_map:
            fail_map[(x, y)] -= 1
            if fail_map[(x, y)] > 0:
                raise RuntimeError('boom')
        return bytes([x, y, zoom]), 'png'

    return fetcher, calls


async def noop_put(x, y, data, ext):
    pass


# ------------------------------------------------------------ download_range

def test_download_range_stable():
    async def main():
        fetcher, calls = make_fetcher()
        put = []

        async def put_cb(x, y, d, e):
            put.append((x, y))

        tiles, failures = await client.download_range(
            fetcher, 2, 0, 4, threads=2, put_cb=put_cb, sleep=asyncio.sleep)
        assert failures == 0
        assert sorted(t[0] for t in tiles) == [0, 1, 2, 3]
        assert sorted((x, y) for (x, y, _e, _d) in tiles) == sorted(
            utils.get_xy(i, 2) for i in range(4))
        assert len(put) == 4
        assert len(calls) == 4
    asyncio.run(main())


def test_download_range_retries_then_succeeds():
    async def main():
        # (0,0) падает дважды, на третьей попытке — успех (max_retries=3)
        fetcher, calls = make_fetcher(fail_map={(0, 0): 3})
        sl = CountSleep()
        tiles, failures = await client.download_range(
            fetcher, 2, 0, 1, threads=1, put_cb=noop_put,
            retry_delay=7, sleep=sl, max_retries=3)
        assert failures == 0
        assert len(tiles) == 1
        assert sl.count == 2          # две паузы между ретраями
        assert sl.values == [7, 7]
        assert len(calls) == 3
    asyncio.run(main())


def test_download_range_gives_up():
    async def main():
        # тайл падает всегда -> в failures, не в tiles
        fetcher, calls = make_fetcher(fail_map={(0, 0): 99})
        sl = CountSleep()
        tiles, failures = await client.download_range(
            fetcher, 2, 0, 1, threads=1, put_cb=noop_put,
            retry_delay=1, sleep=sl, max_retries=3)
        assert tiles == []
        assert failures == 1
        assert sl.count == 2          # паузы только между попытками
    asyncio.run(main())


def test_download_range_thread_limit():
    async def main():
        active = [0]
        peak = [0]

        async def fetcher(x, y, zoom):
            active[0] += 1
            peak[0] = max(peak[0], active[0])
            await asyncio.sleep(0.001)
            active[0] -= 1
            return b'd', 'png'

        await client.download_range(
            fetcher, 2, 0, 4, threads=2, put_cb=noop_put,
            sleep=asyncio.sleep)
        assert peak[0] <= 2
    asyncio.run(main())


# ----------------------------------------------------------------- run

def test_run_done_immediately(tmp_path):
    async def main():
        api = FakeAPI()
        api.done = True
        buf = buffer.ClientBuffer(str(tmp_path), 'c1')
        fetcher, calls = make_fetcher()
        sl = CountSleep()
        rc = await client.run(api, buf, fetcher, sleep=sl, poll_delay=1)
        assert rc == 0
        assert calls == []
        assert api.submitted == {}
        buf.close()
    asyncio.run(main())


def test_run_one_task_submit_and_clear(tmp_path):
    async def main():
        api = FakeAPI()
        api.set_tasks(make_task(1, 2, 0, 4))
        buf = buffer.ClientBuffer(str(tmp_path), 'c1')
        fetcher, calls = make_fetcher()
        sl = CountSleep()
        rc = await client.run(api, buf, fetcher, heartbeat_interval=0,
                              sleep=sl, poll_delay=1, once=True)
        assert rc == 0
        assert len(calls) == 4
        zoom, tiles = api.submitted[1]
        assert zoom == 2
        assert sorted((x, y) for (x, y, _e, _d) in tiles) == sorted(
            utils.get_xy(i, 2) for i in range(4))
        assert buf.count_for_task(1) == 0
        buf.close()
    asyncio.run(main())


def test_run_poll_when_no_task(tmp_path):
    async def main():
        # Первая итерация: task None, done False -> poll. Вторая: задача.
        api = FakeAPI()
        api.set_tasks(None)  # None -> get_task вернёт task None, done False
        # Обходимся напрямую: вручную задаём первую пустую выдачу.
        first = api.get_task

        async def get_once_then_task():
            r = await first()
            if r['task'] is None and not r['done']:
                api.set_tasks(make_task(1, 2, 0, 2))
            return r

        api.get_task = get_once_then_task
        buf = buffer.ClientBuffer(str(tmp_path), 'c1')
        fetcher, calls = make_fetcher()
        sl = CountSleep()
        rc = await client.run(api, buf, fetcher, heartbeat_interval=0,
                              sleep=sl, poll_delay=5, once=True)
        assert rc == 0
        assert sl.count >= 1          # был poll
        assert sl.values[0] == 5
        assert len(calls) == 2
        buf.close()
    asyncio.run(main())


def test_run_heartbeat_sent(tmp_path):
    async def main():
        api = FakeAPI()
        api.set_tasks(make_task(1, 2, 0, 2))

        async def slow_fetcher(x, y, zoom):
            await asyncio.sleep(0.02)
            return bytes([x, y, zoom]), 'png'

        buf = buffer.ClientBuffer(str(tmp_path), 'c1')
        # Реальный sleep: heartbeat_loop успевает сработать во время загрузки.
        rc = await client.run(api, buf, slow_fetcher, heartbeat_interval=0.005,
                              sleep=asyncio.sleep, poll_delay=1, once=True)
        assert rc == 0
        assert api.heartbeats  # хотя бы один heartbeat
        assert all(h == 1 for h in api.heartbeats)
        buf.close()
    asyncio.run(main())


def test_run_no_heartbeat_when_disabled(tmp_path):
    async def main():
        api = FakeAPI()
        api.set_tasks(make_task(1, 2, 0, 2))
        buf = buffer.ClientBuffer(str(tmp_path), 'c1')
        fetcher, _ = make_fetcher()
        sl = CountSleep()
        await client.run(api, buf, fetcher, heartbeat_interval=0,
                         sleep=sl, poll_delay=1, once=True)
        assert api.heartbeats == []
        buf.close()
    asyncio.run(main())


def test_run_unknown_task_clears_buffer(tmp_path):
    async def main():
        api = FakeAPI()
        api.set_tasks(make_task(1, 2, 0, 2))

        async def submit_reject(task_id, zoom, tiles):
            api.submitted[task_id] = (zoom, list(tiles))
            return {'ok': True, 'status': 'unknown_task'}

        api.submit = submit_reject
        buf = buffer.ClientBuffer(str(tmp_path), 'c1')
        fetcher, _ = make_fetcher()
        sl = CountSleep()
        logs = []
        await client.run(api, buf, fetcher, heartbeat_interval=0,
                         sleep=sl, poll_delay=1, once=True,
                         print_fn=logs.append)
        # Буфер очищен даже при отказе сервера.
        assert buf.count_for_task(1) == 0
        assert any('unknown_task' in m for m in logs)
        buf.close()
    asyncio.run(main())


def test_run_resends_leftover(tmp_path):
    async def main():
        # Предыдущая сессия оставила незаписанные тайлы задачи 7.
        buf = buffer.ClientBuffer(str(tmp_path), 'c1')
        buf.save_task_meta(7, 2, 0, 4)
        buf.put(2, 0, 0, b'd0', 'png', 7)
        buf.put(2, 1, 0, b'd1', 'png', 7)
        buf.close()

        api = FakeAPI()
        api.done = True
        buf2 = buffer.ClientBuffer(str(tmp_path), 'c1')
        fetcher, calls = make_fetcher()
        sl = CountSleep()
        rc = await client.run(api, buf2, fetcher, sleep=sl, poll_delay=1)
        assert rc == 0
        # Остаток досдан, буфер очищен, источник не тронут.
        zoom, tiles = api.submitted[7]
        assert zoom == 2
        assert sorted(x for (x, _y, _e, _d) in tiles) == [0, 1]
        assert buf2.count_for_task(7) == 0
        assert calls == []
        buf2.close()
    asyncio.run(main())
