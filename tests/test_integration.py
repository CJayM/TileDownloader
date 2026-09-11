"""Этап 5. Интеграционные сценарии: реальный JobManager + реальный aiohttp-сервер
+ «источник» тайлов (детерминированные байты + счётчик запросов) + реальный цикл
клиента (client.run).

Проверяется полный путь: выдача задачи -> скачивание с источника -> запись в
буфер клиента -> сабмит бинарным фреймом -> запись в мастер-БД.
"""

import asyncio
import os
import sqlite3

import aiohttp
import pytest
from aiohttp.test_utils import TestServer, TestClient

import buffer
import client
import db
import jobs
import server
import utils


# ------------------------------------------------------------------ helpers

class Source:
    """Фейковый источник тайлов: детерминированные байты + счётчик запросов.

    fail_map: {(zoom, x, y): N} — первые (N-1) запросов к тайлу падают,
    N-й — успех. Значение 1 — без сбоев.
    """

    def __init__(self, fail_map=None):
        self.count = 0
        self.tiles = {}
        self.fail_map = fail_map or {}

    async def fetch(self, x, y, zoom):
        self.count += 1
        key = (zoom, x, y)
        if key in self.fail_map:
            self.fail_map[key] -= 1
            if self.fail_map[key] > 0:
                raise RuntimeError('network boom')
        self.tiles[key] = True
        return bytes([zoom, x, y]), 'png'


def _master_total(output_dir, zoom):
    path = os.path.join(output_dir, f'tiles_{zoom}.db3')
    if not os.path.exists(path):
        return 0
    conn = sqlite3.connect(path)
    try:
        return conn.execute(f"SELECT COUNT(*) FROM z{zoom}").fetchone()[0]
    finally:
        conn.close()


async def run_client(base, session, client_id, buf_dir, src, **kwargs):
    """Запускает client.run с реальным ServerAPI и фейковым источником."""
    api = client.ServerAPI(base, client_id, session=session)
    buf = buffer.ClientBuffer(buf_dir, client_id)
    sleep = kwargs.pop('sleep', asyncio.sleep)
    try:
        return await client.run(api, buf, src.fetch, sleep=sleep, **kwargs)
    finally:
        buf.close()


@pytest.fixture
def run_integration(output_dir, clock):
    """run_integration(body, **cfg): поднимает сервер, вызывает body(base, mgr, clock, session, output_dir)."""

    def _run(body, *, min_zoom=1, max_zoom=2, chunk_size=5, task_ttl=900,
             reap_interval=15, zoom=None):
        async def _go():
            cfg = jobs.ServerConfig(
                output_dir=output_dir, min_zoom=min_zoom, max_zoom=max_zoom,
                chunk_size=chunk_size, task_ttl=task_ttl,
                reap_interval=reap_interval, zoom=zoom)
            mgr = jobs.JobManager(cfg, clock=clock)
            mgr.open()
            app = server.create_app(cfg, mgr)
            tc = TestClient(TestServer(app))
            await tc.start_server()
            base = str(tc.make_url('/')).rstrip('/')
            session = aiohttp.ClientSession()
            try:
                await body(base, mgr, clock, session, output_dir)
            finally:
                await session.close()
                await tc.close()
                mgr.close()
        asyncio.run(_go())

    return _run


async def _start(output_dir, clock, **cfg_kwargs):
    cfg = jobs.ServerConfig(output_dir=output_dir, **cfg_kwargs)
    mgr = jobs.JobManager(cfg, clock=clock)
    mgr.open()
    app = server.create_app(cfg, mgr)
    tc = TestClient(TestServer(app))
    await tc.start_server()
    base = str(tc.make_url('/')).rstrip('/')
    return base, mgr, tc


async def _download_task(api, src, task):
    tiles = []
    for i in range(task['start_idx'], task['end_idx']):
        x, y = utils.get_xy(i, task['zoom'])
        data, ext = await src.fetch(x, y, task['zoom'])
        tiles.append((x, y, ext, data))
    return tiles


# ------------------------------------------------------------------ сценарии

def test_full_cycle_and_idempotent_rerun(run_integration):
    async def body(base, mgr, clock, session, output_dir):
        zoom = 2
        total = 2 ** (2 * zoom)  # 16
        src = Source()
        rc = await run_client(base, session, 'C1',
                              os.path.join(output_dir, 'buf1'), src)
        assert rc == 0
        assert src.count == total
        assert _master_total(output_dir, zoom) == total

        # Повторный прогон: всё уже в БД -> done сразу, 0 запросов источнику.
        src2 = Source()
        rc2 = await run_client(base, session, 'C2',
                               os.path.join(output_dir, 'buf2'), src2)
        assert rc2 == 0
        assert src2.count == 0

        st = await (await session.get(base + '/api/status')).json()
        z = [x for x in st['zooms'] if x['zoom'] == zoom][0]
        assert z['percent'] == 100.0
        assert z['done'] is True
        assert st['global']['downloaded_tiles'] == total
    run_integration(body, min_zoom=2, max_zoom=2, chunk_size=5)


def test_two_clients_no_overlap(run_integration):
    async def body(base, mgr, clock, session, output_dir):
        zoom = 3
        total = 2 ** (2 * zoom)  # 64
        src = Source()

        async def go(cid):
            return await run_client(base, session, cid,
                                    os.path.join(output_dir, f'buf_{cid}'), src)

        rcs = await asyncio.gather(go('X'), go('Y'))
        assert all(rc == 0 for rc in rcs)
        # Каждый тайл скачан ровно один раз, без пересечений.
        assert src.count == total
        assert len(src.tiles) == total
        assert _master_total(output_dir, zoom) == total
    run_integration(body, min_zoom=3, max_zoom=3, chunk_size=16)


def test_dead_client_tiles_reissued(run_integration):
    async def body(base, mgr, clock, session, output_dir):
        api_a = client.ServerAPI(base, 'A', session)
        t_a = (await api_a.get_task())['task']
        # Клиент «умирает»: не сдаёт тайлы и не шлёт heartbeat.
        clock.advance(900 + 1)
        mgr.reap_expired(clock())
        assert mgr.tasks[t_a['task_id']].status == 'reaped'
        # Второй клиент получает освободившиеся тайлы.
        api_b = client.ServerAPI(base, 'B', session)
        t_b = (await api_b.get_task())['task']
        assert t_b is not None
        assert t_b['start_idx'] < t_a['end_idx']
        assert t_a['start_idx'] < t_b['end_idx']
    run_integration(body, min_zoom=2, max_zoom=2, chunk_size=8, task_ttl=900)


def test_heartbeat_extends_and_prevents_reap(run_integration):
    async def body(base, mgr, clock, session, output_dir):
        api = client.ServerAPI(base, 'H', session)
        t = (await api.get_task())['task']
        d0 = t['deadline']
        clock.advance(600)  # время идёт, срок близится
        hb = await api.heartbeat(t['task_id'])
        assert hb['ok'] is True
        d1 = mgr.tasks[t['task_id']].deadline
        assert d1 > d0
        # Прошли исходный дедлайн, но продлённый ещё впереди -> не reaped.
        clock.advance(d0 - clock() + 100)
        mgr.reap_expired(clock())
        assert mgr.tasks[t['task_id']].status == 'issued'
    run_integration(body, min_zoom=2, max_zoom=2, chunk_size=8, task_ttl=900)


def test_partial_master_backfill(run_integration):
    async def body(base, mgr, clock, session, output_dir):
        zoom = 1
        total = 2 ** (2 * zoom)  # 4
        # Предзаполняем два тайла в мастер-БД (докачка старых данных).
        r = db.Repository(output_dir)
        r.open(zoom)
        r.create_table(zoom)
        r.insert_ignore_many(zoom, [(0, 0, b'data0', b'png'),
                                    (1, 0, b'data1', b'png')])
        r.close()
        # Источник запрашивается только для недостающих двух.
        src = Source()
        rc = await run_client(base, session, 'P',
                              os.path.join(output_dir, 'bufP'), src)
        assert rc == 0
        assert src.count == total - 2
        assert _master_total(output_dir, zoom) == total
    run_integration(body, min_zoom=1, max_zoom=1, chunk_size=2)


def test_source_retries(run_integration):
    async def body(base, mgr, clock, session, output_dir):
        zoom = 1
        total = 2 ** (2 * zoom)  # 4
        # Каждый тайл падает на первой попытке, на второй — успех.
        fail_map = {(zoom, x, y): 2
                    for y in range(2 ** zoom) for x in range(2 ** zoom)}
        src = Source(fail_map=fail_map)
        rc = await run_client(base, session, 'R',
                              os.path.join(output_dir, 'bufR'), src,
                              retry_delay=0)
        assert rc == 0
        assert src.count == 2 * total  # по две попытки на тайл
        assert _master_total(output_dir, zoom) == total
    run_integration(body, min_zoom=1, max_zoom=1, chunk_size=4)


def test_server_restart_midway(output_dir, clock):
    """Клиент сдаёт часть, сервер «гаснет» с невыданным/непереданным,
    после рестарта второй клиент добирает остаток без повторной загрузки."""

    async def main():
        cfg_kw = dict(min_zoom=1, max_zoom=1, chunk_size=2, task_ttl=900,
                      reap_interval=15)
        zoom = 1
        total = 2 ** (2 * zoom)  # 4
        src = Source()

        # Фаза 1: сервер запущен.
        base, mgr, tc = await _start(output_dir, clock, **cfg_kw)
        session = aiohttp.ClientSession()
        try:
            api = client.ServerAPI(base, 'A', session)
            # Задача 1: скачал и сдал полностью.
            t1 = (await api.get_task())['task']
            tiles1 = await _download_task(api, src, t1)
            sub1 = await api.submit(t1['task_id'], t1['zoom'], tiles1)
            assert sub1['status'] == 'submitted'
            assert sub1['saved'] == t1['count']
            # Задача 2: получил, но «умирает» до сдачи.
            t2 = (await api.get_task())['task']
            assert t2 is not None
        finally:
            await session.close()
            await tc.close()
            mgr.close()  # «гашение» сервера

        # Фаза 2: рестарт на том же каталоге.
        base2, mgr2, tc2 = await _start(output_dir, clock, **cfg_kw)
        session2 = aiohttp.ClientSession()
        try:
            rc = await run_client(base2, session2, 'B',
                                  os.path.join(output_dir, 'bufB'), src)
            assert rc == 0
        finally:
            await session2.close()
            await tc2.close()
            mgr2.close()

        # Каждый тайл скачан ровно один раз: сданные в фазе 1 не перекачиваются.
        assert src.count == total
        assert len(src.tiles) == total
        assert _master_total(output_dir, zoom) == total
    asyncio.run(main())
