"""Этап 3. HTTP-контракт сервера (реальный aiohttp на ephemeral-порту)."""

import asyncio

import pytest
from aiohttp.test_utils import TestServer, TestClient

import jobs
import protocol
import server
import utils


def _frame_for(task):
    tiles = []
    for i in range(task['start_idx'], task['end_idx']):
        x, y = utils.get_xy(i, task['zoom'])
        tiles.append((x, y, b'png', b'data-%d' % i))
    return protocol.encode_submit(task['zoom'], tiles)


@pytest.fixture
def run_server(output_dir, clock):
    """run_server(body, **cfg): поднимает сервер и вызывает body(client, mgr, clock)."""

    def _run(body, *, min_zoom=1, max_zoom=2, chunk_size=5, task_ttl=900,
             reap_interval=15, zoom=None, max_request_bytes=None):
        async def _go():
            cfg = jobs.ServerConfig(
                output_dir=output_dir, min_zoom=min_zoom, max_zoom=max_zoom,
                chunk_size=chunk_size, task_ttl=task_ttl, reap_interval=reap_interval,
                zoom=zoom)
            mgr = jobs.JobManager(cfg, clock=clock)
            mgr.open()
            app = server.create_app(
                cfg, mgr,
                max_request_bytes=max_request_bytes or protocol.DEFAULT_MAX_REQUEST_BYTES)
            client = TestClient(TestServer(app))
            await client.start_server()
            try:
                await body(client, mgr, clock)
            finally:
                await client.close()
                mgr.close()
        asyncio.run(_go())

    return _run


# ------------------------------------------------------------------ базовое

def test_healthz(run_server):
    async def body(client, mgr, clock):
        resp = await client.get('/healthz')
        assert resp.status == 200
        assert (await resp.json())['ok'] is True
    run_server(body)


def test_task_requires_client_id(run_server):
    async def body(client, mgr, clock):
        resp = await client.get('/api/task')
        assert resp.status == 400
        assert (await resp.json())['ok'] is False
    run_server(body)


def test_task_contract(run_server):
    async def body(client, mgr, clock):
        resp = await client.get('/api/task?client_id=c1')
        assert resp.status == 200
        data = await resp.json()
        task = data['task']
        assert task is not None
        for field in ('task_id', 'zoom', 'start_idx', 'end_idx', 'count', 'deadline'):
            assert field in task
        assert task['end_idx'] - task['start_idx'] == task['count']
    run_server(body)


# ------------------------------------------------------------------ submit

def test_submit_ok_and_repeat_unknown(run_server):
    async def body(client, mgr, clock):
        task = (await (await client.get('/api/task?client_id=c1')).json())['task']
        resp = await client.post(
            f"/api/tasks/{task['task_id']}/submit?client_id=c1", data=_frame_for(task))
        assert resp.status == 200
        data = await resp.json()
        assert data['status'] == 'submitted'
        assert data['saved'] == task['count']
        # повторный сабмит закрытой задачи -> unknown_task
        resp2 = await client.post(
            f"/api/tasks/{task['task_id']}/submit?client_id=c1", data=_frame_for(task))
        assert (await resp2.json())['status'] == 'unknown_task'
    run_server(body, min_zoom=1, max_zoom=1, chunk_size=4)


def test_submit_invalid_frame_400(run_server):
    async def body(client, mgr, clock):
        task = (await (await client.get('/api/task?client_id=c1')).json())['task']
        resp = await client.post(
            f"/api/tasks/{task['task_id']}/submit?client_id=c1", data=b'garbage-bytes!!')
        assert resp.status == 400
        assert (await resp.json())['ok'] is False
    run_server(body)


def test_submit_too_large_413(run_server):
    async def body(client, mgr, clock):
        task = (await (await client.get('/api/task?client_id=c1')).json())['task']
        frame = _frame_for(task)
        resp = await client.post(
            f"/api/tasks/{task['task_id']}/submit?client_id=c1", data=frame)
        assert resp.status == 413
    run_server(body, min_zoom=1, max_zoom=1, chunk_size=4, max_request_bytes=10)


def test_submit_requires_client_id(run_server):
    async def body(client, mgr, clock):
        task = (await (await client.get('/api/task?client_id=c1')).json())['task']
        resp = await client.post(f"/api/tasks/{task['task_id']}/submit", data=_frame_for(task))
        assert resp.status == 400
    run_server(body, min_zoom=1, max_zoom=1, chunk_size=4)


# ----------------------------------------------------------------- heartbeat

def test_heartbeat_extends_deadline(run_server):
    async def body(client, mgr, clock):
        task = (await (await client.get('/api/task?client_id=c1')).json())['task']
        before = mgr.tasks[task['task_id']].deadline
        resp = await client.post(f"/api/tasks/{task['task_id']}/heartbeat?client_id=c1")
        assert resp.status == 200
        assert (await resp.json())['ok'] is True
        assert mgr.tasks[task['task_id']].deadline >= before
        # чужой клиент -> ok: false
        resp2 = await client.post(f"/api/tasks/{task['task_id']}/heartbeat?client_id=c2")
        assert (await resp2.json())['ok'] is False
    run_server(body, min_zoom=1, max_zoom=1, chunk_size=4)


# ------------------------------------------------------- параллельные клиенты

def test_two_clients_no_overlap(run_server):
    async def body(client, mgr, clock):
        async def get(cid):
            return (await (await client.get(f'/api/task?client_id={cid}')).json())['task']
        t1, t2 = await asyncio.gather(get('c1'), get('c2'))
        assert t1 and t2
        assert t1['end_idx'] <= t2['start_idx'] or t2['end_idx'] <= t1['start_idx']
    run_server(body, min_zoom=2, max_zoom=2, chunk_size=4)


# --------------------------------------------------------------------- рипер

def test_reap_by_schedule(run_server):
    async def body(client, mgr, clock):
        task = (await (await client.get('/api/task?client_id=c1')).json())['task']
        assert task
        # двигаем часы за deadline
        clock.advance((task['deadline'] - clock()) + 1)
        await asyncio.sleep(0.2)          # даём риперу отработать
        st = await (await client.get('/api/status')).json()
        z1 = [z for z in st['zooms'] if z['zoom'] == 1][0]
        assert z1['active_tasks'] == 0
        # освобождённые тайлы снова выдаются
        t2 = (await (await client.get('/api/task?client_id=c2')).json())['task']
        assert t2 is not None
    run_server(body, min_zoom=1, max_zoom=1, chunk_size=2, reap_interval=0.05)


# ----------------------------------------------------------- status / events

def test_status_fields_and_rate(run_server):
    async def body(client, mgr, clock):
        task = (await (await client.get('/api/task?client_id=c1')).json())['task']
        await client.post(f"/api/tasks/{task['task_id']}/submit?client_id=c1",
                          data=_frame_for(task))
        st = await (await client.get('/api/status?window=300')).json()
        for field in ('done', 'active_zoom', 'window_sec', 'zooms', 'clients', 'global'):
            assert field in st
        assert st['global']['rate_tps'] > 0
        assert st['global']['downloaded_tiles'] == task['count']
    run_server(body, min_zoom=1, max_zoom=1, chunk_size=4)


def test_events_respects_limit(run_server):
    async def body(client, mgr, clock):
        task = (await (await client.get('/api/task?client_id=c1')).json())['task']
        await client.post(f"/api/tasks/{task['task_id']}/submit?client_id=c1",
                          data=_frame_for(task))
        ev = await (await client.get('/api/events?limit=2')).json()
        assert ev['ok'] is True
        assert len(ev['events']) <= 2
    run_server(body, min_zoom=1, max_zoom=1, chunk_size=4)


# ----------------------------------------------------------------- done=null

def test_task_null_then_done(run_server):
    async def body(client, mgr, clock):
        # докачиваем зум целиком
        for _ in range(10):
            d = await (await client.get('/api/task?client_id=c1')).json()
            if d['task'] is None:
                break
            await client.post(f"/api/tasks/{d['task']['task_id']}/submit?client_id=c1",
                              data=_frame_for(d['task']))
        d = await (await client.get('/api/task?client_id=c1')).json()
        assert d['task'] is None and d['done'] is True
    run_server(body, min_zoom=1, max_zoom=1, chunk_size=2)


# ------------------------------------------------------------------ дашборд

def test_dashboard_html(run_server):
    async def body(client, mgr, clock):
        resp = await client.get('/dashboard')
        assert resp.status == 200
        assert 'text/html' in resp.headers['Content-Type']
        text = await resp.text()
        assert 'active-zoom' in text
        assert 'clients-table' in text
        assert '/api/status' in text
    run_server(body)
