"""HTTP-сервер заданий (aiohttp).

Обёртка над jobs.JobManager: все вызовы ядра под одним asyncio.Lock.
Фоновый цикл reap_expired освобождает истёкшие задачи.

Эндпоинты (см. plan.md §5):
    GET  /healthz
    GET  /api/task?client_id=<id>
    POST /api/tasks/{task_id}/submit?client_id=<id>   (тело — бинарный фрейм)
    POST /api/tasks/{task_id}/heartbeat?client_id=<id>
    GET  /api/status?window=300
    GET  /api/events?limit=50
    GET  /dashboard
"""

import argparse
import asyncio
import os
import sys

from aiohttp import web

import dashboard
import jobs
import protocol

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)


def create_app(config, manager, lock=None,
               max_request_bytes=protocol.DEFAULT_MAX_REQUEST_BYTES):
    """Создаёт aiohttp-приложение (без запуска). manager должен быть открыт."""
    lock = lock or asyncio.Lock()

    async def _with_lock(fn, *args):
        async with lock:
            return fn(*args)

    async def healthz(request):
        return web.json_response({'ok': True})

    async def api_task(request):
        client_id = request.query.get('client_id')
        if not client_id:
            return web.json_response({'ok': False, 'error': 'client_id required'}, status=400)
        task, done = await _with_lock(manager.issue, client_id)
        if task is None:
            return web.json_response({'ok': True, 'task': None, 'done': done})
        return web.json_response({
            'ok': True,
            'task': {
                'task_id': task.id,
                'zoom': task.zoom,
                'start_idx': task.start_idx,
                'end_idx': task.end_idx,
                'count': task.count,
                'deadline': task.deadline,
            },
            'done': False,
        })

    async def api_submit(request):
        client_id = request.query.get('client_id')
        if not client_id:
            return web.json_response({'ok': False, 'error': 'client_id required'}, status=400)
        task_id = int(request.match_info['task_id'])
        body = await request.read()
        if len(body) > max_request_bytes:
            return web.json_response({'ok': False, 'error': 'request too large'}, status=413)
        try:
            zoom, tiles = protocol.decode_submit(
                body, max_tiles=config.chunk_size, max_request_bytes=max_request_bytes)
        except protocol.ProtocolError as e:
            return web.json_response({'ok': False, 'error': f'invalid frame: {e}'}, status=400)
        tiles = [(x, y, ext.decode('ascii', 'replace'), data) for (x, y, ext, data) in tiles]
        res = await _with_lock(manager.submit, client_id, task_id, tiles)
        return web.json_response({
            'ok': True,
            'status': res.status,
            'saved': res.saved,
            'duplicates': res.duplicates,
            'out_of_range': res.out_of_range,
        })

    async def api_heartbeat(request):
        client_id = request.query.get('client_id')
        if not client_id:
            return web.json_response({'ok': False, 'error': 'client_id required'}, status=400)
        task_id = int(request.match_info['task_id'])
        ok = await _with_lock(manager.heartbeat, client_id, task_id)
        return web.json_response({'ok': ok})

    async def api_status(request):
        window = int(request.query.get('window', '300'))
        st = await _with_lock(manager.status, window)
        return web.json_response({'ok': True, **st})

    async def api_events(request):
        limit = int(request.query.get('limit', '50'))
        ev = await _with_lock(manager.events, limit)
        return web.json_response({'ok': True, 'events': ev})

    async def dashboard_page(request):
        return web.Response(text=dashboard.HTML, content_type='text/html')

    async def _reap_loop():
        while True:
            await asyncio.sleep(config.reap_interval)
            await _with_lock(manager.reap_expired, manager.clock())

    reap_holder = {}

    async def _start_reap(app):
        reap_holder['task'] = asyncio.create_task(_reap_loop())

    async def _stop_reap(app):
        reap_holder['task'].cancel()
        try:
            await reap_holder['task']
        except asyncio.CancelledError:
            pass

    # client_max_size по умолчанию у aiohttp — 1 МиБ, и любой submit больше
    # (пачка тайлов) отклонялся им самим 413 text/plain до нашего обработчика.
    # Растягиваем до max_request_bytes, чтобы приложение само решало лимит.
    app = web.Application(client_max_size=max_request_bytes)
    app.router.add_get('/healthz', healthz)
    app.router.add_get('/api/task', api_task)
    app.router.add_post('/api/tasks/{task_id}/submit', api_submit)
    app.router.add_post('/api/tasks/{task_id}/heartbeat', api_heartbeat)
    app.router.add_get('/api/status', api_status)
    app.router.add_get('/api/events', api_events)
    app.router.add_get('/dashboard', dashboard_page)
    app.on_startup.append(_start_reap)
    app.on_cleanup.append(_stop_reap)
    return app


def main():
    parser = argparse.ArgumentParser(description='Tile Downloader server')
    parser.add_argument('-o', '--output-dir', default='.', help='Каталог мастер-БД и jobs.db3')
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--min-zoom', type=int, default=1)
    parser.add_argument('--max-zoom', type=int, default=14)
    parser.add_argument('-z', '--zoom', type=int, default=None, help='Скачивать только зум N')
    parser.add_argument('--chunk-size', type=int, default=2000)
    parser.add_argument('--task-ttl', type=int, default=900)
    parser.add_argument('--reap-interval', type=int, default=15)
    args = parser.parse_args()

    config = jobs.ServerConfig(
        output_dir=args.output_dir, min_zoom=args.min_zoom, max_zoom=args.max_zoom,
        zoom=args.zoom, chunk_size=args.chunk_size, task_ttl=args.task_ttl,
        reap_interval=args.reap_interval)
    manager = jobs.JobManager(config)
    manager.open()
    app = create_app(config, manager)
    print(f'Server: http://{args.host}:{args.port}  (dashboard: /dashboard)')
    web.run_app(app, host=args.host, port=args.port)
    manager.close()


if __name__ == '__main__':
    main()
