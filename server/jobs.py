"""Ядро сервера заданий (без сети и asyncio).

JobManager отвечает за:
  * выдачу задач (диапазонов индексов) клиентам;
  * учёт выданного (in-flight) и возврат в пул;
  * приём тайлов (идемпотентная запись в мастер-БД);
  * TTL / heartbeat;
  * выбор зума и поиск недостающего тайла;
  * статистику (клиенты, скорости, события).

Модуль синхронный: HTTP-слой (server.py) вызывает его методы под одним
asyncio.Lock. Время внедряется (clock) — для тестов TTL и окон.
"""

import os
import sqlite3
import time
from dataclasses import dataclass, field

import db
import utils


@dataclass
class ServerConfig:
    output_dir: str
    min_zoom: int = 1
    max_zoom: int = 14
    zoom: int = None            # если задан — скачивается только этот зум
    chunk_size: int = 2000      # размер задачи в тайлах
    task_ttl: int = 900         # время жизни задачи без heartbeat, c
    reap_interval: int = 15     # период TTL-рипера, c
    idle_window: int = 15       # c: молчание дольше — клиент считается неактивным


@dataclass
class TileTask:
    id: int
    zoom: int
    client_id: str
    start_idx: int
    count: int
    issued_at: float
    deadline: float
    status: str                 # issued | submitted | reaped

    @property
    def end_idx(self):
        return self.start_idx + self.count


@dataclass
class SubmitResult:
    status: str                 # submitted | unknown_task
    saved: int = 0
    duplicates: int = 0
    out_of_range: int = 0


class JobManager:
    def __init__(self, config, clock=None, logger=None):
        self.config = config
        self.clock = clock or time.time
        self.logger = logger or (lambda msg: None)

        self.jobs_conn = None
        self.repos = {}                    # zoom -> db.Repository
        self.meta = {}                     # кэш meta в памяти
        self.tasks = {}                    # id -> TileTask
        self.clients = {}                  # client_id -> {last_seen, submitted_tiles}
        self.submitting = set()            # client_id, чей сабмит сейчас принимается

    # ------------------------------------------------------------------ open

    def open(self):
        os.makedirs(self.config.output_dir, exist_ok=True)
        path = os.path.join(self.config.output_dir, 'jobs.db3')
        self.jobs_conn = sqlite3.connect(path, isolation_level=None)
        self.jobs_conn.execute('pragma journal_mode=wal')
        self._create_schema()
        self._load_meta()
        self._load_tasks()

        # При старте: все issued сбрасываем в reaped (клиенты не смогут их сдать),
        # выставляем rescan — следующий issue пересчитает первый недостающий тайл.
        now = self.clock()
        for t in self.tasks.values():
            if t.status == 'issued':
                t.status = 'reaped'
                self.jobs_conn.execute(
                    "UPDATE tasks SET status='reaped' WHERE id=?", (t.id,))

        for z in range(self.config.min_zoom, self.config.max_zoom + 1):
            self._set_rescan(z, True)
            # Индексы (y) для существующих мастер-БД + счётчик скачанных.
            fpath = os.path.join(self.config.output_dir, f'tiles_{z}.db3')
            if os.path.exists(fpath):
                repo = self._repo(z)
                n = repo._require_conn().execute(
                    f"SELECT COUNT(*) FROM z{z}").fetchone()[0]
            else:
                n = 0
            self._set_meta(self._zoom_key(z, 'downloaded'), n)

    def close(self):
        for r in self.repos.values():
            r.close()
        self.repos.clear()
        if self.jobs_conn is not None:
            self.jobs_conn.close()
            self.jobs_conn = None

    def _create_schema(self):
        c = self.jobs_conn
        c.execute("""CREATE TABLE IF NOT EXISTS meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS tasks (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            zoom       INTEGER NOT NULL,
            client_id  TEXT    NOT NULL,
            start_idx  INTEGER NOT NULL,
            count      INTEGER NOT NULL,
            issued_at  REAL    NOT NULL,
            deadline   REAL    NOT NULL,
            status     TEXT    NOT NULL DEFAULT 'issued')""")
        c.execute("""CREATE TABLE IF NOT EXISTS events (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ts        REAL    NOT NULL,
            zoom      INTEGER NOT NULL,
            kind      TEXT    NOT NULL,
            task_id   INTEGER,
            client_id TEXT,
            detail    TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS submits (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ts        REAL    NOT NULL,
            client_id TEXT    NOT NULL,
            zoom      INTEGER NOT NULL,
            saved     INTEGER NOT NULL)""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_submits_ts ON submits(ts)")

    def _load_meta(self):
        self.meta = {}
        for k, v in self.jobs_conn.execute("SELECT key, value FROM meta").fetchall():
            self.meta[k] = v

    def _load_tasks(self):
        self.tasks = {}
        rows = self.jobs_conn.execute(
            "SELECT id, zoom, client_id, start_idx, count, issued_at, deadline, status "
            "FROM tasks").fetchall()
        for r in rows:
            t = TileTask(*r)
            self.tasks[t.id] = t

    # ----------------------------------------------------------------- meta

    def _zoom_key(self, zoom, name):
        return f"zoom_{zoom}_{name}"

    def _meta_get(self, key):
        return self.meta.get(key)

    def _set_meta(self, key, value):
        self.meta[key] = str(value)
        self.jobs_conn.execute(
            "INSERT INTO meta (key, value) VALUES (?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))

    def _cursor(self, zoom):
        v = self._meta_get(self._zoom_key(zoom, 'cursor'))
        return int(v) if v is not None else 0

    def _set_cursor(self, zoom, val):
        self._set_meta(self._zoom_key(zoom, 'cursor'), val)

    def _rescan_flag(self, zoom):
        return self._meta_get(self._zoom_key(zoom, 'rescan')) == '1'

    def _set_rescan(self, zoom, flag):
        self._set_meta(self._zoom_key(zoom, 'rescan'), '1' if flag else '0')

    def _downloaded(self, zoom):
        v = self._meta_get(self._zoom_key(zoom, 'downloaded'))
        if v is None:
            v = self._repo(zoom)._require_conn().execute(
                f"SELECT COUNT(*) FROM z{zoom}").fetchone()[0]
            self._set_meta(self._zoom_key(zoom, 'downloaded'), v)
        return int(v)

    def _inc_downloaded(self, zoom, n):
        self._set_meta(self._zoom_key(zoom, 'downloaded'), self._downloaded(zoom) + n)

    def _total(self, zoom):
        return (2 ** zoom) ** 2

    def _zoom_done(self, zoom):
        return self._downloaded(zoom) >= self._total(zoom)

    def _active_zoom_val(self):
        v = self._meta_get('active_zoom')
        return int(v) if v is not None else None

    # ----------------------------------------------------------------- repo

    def _repo(self, zoom):
        if zoom not in self.repos:
            r = db.Repository(self.config.output_dir)
            r.open(zoom)
            r.create_table(zoom)
            self.repos[zoom] = r
        return self.repos[zoom]

    # ---------------------------------------------------------------- issue

    def issue(self, client_id):
        """Выдать задачу клиенту. Возвращает (TileTask|None, done)."""
        now = self.clock()
        # Регистрируем клиента на каждом запросе — так видно и «ждущих».
        self._touch_client(client_id, now)
        self._reset_client_tasks(client_id, now)
        return self._issue_next(client_id, now)

    def set_submitting(self, client_id, flag):
        """Пометка «клиент отдаёт результат» на время приёма сабмита."""
        now = self.clock()
        self._touch_client(client_id, now)
        if flag:
            self.submitting.add(client_id)
        else:
            self.submitting.discard(client_id)

    def _issue_next(self, client_id, now):
        zoom = self._activate_zoom(now)
        if zoom is None:
            return (None, True)

        if self._rescan_flag(zoom):
            self._set_cursor(zoom, 0)
            self._set_rescan(zoom, False)

        cursor = self._cursor(zoom)
        start = self._find_next_free(zoom, cursor)
        if start is None and cursor != 0:
            # Свободного от курсора нет — возможно, освободилось позади фронта.
            self._set_cursor(zoom, 0)
            start = self._find_next_free(zoom, 0)

        if start is None:
            if self._active_task_count(zoom) > 0:
                return (None, False)          # есть in-flight — клиент подождёт
            self._mark_zoom_done(zoom, now)
            return self._issue_next(client_id, now)

        end = self._expand_run(zoom, start)
        self._set_cursor(zoom, end)
        task = self._record_task(zoom, client_id, start, end, now)
        self._touch_client(client_id, now)
        return (task, False)

    def _activate_zoom(self, now):
        cfg = self.config
        if cfg.zoom is not None:
            z = cfg.zoom
            if self._zoom_done(z):
                return None
            self._set_meta('active_zoom', z)
            return z

        z = self._active_zoom_val()
        if z is not None and not self._zoom_done(z):
            return z

        for z in range(cfg.min_zoom, cfg.max_zoom + 1):
            if not self._zoom_done(z):
                prev = self._active_zoom_val()
                self._set_meta('active_zoom', z)
                if prev != z:
                    self._event(now, z, 'zoom_start', None, None, None)
                return z
        return None

    def _active_intervals(self, zoom):
        intervals = [(t.start_idx, t.end_idx) for t in self.tasks.values()
                     if t.status == 'issued' and t.zoom == zoom]
        intervals.sort()
        return intervals

    def _is_covered(self, intervals, idx):
        for start, end in intervals:
            if start <= idx < end:
                return True
            if start > idx:
                break
        return False

    def _is_occupied(self, zoom, idx, intervals):
        if self._is_covered(intervals, idx):
            return True
        x, y = utils.get_xy(idx, zoom)
        return self._repo(zoom).exists_xy(x, y, zoom)

    def _find_next_free(self, zoom, cursor):
        """Первый свободный индекс >= cursor, либо None если зум заполнен."""
        size = 2 ** zoom
        total = size * size
        if cursor >= total:
            return None
        intervals = self._active_intervals(zoom)
        repo = self._repo(zoom)
        start_y, start_x = utils.get_xy(cursor, zoom)

        for y in range(start_y, size):
            x_begin = start_x if y == start_y else 0
            if x_begin > 0:
                # Первая (частичная) строка — идём от x_begin.
                for x in range(x_begin, size):
                    idx = utils.get_index(x, y, zoom)
                    if not self._is_occupied(zoom, idx, intervals):
                        return idx
                continue
            # Целая строка от x=0 — быстрый путь через count по ряду.
            if repo.is_full_row(y, zoom):
                continue
            for x in range(size):
                idx = utils.get_index(x, y, zoom)
                if not self._is_occupied(zoom, idx, intervals):
                    return idx
        return None

    def _expand_run(self, zoom, start):
        size = 2 ** zoom
        total = size * size
        intervals = self._active_intervals(zoom)
        end = start + 1
        while end < total and (end - start) < self.config.chunk_size:
            if self._is_occupied(zoom, end, intervals):
                break
            end += 1
        return end

    def _record_task(self, zoom, client_id, start, end, now):
        count = end - start
        deadline = now + self.config.task_ttl
        cur = self.jobs_conn.execute(
            "INSERT INTO tasks (zoom, client_id, start_idx, count, issued_at, deadline, status) "
            "VALUES (?,?,?,?,?,?, 'issued')",
            (zoom, client_id, start, count, now, deadline))
        task_id = cur.lastrowid
        task = TileTask(id=task_id, zoom=zoom, client_id=client_id, start_idx=start,
                        count=count, issued_at=now, deadline=deadline, status='issued')
        self.tasks[task_id] = task
        self._event(now, zoom, 'issue', task_id, client_id,
                    f"start={start} end={end}")
        return task

    def _mark_zoom_done(self, zoom, now):
        self._set_meta(self._zoom_key(zoom, 'downloaded'), self._total(zoom))
        self._event(now, zoom, 'zoom_done', None, None, None)

    # --------------------------------------------------------------- submit

    def submit(self, client_id, task_id, tiles):
        """tiles: список (x, y, ext, data). Возвращает SubmitResult."""
        now = self.clock()
        task = self.tasks.get(task_id)
        if task is None or task.client_id != client_id or task.status != 'issued':
            return SubmitResult(status='unknown_task')

        zoom = task.zoom
        start, end = task.start_idx, task.end_idx
        size = 2 ** zoom

        valid = []
        seen = set()
        out_of_range = 0
        dup_in_batch = 0
        for (x, y, ext, data) in tiles:
            if not (0 <= x < size and 0 <= y < size):
                out_of_range += 1
                continue
            idx = utils.get_index(x, y, zoom)
            if not (start <= idx < end):
                out_of_range += 1
                continue
            if (x, y) in seen:
                dup_in_batch += 1            # дубль внутри пачки
                continue
            seen.add((x, y))
            valid.append((x, y, data, ext))

        saved = self._repo(zoom).insert_ignore_many(zoom, valid) if valid else 0
        duplicates = dup_in_batch + (len(valid) - saved)

        task.status = 'submitted'
        self.jobs_conn.execute("UPDATE tasks SET status='submitted' WHERE id=?", (task_id,))
        self._inc_downloaded(zoom, saved)
        self._record_submit(now, client_id, zoom, saved)
        self._event(now, zoom, 'submit', task_id, client_id,
                    f"saved={saved} duplicates={duplicates} out_of_range={out_of_range}")
        self._touch_client(client_id, now)

        # Остаток диапазона (неотданные тайлы) возвращается в пул.
        if len(seen) < task.count:
            self._set_rescan(zoom, True)

        return SubmitResult(status='submitted', saved=saved,
                            duplicates=duplicates, out_of_range=out_of_range)

    # ------------------------------------------------------------- heartbeat

    def heartbeat(self, client_id, task_id):
        now = self.clock()
        task = self.tasks.get(task_id)
        if task is None or task.client_id != client_id or task.status != 'issued':
            return False
        task.deadline = now + self.config.task_ttl
        self.jobs_conn.execute("UPDATE tasks SET deadline=? WHERE id=?",
                               (task.deadline, task_id))
        self._touch_client(client_id, now)
        return True

    # ----------------------------------------------------------------- reap

    def reap_expired(self, now):
        """Освободить задачи с истёкшим deadline. Возвращает список task_id."""
        reaped = []
        for task in list(self.tasks.values()):
            if task.status == 'issued' and task.deadline < now:
                self._reap(task, now, 'ttl')
                reaped.append(task.id)
        return reaped

    def _reap(self, task, now, reason):
        task.status = 'reaped'
        self.jobs_conn.execute("UPDATE tasks SET status='reaped' WHERE id=?", (task.id,))
        self._release_range(task.zoom, task.start_idx, task.end_idx, now)
        self._event(now, task.zoom, 'reap', task.id, task.client_id, reason)

    def _reset_client_tasks(self, client_id, now):
        for task in list(self.tasks.values()):
            if task.status == 'issued' and task.client_id == client_id:
                self._reap(task, now, 'reset')

    def _release_range(self, zoom, start, end, now):
        # Диапазон снова свободен. Если он позади фронта — нужен rescan.
        if start < self._cursor(zoom):
            self._set_rescan(zoom, True)

    # ------------------------------------------------------------ statistics

    def status(self, window_sec=300):
        now = self.clock()
        zooms = []
        total_downloaded = 0
        for z in range(self.config.min_zoom, self.config.max_zoom + 1):
            total = self._total(z)
            downloaded = self._downloaded(z)
            done = downloaded >= total
            percent = (downloaded / total * 100.0) if total else 100.0
            zooms.append({
                'zoom': z,
                'total': total,
                'downloaded': downloaded,
                'percent': round(percent, 2),
                'done': done,
                'active_tasks': self._active_task_count(z),
            })
            total_downloaded += downloaded

        clients = []
        for cid, info in self.clients.items():
            clients.append({
                'client_id': cid,
                'state': self._client_state(cid, now),
                'active_tasks': self._client_active_tasks(cid),
                'last_seen': info['last_seen'],
                'submitted_tiles': info['submitted_tiles'],
                'rate_tps': self._client_rate(cid, now, window_sec),
            })
        active_clients = sum(1 for c in clients
                             if self._client_is_active(c['client_id'], now, window_sec))

        total_expected = sum(zz['total'] for zz in zooms)
        total_percent = (total_downloaded / total_expected * 100.0) if total_expected else 100.0
        by_state = {}
        for c in clients:
            by_state[c['state']] = by_state.get(c['state'], 0) + 1

        return {
            'done': all(zz['done'] for zz in zooms),
            'active_zoom': self._active_zoom_val(),
            'window_sec': window_sec,
            'zooms': zooms,
            'clients': clients,
            'global': {
                'downloaded_tiles': total_downloaded,
                'total_tiles': total_expected,
                'percent': round(total_percent, 2),
                'active_clients': active_clients,
                'clients_total': len(clients),
                'clients_waiting': by_state.get('waiting', 0),
                'clients_working': by_state.get('working', 0),
                'clients_submitting': by_state.get('submitting', 0),
                'clients_offline': by_state.get('offline', 0),
                'rate_tps': self._global_rate(now, window_sec),
            },
        }

    def events(self, limit=50):
        rows = self.jobs_conn.execute(
            "SELECT ts, zoom, kind, task_id, client_id, detail "
            "FROM events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [{'ts': r[0], 'zoom': r[1], 'kind': r[2], 'task_id': r[3],
                 'client_id': r[4], 'detail': r[5]} for r in rows]

    def _active_task_count(self, zoom):
        return sum(1 for t in self.tasks.values()
                   if t.status == 'issued' and t.zoom == zoom)

    def _client_active_tasks(self, cid):
        return sum(1 for t in self.tasks.values()
                   if t.status == 'issued' and t.client_id == cid)

    def _touch_client(self, cid, now):
        if cid not in self.clients:
            self.clients[cid] = {'last_seen': now, 'submitted_tiles': 0}
        self.clients[cid]['last_seen'] = now

    def _record_submit(self, now, cid, zoom, saved):
        self.jobs_conn.execute(
            "INSERT INTO submits (ts, client_id, zoom, saved) VALUES (?,?,?,?)",
            (now, cid, zoom, saved))
        if cid not in self.clients:
            self.clients[cid] = {'last_seen': now, 'submitted_tiles': 0}
        self.clients[cid]['submitted_tiles'] += saved

    def _client_state(self, cid, now):
        """Состояние клиента: submitting | working | waiting | offline."""
        if cid in self.submitting:
            return 'submitting'
        if self._client_active_tasks(cid) > 0:
            return 'working'
        info = self.clients.get(cid)
        if info and (now - info['last_seen']) <= self.config.idle_window:
            return 'waiting'
        return 'offline'

    def _client_is_active(self, cid, now, window_sec):
        if self._client_active_tasks(cid) > 0:
            return True
        info = self.clients.get(cid)
        return bool(info and (now - info['last_seen']) <= window_sec)

    def _global_rate(self, now, window_sec):
        since = now - window_sec
        row = self.jobs_conn.execute(
            "SELECT COALESCE(SUM(saved),0) FROM submits WHERE ts >= ?", (since,)).fetchone()
        return row[0] / window_sec

    def _client_rate(self, cid, now, window_sec):
        since = now - window_sec
        row = self.jobs_conn.execute(
            "SELECT COALESCE(SUM(saved),0) FROM submits WHERE ts >= ? AND client_id=?",
            (since, cid)).fetchone()
        return row[0] / window_sec

    def _event(self, now, zoom, kind, task_id, client_id, detail):
        self.jobs_conn.execute(
            "INSERT INTO events (ts, zoom, kind, task_id, client_id, detail) "
            "VALUES (?,?,?,?,?,?)", (now, zoom, kind, task_id, client_id, detail))
