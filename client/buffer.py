"""Локальный буфер клиента (SQLite buffer_<client_id>.db3).

Точка устойчивости: скачанные тайлы пишутся в буфер по мере скачивания и
переживают падение клиента. При следующем старте остатки досдаются.
"""

import os
import sqlite3
from sqlite3 import Error


class ClientBuffer:
    def __init__(self, buffer_dir, client_id):
        self.buffer_dir = buffer_dir
        self.client_id = client_id
        os.makedirs(buffer_dir, exist_ok=True)
        self.db_file = os.path.join(buffer_dir, f'buffer_{client_id}.db3')
        self.conn = sqlite3.connect(self.db_file, isolation_level=None)
        self.conn.execute('pragma journal_mode=wal')
        self._create_schema()

    def _create_schema(self):
        c = self.conn
        c.execute("""CREATE TABLE IF NOT EXISTS buffer (
            zoom    INTEGER NOT NULL,
            x       INTEGER NOT NULL,
            y       INTEGER NOT NULL,
            image   BLOB    NOT NULL,
            ext     TEXT    NOT NULL,
            task_id INTEGER NOT NULL,
            PRIMARY KEY (zoom, x, y))""")
        c.execute("""CREATE TABLE IF NOT EXISTS buffer_meta (
            task_id   INTEGER PRIMARY KEY,
            zoom      INTEGER,
            start_idx INTEGER,
            count     INTEGER)""")

    def put(self, zoom, x, y, image, ext, task_id):
        self.conn.execute(
            "INSERT OR REPLACE INTO buffer (zoom, x, y, image, ext, task_id) "
            "VALUES (?,?,?,?,?,?)", (zoom, x, y, image, ext, task_id))

    def put_many(self, rows):
        """rows: список (zoom, x, y, image, ext, task_id)."""
        self.conn.executemany(
            "INSERT OR REPLACE INTO buffer (zoom, x, y, image, ext, task_id) "
            "VALUES (?,?,?,?,?,?)", rows)

    def all_for_task(self, task_id):
        """Возвращает список (zoom, x, y, image, ext) для задачи."""
        rows = self.conn.execute(
            "SELECT zoom, x, y, image, ext FROM buffer WHERE task_id=? "
            "ORDER BY zoom, x, y", (task_id,)).fetchall()
        return list(rows)

    def count_for_task(self, task_id):
        return self.conn.execute(
            "SELECT COUNT(*) FROM buffer WHERE task_id=?", (task_id,)).fetchone()[0]

    def clear_task(self, task_id):
        self.conn.execute("DELETE FROM buffer WHERE task_id=?", (task_id,))
        self.conn.execute("DELETE FROM buffer_meta WHERE task_id=?", (task_id,))

    def save_task_meta(self, task_id, zoom, start_idx, count):
        self.conn.execute(
            "INSERT OR REPLACE INTO buffer_meta (task_id, zoom, start_idx, count) "
            "VALUES (?,?,?,?)", (task_id, zoom, start_idx, count))

    def load_meta(self):
        """Метаданные текущей задачи в буфере, либо None."""
        row = self.conn.execute(
            "SELECT task_id, zoom, start_idx, count FROM buffer_meta").fetchone()
        if row is None:
            return None
        return {'task_id': row[0], 'zoom': row[1], 'start_idx': row[2], 'count': row[3]}

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
