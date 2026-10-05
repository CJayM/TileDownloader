import sqlite3
from sqlite3 import Error
import asyncio
import os

# import db  # Removed circular import
import utils

BUFFER_SIZE = 10000


def make_connection(zoom: int, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    db_file = os.path.join(output_dir, f"tiles_{zoom}.db3")
    conn = sqlite3.connect(db_file, isolation_level=None)
    conn.execute('pragma journal_mode=wal')
    return conn


class Repository:
    """Доступ к мастер-БД тайлов (tiles_{zoom}.db3).

    Всё изменяемое состояние (соединение, буфер, блокировка) — инстансное:
    несколько Repository в одном процессе не смешивают свои буферы.
    Инстанс работает с одним активным зумом (см. open()).
    """

    def __init__(self, output_dir):
        self.output_dir = output_dir
        self.conn = None
        self.zoom = None
        self.buffer = []
        self.lock = asyncio.Lock()

    def open(self, zoom: int):
        if self.conn is not None:
            # Заменяем соединение: незаписанный буфер старого зума сбрасываем,
            # чтобы он не ушёл в файл другого зума (старый баг глобального буфера).
            self.conn.close()
            self.conn = None
            if self.buffer:
                print(f"Discarding {len(self.buffer)} unsaved buffered tiles (repository reopened)")
            self.buffer = []
        self.zoom = zoom
        self.conn = make_connection(zoom, self.output_dir)

    def create_table(self, zoom):
        conn = self._require_conn()
        create_table_sql = f"""CREATE TABLE IF NOT EXISTS z{zoom} (
            x     INTEGER NOT NULL,
            y     INTEGER NOT NULL,
            image BLOB    NOT NULL,
            ext   TEXT    NOT NULL,
            PRIMARY KEY (
                x,
                y
            )
        );
        """
        cur = conn.cursor()
        cur.execute(create_table_sql)
        # Для новых БД индекс для проверок «ряд заполнен» создаём сразу
        self.add_y_index(zoom)

    def add_y_index(self, zoom):
        """Индекс idx_z{z}_y для быстрых проверок по ряду (y).

        Для существующих БД вызывается при старте сервера; идемпотентен.
        """
        conn = self._require_conn()
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_z{zoom}_y ON z{zoom}(y)")

    async def save(self, x, y, zoom, content):
        """Добавить тайл в буфер; при заполнении — пакетная запись в БД."""
        self.buffer.append((x, y, content, 'png', zoom))
        if len(self.buffer) >= BUFFER_SIZE:
            await self._flush_buffer()

    async def _flush_buffer(self):
        """Пакетная запись всех записей буфера в БД."""
        if not self.buffer:
            return

        print("flush buffer to db")

        conn = self._require_conn()

        # Group records by zoom level
        records_by_zoom = {}
        for x, y, data, ext, zoom in self.buffer:
            if zoom not in records_by_zoom:
                records_by_zoom[zoom] = []
            records_by_zoom[zoom].append((x, y, data, ext))

        async with self.lock:
            cur = conn.cursor()
            for zoom, records in records_by_zoom.items():
                sql = f'INSERT OR IGNORE INTO z{zoom}(x, y, image, ext) VALUES (?,?,?,?)'
                cur.executemany(sql, records)

        # Clear the buffer after flushing
        self.buffer = []

    async def flush_buffer(self):
        """Flush any remaining records in the buffer"""
        await self._flush_buffer()

    async def commit(self):
        await self.flush_buffer()

        conn = self._require_conn()
        async with self.lock:
            conn.commit()

    def insert_ignore_many(self, zoom, rows):
        """executemany INSERT OR IGNORE одной транзакцией.

        rows: итерация кортежей (x, y, image, ext).
        Возвращает число реально вставленных записей (дубликаты не считаются).
        """
        conn = self._require_conn()
        if not rows:
            return 0

        cur = conn.cursor()
        cur.execute("SELECT total_changes()")
        before = cur.fetchone()[0]

        sql = f'INSERT OR IGNORE INTO z{zoom}(x, y, image, ext) VALUES (?,?,?,?)'
        cur.execute("BEGIN IMMEDIATE")
        try:
            cur.executemany(sql, rows)
            cur.execute("COMMIT")
        except Error:
            try:
                cur.execute("ROLLBACK")
            except Error:
                pass
            raise

        cur.execute("SELECT total_changes()")
        after = cur.fetchone()[0]
        return after - before

    def row_count(self, y, zoom):
        """Число тайлов в ряду y."""
        conn = self._require_conn()
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(x) FROM z{zoom} WHERE y = ?", (y,))
        return cur.fetchone()[0]

    def is_full_row(self, y, zoom):
        return self.row_count(y, zoom) == (2 ** zoom)

    def exists_xy(self, x, y, zoom):
        conn = self._require_conn()
        cur = conn.cursor()
        cur.execute(f"SELECT EXISTS(SELECT 1 FROM z{zoom} WHERE x=? AND y=?)", (x, y))
        return cur.fetchone()[0] == 1

    def is_exists(self, x, y, zoom):
        return self.exists_xy(x, y, zoom)

    def find_start(self, zoom, start_cell):
        max_size = 2 ** zoom
        current = 0
        total = max_size * max_size - 1

        start_y = 0
        start_x = 0
        if start_cell != -1:
            start_y = start_cell // max_size
            start_x = start_cell - start_y * max_size

        checked_rows = 0
        total_rows = max_size - start_y

        for y in range(start_y, max_size):
            checked_rows += 1

            # Выводим прогресс каждые 10 строк или если это первая или последняя строка
            if y == start_y or (y - start_y) % 10 == 9 or y == max_size - 1:
                percent = (checked_rows / total_rows) * 100
                print(f"Checking row {y} of {max_size-1} ({percent:.2f}%)")

            if self.is_full_row(y, zoom):
                current = utils.get_index(0, y + 1, zoom)
                continue
            else:
                print(f"Row {y} not full")
                start_x = 0

            for x in range(start_x, max_size):
                current = utils.get_index(x, y, zoom)
                if self.is_exists(x, y, zoom) == False:
                    return current

        return current

    def close(self):
        """Закрывает соединение с базой данных (повторно безопасен)"""
        if self.conn is not None:
            self.conn.close()
            self.conn = None
        self.buffer = []

    def _require_conn(self):
        if self.conn is None:
            raise Error("repository is not opened")
        return self.conn
