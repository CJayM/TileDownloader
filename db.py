import sqlite3
from sqlite3 import Error
import asyncio

import utils

DB_FILE = "tiles.db3"

db_lock = asyncio.Lock()


def make_connection():
    conn = sqlite3.connect(DB_FILE, isolation_level=None)
    conn.execute('pragma journal_mode=wal')
    return conn


def create_table(conn, zoom):
    create_table_sql = f"""CREATE TABLE if not exists z{zoom} (
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
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


async def save_in_db(conn, x, y, zoom, data):
    task_1 = (x, y, data, 'png')
    sql = f'INSERT OR IGNORE INTO z{zoom}(x,y,image, ext) VALUES(?,?,?,?)'

    async with db_lock:
        cur = conn.cursor()
        cur.execute(sql, task_1)
        conn.commit()


def is_tile_exists(conn, x, y, zoom):
    sql = f"SELECT EXISTS(SELECT 1 FROM z{zoom} WHERE x=\"{x}\" and y=\"{y}\" LIMIT 1);"
    c = conn.cursor()
    c.execute(sql)
    rows = c.fetchall()
    return rows[0][0] == 1


class Repository:
    def __init__(self):
        self.conn = None

    def open(self):
        self.conn = make_connection()

    def create_table(self, zoom):
        create_table(self.conn, zoom)

    async def save(self, x, y, zoom, content):
        await save_in_db(self.conn, x, y, zoom, content)

    def is_exists(self, x, y, zoom):
        return is_tile_exists(self.conn, x, y, zoom)

    def find_start(self, zoom, start_cell):
        max_size = 2 ** zoom
        current = 0
        total = max_size * max_size - 1

        start_y = 0
        start_x = 0
        if start_cell != -1:
            start_y = start_cell // max_size
            start_x = start_cell - start_y * max_size

        for y in range(start_y, max_size):
            for x in range(start_x, max_size):
                current = utils.get_index(x, y, zoom)
                if is_tile_exists(self.conn, x, y, zoom) == False:
                    return current

        return current
