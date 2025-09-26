import sqlite3
from sqlite3 import Error
import asyncio
import os

# import db  # Removed circular import
import utils

db_lock = asyncio.Lock()


def make_connection(output_dir):
    db_file = os.path.join(output_dir, "tiles.db3")
    conn = sqlite3.connect(db_file, isolation_level=None)
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


def is_full_row(conn, y, zoom):
    sql = f"select count(x) as size, y from z{zoom} where y = {y}  group by y "
    c = conn.cursor()
    c.execute(sql)
    rows = c.fetchall()
    if not rows:
        return 0
    return rows[0][0] == (2 ** zoom)


def is_tile_exists(conn, x, y, zoom):
    # Fixed SQL injection vulnerability with parameterized query
    sql = f"SELECT EXISTS(SELECT 1 FROM z{zoom} WHERE x=? AND y=? LIMIT 1);"
    c = conn.cursor()
    c.execute(sql, (x, y))
    rows = c.fetchall()
    return rows[0][0] == 1


class Repository:
    def __init__(self, output_dir):
        self.conn = None
        self.output_dir = output_dir

    def open(self):
        self.conn = make_connection(self.output_dir)

    async def commit(self):
        async with db_lock:
            self.conn.commit()

    def create_table(self, zoom):
        create_table(self.conn, zoom)

    async def save(self, x, y, zoom, content):
        await save_in_db(self.conn, x, y, zoom, content)

    def is_exists(self, x, y, zoom):
        return is_tile_exists(self.conn, x, y, zoom)

    def is_full_row(self, y, zoom):
        return is_full_row(self.conn, y, zoom)  # Fixed: removed circular reference

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
