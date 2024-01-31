import sqlite3
from sqlite3 import Error

import asyncio
import aiohttp
import pickle
import os

conn = None

DB_FILE = "tiles.db3"
pickle_lock = asyncio.Lock()
db_lock = asyncio.Lock()


class Settings:
    FILE_NAME = "settings.pickle"

    def __init__(self):
        self.current_zoom = 1
        self.current_cell = -1
        self.buffered_cells = set()


SETTINGS = Settings()


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


async def save_in_db(x, y, zoom, data, percent):
    task_1 = (x, y, data, 'png')
    sql = f'INSERT OR IGNORE INTO z{zoom}(x,y,image, ext) VALUES(?,?,?,?)'

    async with db_lock:
        cur = conn.cursor()
        cur.execute(sql, task_1)
        conn.commit()


async def save_in_pickle(x, y, zoom):
    if zoom < SETTINGS.current_zoom:
        return

    size = 2 ** zoom
    index = y * size + x

    if index < SETTINGS.current_cell:
        return

    async with pickle_lock:
        if SETTINGS.current_cell == index - 1:
            SETTINGS.current_cell = index
        else:
            SETTINGS.buffered_cells.add(index)

        if SETTINGS.buffered_cells:
            buffered = list(SETTINGS.buffered_cells)
            while buffered and buffered[0] == SETTINGS.current_cell + 1:
                SETTINGS.current_cell += 1
                del buffered[0]
                SETTINGS.buffered_cells = set(buffered)

        if SETTINGS.current_cell + 1 == size * size:
            SETTINGS.current_cell = -1
            SETTINGS.current_zoom += 1

        with open(Settings.FILE_NAME, 'wb') as file:
            pickle.dump(SETTINGS, file)


def is_tile_exsists(x, y, zoom):
    sql = f"SELECT EXISTS(SELECT 1 FROM z{zoom} WHERE x=\"{x}\" and y=\"{y}\" LIMIT 1);"
    c = conn.cursor()
    c.execute(sql)
    rows = c.fetchall()
    return rows[0][0] == 1


async def download_tile(x, y, zoom, percent):
    url = f"https://core-sat.maps.yandex.net/tiles?l=sat&x={x}&y={y}&z={zoom}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            content = await resp.read()

            try:
                await save_in_db(x, y, zoom, content, percent)
                await save_in_pickle(x, y, zoom)
                print(f"[{percent:.2f}%]    Saved [{zoom}]:{x}x{y} ")
            except Error as e:
                print(e)


async def download_bucket(buckets):
    await asyncio.gather(*[download_tile(*bucket) for bucket in buckets])


async def download(zoom):
    max_size = 2 ** zoom
    current = 0
    total = max_size * max_size

    bucket = []

    for x in range(max_size):
        for y in range(max_size):
            current += 1

            if is_tile_exsists(x, y, zoom):
                save_in_pickle(x, y, zoom)
                continue

            percent = current / total * 100.0
            bucket.append((x, y, zoom, percent))

            if len(bucket) > 10:
                await download_bucket(bucket)
                bucket.clear()

    await download_bucket(bucket)


if __name__ == "__main__":
    if os.path.exists(Settings.FILE_NAME):
        with open(Settings.FILE_NAME, 'rb') as file:
            SETTINGS = pickle.load(file)

    try:
        conn = sqlite3.connect(DB_FILE)
        create_table(conn, SETTINGS.current_zoom)

        asyncio.run(download(SETTINGS.current_zoom))
    except Error as e:
        print(e)

    print("Finished")
