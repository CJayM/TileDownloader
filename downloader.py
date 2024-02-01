import sqlite3
import time
from sqlite3 import Error

import asyncio
import aiohttp
import pickle
import os
import sys

conn = None

DB_FILE = "tiles.db3"
pickle_lock = asyncio.Lock()
db_lock = asyncio.Lock()

MAX_ZOOM = 12


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


async def save_in_db(x, y, zoom, data):
    task_1 = (x, y, data, 'png')
    sql = f'INSERT OR IGNORE INTO z{zoom}(x,y,image, ext) VALUES(?,?,?,?)'

    async with db_lock:
        cur = conn.cursor()
        cur.execute(sql, task_1)
        conn.commit()


last_save = time.time()


def save_state():
    global last_save

    with open(Settings.FILE_NAME, 'wb') as file:
        pickle.dump(SETTINGS, file)
    last_save = time.time()


def get_index(x, y, zoom):
    max_size = 2 ** zoom
    return y * max_size + x


async def save_in_pickle(x, y, zoom):
    if zoom < SETTINGS.current_zoom:
        return

    size = 2 ** zoom
    index = get_index(x, y, zoom)

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

        global last_save
        delta = time.time() - last_save
        if delta > 60:
            save_state()
        else:
            sys.stdout.write("\033[F")
            print("Index:", index)


def is_tile_exists(x, y, zoom):
    sql = f"SELECT EXISTS(SELECT 1 FROM z{zoom} WHERE x=\"{x}\" and y=\"{y}\" LIMIT 1);"
    c = conn.cursor()
    c.execute(sql)
    rows = c.fetchall()
    return rows[0][0] == 1


async def download_tile(x, y, zoom, percent):
    url = f"https://core-sat.maps.yandex.net/tiles?l=sat&x={x}&y={y}&z={zoom}"

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                content = await resp.read()

                try:
                    await save_in_db(x, y, zoom, content)
                    await save_in_pickle(x, y, zoom)
                except Error as e:
                    print(e)
        except aiohttp.client_exceptions.ClientConnectorError as conn_error:
            print(".", end=" ")
            await asyncio.sleep(30)


start_time = time.time()


async def download_bucket(buckets):
    await asyncio.gather(*[download_tile(*bucket) for bucket in buckets])

    if not buckets:
        return

    global start_time
    buckets.sort(key=lambda bucket: get_index(bucket[0], bucket[1], bucket[2]))
    x, y, zoom, percent = buckets[-1]
    now = time.time()
    total = now - start_time
    speed = total / len(buckets)
    if speed != 0:
        speed = 1.0 / speed
    print(f"[{percent:.2f}%]    Saved [{zoom}]:{x}x{y}  count:{len(buckets)}  TPS:{speed:.0f}")
    start_time = time.time()


async def download_zoom(zoom):
    max_size = 2 ** zoom
    current = 0
    total = max_size * max_size

    bucket = []

    for y in range(max_size):
        for x in range(max_size):
            current += 1

            if is_tile_exists(x, y, zoom):
                await save_in_pickle(x, y, zoom)
                continue

            percent = current / total * 100.0
            bucket.append((x, y, zoom, percent))

            if len(bucket) > 9:
                await download_bucket(bucket)
                bucket.clear()

    await download_bucket(bucket)


def find_start(zoom):
    max_size = 2 ** zoom
    current = 0

    for y in range(max_size):
        for x in range(max_size):
            current += 1
            index = get_index(x, y, zoom)
            if is_tile_exists(x, y, zoom) == False:
                SETTINGS.current_cell = index
                break
            
            sys.stdout.write("\033[F")
            print("Index:", index)


if __name__ == "__main__":
    if os.path.exists(Settings.FILE_NAME):
        with open(Settings.FILE_NAME, 'rb') as file:
            SETTINGS = pickle.load(file)

    while SETTINGS.current_zoom <= MAX_ZOOM:
        try:
            conn = sqlite3.connect(DB_FILE, isolation_level=None)
            conn.execute('pragma journal_mode=wal')
            create_table(conn, SETTINGS.current_zoom)

            print("Check ZOOM", SETTINGS.current_zoom)
            print("")
            find_start(SETTINGS.current_zoom)
            size = 2 ** SETTINGS.current_zoom
            max_index = size * size - 1
            if SETTINGS.current_cell != max_index:
                print("Download at ZOOM", SETTINGS.current_zoom)
                print("")
                asyncio.run(download_zoom(SETTINGS.current_zoom))
            else:
                SETTINGS.current_cell = -1
                SETTINGS.current_zoom += 1

            save_state()
        except Error as e:
            print(e)

    print("Finished")
