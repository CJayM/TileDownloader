import time

import asyncio
import aiohttp
import pickle
import os
import sys
from sqlite3 import Error

import db

conn = None

pickle_lock = asyncio.Lock()

MAX_ZOOM = 14
THREAD_COUNTS = 20


class Settings:
    FILE_NAME = "settings.pickle"

    def __init__(self):
        self.current_zoom = 1
        self.current_cell = -1
        self.buffered_cells = set()


SETTINGS = Settings()

last_save = time.time()


def save_state():
    global last_save

    with open(Settings.FILE_NAME, 'wb') as file:
        pickle.dump(SETTINGS, file)
    last_save = time.time()
    print("\t\tState saved")


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
            pass
            # sys.stdout.write("\033[F")
            # print("Index:", index)


async def download_tile(x, y, zoom, percent):
    url = f"https://core-sat.maps.yandex.net/tiles?l=sat&x={x}&y={y}&z={zoom}"

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                content = await resp.read()

                try:
                    await db.save_in_db(conn,x, y, zoom, content)
                    await save_in_pickle(x, y, zoom)
                except Error as e:
                    print(e)
        except aiohttp.client_exceptions.ClientConnectorError as conn_error:
            print(".", end=" ")
            await asyncio.sleep(30)
        except aiohttp.client_exceptions.ClientOSError as err:
            print("Превышен таймаут семафора")
            await asyncio.sleep(30)


start_time = time.time()

MINUTE = 60 * 60
HOUR = 60 * MINUTE
DAY = HOUR * 24


def humanized_time(secs):
    return f"{secs:.2f} sec."


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

    total_count = (2 ** SETTINGS.current_zoom) ** 2
    elapsed_count = total_count - SETTINGS.current_cell
    elapsed_secs = elapsed_count / speed
    elapsed_text = humanized_time(elapsed_secs)
    print(f"[{percent:.2f}%]    Downloaded [{zoom}]:{x}x{y}  count:{len(buckets)}  TPS:{speed:.0f}  [{elapsed_text}]")
    start_time = time.time()


async def download_zoom(zoom):
    max_size = 2 ** zoom
    current = 0
    total = max_size * max_size

    bucket = []

    start_y = 0
    start_x = 0
    if SETTINGS.current_cell != -1:
        start_y = SETTINGS.current_cell // max_size
        start_x = SETTINGS.current_cell - start_y * max_size

    for y in range(start_y, max_size):
        for x in range(start_x, max_size):
            current += 1

            if db.is_tile_exists(conn, x, y, zoom):
                await save_in_pickle(x, y, zoom)
                continue

            percent = current / total * 100.0
            bucket.append((x, y, zoom, percent))

            if len(bucket) >= THREAD_COUNTS:
                await download_bucket(bucket)
                bucket.clear()

    await download_bucket(bucket)


def find_start(zoom, start_cell):
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
            current = get_index(x, y, zoom)
            if db.is_tile_exists(conn, x, y, zoom) == False:
                break

            # percent = float(current) / total * 100.0
            # print(f"checked {current}/{total}  [{percent:.2f}%] ")

    return current


if __name__ == "__main__":
    if os.path.exists(Settings.FILE_NAME):
        with open(Settings.FILE_NAME, 'rb') as file:
            SETTINGS = pickle.load(file)

    while SETTINGS.current_zoom <= MAX_ZOOM:
        try:
            conn = db.make_connection()
            db.create_table(conn, SETTINGS.current_zoom)

            print("Check ZOOM", SETTINGS.current_zoom)
            print("")
            current = find_start(SETTINGS.current_zoom, SETTINGS.current_cell)
            size = 2 ** SETTINGS.current_zoom
            max_index = size * size - 1
            if current != max_index:
                SETTINGS.current_cell = current
                print("Download at ZOOM", SETTINGS.current_zoom)
                print("")
                asyncio.run(download_zoom(SETTINGS.current_zoom))

            if current == max_index:
                SETTINGS.current_cell = -1
                SETTINGS.current_zoom += 1

            save_state()
        except Error as e:
            print(e)

    print("Finished")
