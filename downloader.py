import time
import argparse
import sys
import os

# Add the current directory to the Python path to ensure modules can be found
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

import asyncio
import aiohttp
import pickle
from sqlite3 import Error

import db
import utils

# Parse command line arguments
parser = argparse.ArgumentParser(description='Tile Downloader')
parser.add_argument('-o', '--output-dir', default='.', help='Output directory for database and settings (default: current directory)')
parser.add_argument('--threads', type=int, default=4, help='Number of concurrent download threads (default: 4)')
args = parser.parse_args()

# Initialize repository with output directory
repo = db.Repository(args.output_dir)

pickle_lock = asyncio.Lock()

MAX_ZOOM = 14


class Settings:
    def __init__(self, output_dir='.'):
        self.output_dir = output_dir
        self.FILE_NAME = os.path.join(output_dir, "settings.pickle")
        self.current_zoom = 1
        self.current_cell = -1
        self.buffered_cells = set()


SETTINGS = Settings(args.output_dir)

last_save = time.time()


def save_state():
    global last_save

    with open(SETTINGS.FILE_NAME, 'wb') as file:
        pickle.dump(SETTINGS, file)
    last_save = time.time()
    print("\t\tState saved")


async def save_in_pickle(x, y, zoom):
    if zoom < SETTINGS.current_zoom:
        return

    size = 2 ** zoom
    index = utils.get_index(x, y, zoom)

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
            await repo.commit()
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
                    await repo.save(x, y, zoom, content)
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


async def download_bucket(buckets):
    await asyncio.gather(*[download_tile(*bucket) for bucket in buckets])

    if not buckets:
        return

    global start_time
    buckets.sort(key=lambda bucket: utils.get_index(bucket[0], bucket[1], bucket[2]))
    x, y, zoom, percent = buckets[-1]
    now = time.time()
    total = now - start_time
    speed = total / len(buckets)
    if speed != 0:
        speed = 1.0 / speed

    total_count = (2 ** SETTINGS.current_zoom) ** 2
    elapsed_count = total_count - SETTINGS.current_cell
    elapsed_secs = elapsed_count / speed
    elapsed_text = utils.humanized_time(elapsed_secs)

    percent = float(utils.get_index(x, y, zoom)) / total_count * 100.0

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
        print(f"Zoom {zoom} - Check row", y)
        if repo.is_full_row(y, zoom):
            start_x = 0
            continue

        for x in range(start_x, max_size):
            current += 1

            if repo.is_exists(x, y, zoom):
                await save_in_pickle(x, y, zoom)
                continue

            percent = current / total * 100.0
            bucket.append((x, y, zoom, percent))

            if len(bucket) >= args.threads:
                await download_bucket(bucket)
                bucket.clear()
        start_x = 0

    await download_bucket(bucket)


if __name__ == "__main__":
    if os.path.exists(SETTINGS.FILE_NAME):
        with open(SETTINGS.FILE_NAME, 'rb') as file:
            loaded_settings = pickle.load(file)
            # Update the loaded settings with the output directory
            loaded_settings.output_dir = args.output_dir
            loaded_settings.FILE_NAME = os.path.join(args.output_dir, "settings.pickle")
            SETTINGS = loaded_settings

    while SETTINGS.current_zoom <= MAX_ZOOM:
        try:
            repo.open()
            repo.create_table(SETTINGS.current_zoom)

            print("Check ZOOM", SETTINGS.current_zoom)
            print("")
            current = repo.find_start(SETTINGS.current_zoom, SETTINGS.current_cell)
            # current = 0
            size = 2 ** SETTINGS.current_zoom
            max_index = size * size - 1
            if current < max_index:
                SETTINGS.current_cell = current
                print("Download at ZOOM", SETTINGS.current_zoom)
                print("")
                asyncio.run(download_zoom(SETTINGS.current_zoom))

            if current >= max_index:
                SETTINGS.current_cell = -1
                SETTINGS.current_zoom += 1

            save_state()
            asyncio.run(repo.commit())

        except Error as e:
            print(e)

    print("Finished")