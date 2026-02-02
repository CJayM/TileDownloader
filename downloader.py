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
parser.add_argument('-z', '--zoom', default=None, help='Zoom level (default: None)')
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

    def next_zoom(self):
        self.current_cell = -1
        self.current_zoom += 1
        self.buffered_cells = set()


async def clean_buffered_cells(settings, repo):
    """
    Проверяет, сохранены ли тайлы из buffered_cells в базе данных,
    и удаляет их из buffered_cells, если они уже сохранены.
    Также обновляет current_cell, если есть последовательные тайлы в начале buffered_cells.
    """
    if not settings.buffered_cells:
        return

    total_cells = len(settings.buffered_cells)
    print(f"Cleaning {total_cells} buffered cells...")

    # Создаем отсортированный список buffered_cells для эффективной обработки
    sorted_buffered = sorted(settings.buffered_cells)

    # Проверяем, можно ли продвинуть current_cell за счет уже сохраненных тайлов
    # Начинаем с текущего current_cell и смотрим, есть ли последовательные тайлы в buffered_cells
    new_current_cell = settings.current_cell

    for i, cell_index in enumerate(sorted_buffered):
        # Выводим прогресс каждые 100 элементов или если это первый или последний элемент
        if i == 0 or i % 100 == 0 or i == total_cells - 1:
            progress_idx = i + 1
            percent = (progress_idx / total_cells) * 100
            print(f"Checking index {progress_idx} of {total_cells} ({percent:.2f}%)")

        # Проверяем, существует ли тайл в базе данных
        size = 2 ** settings.current_zoom
        y = cell_index // size
        x = cell_index % size

        if repo.is_exists(x, y, settings.current_zoom):
            # Если тайл уже существует в базе, удаляем его из buffered_cells
            settings.buffered_cells.discard(cell_index)

            # Проверяем, можем ли мы продвинуть current_cell
            # Если cell_index - это следующий ожидаемый индекс, то продвигаем current_cell
            if cell_index == new_current_cell + 1:
                new_current_cell = cell_index
                # Продолжаем проверять, может быть, дальше тоже есть последовательные тайлы
                while new_current_cell + 1 in settings.buffered_cells:
                    if repo.is_exists(new_current_cell + 1, 0, settings.current_zoom):  # Проверяем, что тайл реально существует
                        # Нужно проверить координаты для следующего индекса
                        next_y = (new_current_cell + 1) // size
                        next_x = (new_current_cell + 1) % size
                        if repo.is_exists(next_x, next_y, settings.current_zoom):
                            settings.buffered_cells.discard(new_current_cell + 1)
                            new_current_cell += 1
                        else:
                            break
                    else:
                        break

    # Обновляем current_cell, если удалось продвинуть
    if new_current_cell > settings.current_cell:
        settings.current_cell = new_current_cell
        print(f"Updated current_cell to {new_current_cell}")

    removed_count = total_cells - len(settings.buffered_cells)
    if removed_count > 0:
        print(f"Removed {removed_count} already saved tiles from buffered_cells")
    else:
        print("No tiles were removed from buffered_cells")


SETTINGS = Settings(args.output_dir)

last_save = time.time()
tiles_processed_since_clean = 0  # Счетчик тайлов с момента последней очистки


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

        # Обновляем current_cell на основе последовательных тайлов в buffered_cells
        while SETTINGS.current_cell + 1 in SETTINGS.buffered_cells:
            # Проверяем, что следующий тайл действительно существует в базе данных
            size = 2 ** SETTINGS.current_zoom
            next_index = SETTINGS.current_cell + 1
            next_y = next_index // size
            next_x = next_index % size

            if repo.is_exists(next_x, next_y, SETTINGS.current_zoom):
                SETTINGS.current_cell = next_index
                SETTINGS.buffered_cells.discard(next_index)  # Удаляем из буфера, так как уже учтён
            else:
                break

        global last_save, tiles_processed_since_clean
        tiles_processed_since_clean += 1

        # Вызываем очистку buffered_cells каждые 10000 тайлов
        if tiles_processed_since_clean >= 10000:
            print("Performing periodic clean of buffered cells...")
            temp_repo = db.Repository(args.output_dir)
            temp_repo.open(SETTINGS.current_zoom)
            await clean_buffered_cells(SETTINGS, temp_repo)
            temp_repo.close()
            tiles_processed_since_clean = 0  # Сбрасываем счетчик
            save_state()  # Сохраняем обновленное состояние

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
        percentage = (y / max_size) * 100 if max_size > 0 else 0
        print(f"Zoom {zoom} - Check row {y}/{max_size} ({percentage:.2f}%)")
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
            if args.zoom:
                loaded_settings.current_zoom = int(args.zoom)
            SETTINGS = loaded_settings

        # Clean buffered cells that may have already been saved to the database
        temp_repo = db.Repository(args.output_dir)
        temp_repo.open(SETTINGS.current_zoom)
        asyncio.run(clean_buffered_cells(SETTINGS, temp_repo))
        temp_repo.close()  # Закрываем соединение после очистки

        # Сохраняем обновленные настройки после очистки buffered_cells
        save_state()

    while int(SETTINGS.current_zoom) <= MAX_ZOOM:
        try:
            repo.open(SETTINGS.current_zoom)
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
                SETTINGS.next_zoom()                

            save_state()
            asyncio.run(repo.commit())

        except Error as e:
            print(e)

    print("Finished")
    
    # Flush any remaining records in the buffer
    asyncio.run(repo.flush_buffer())