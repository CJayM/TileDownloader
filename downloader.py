import sqlite3
from sqlite3 import Error

import asyncio
import aiohttp

conn = None


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


def save_in_db(x, y, zoom, data, percent):
    task_1 = (x, y, data, 'png')
    sql = f'INSERT OR IGNORE INTO z{zoom}(x,y,image, ext) VALUES(?,?,?,?)'
    cur = conn.cursor()
    cur.execute(sql, task_1)
    conn.commit()
    print(f"[{percent:.2f}%]    Saved [{zoom}]:{x}x{y} ")


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
                save_in_db(x, y, zoom, content, percent)
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
                continue

            percent = current / total * 100.0
            bucket.append((x, y, zoom, percent))

            if len(bucket) > 10:
                await download_bucket(bucket)
                bucket.clear()

    await download_bucket(bucket)


if __name__ == "__main__":
    db_file = "World.db3"

    zoom = 7

    try:
        conn = sqlite3.connect(db_file)
        create_table(conn, zoom)

        asyncio.run(download(zoom))
    except Error as e:
        print(e)

    print("Finished")
