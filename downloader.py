"""
https://core-sat.maps.yandex.net/tiles?l=sat&v=3.1142.0&x=2460&y=1362&z=12&scale=1.25&lang=ru_RU&client_id=yandex-web-maps
https://core-sat.maps.yandex.net/tiles?l=sat&x=2460&y=1362&z=12
"""
import sqlite3
from sqlite3 import Error

import requests

conn = None


def create_table(conn, zoom):
    create_table_sql = f"""CREATE TABLE z{zoom} (
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


def save_in_db(x, y, zoom, data):
    task_1 = (x, y, data, 'png')
    sql = f'INSERT OR IGNORE INTO z{zoom}(x,y,image, ext) VALUES(?,?,?,?)'
    cur = conn.cursor()
    cur.execute(sql, task_1)
    conn.commit()
    print("Saved")


def is_tile_exsists(x, y, zoom):
    sql = f"SELECT EXISTS(SELECT 1 FROM z{zoom} WHERE x=\"{x}\" and y=\"{y}\" LIMIT 1);"
    c = conn.cursor()
    c.execute(sql)
    rows = c.fetchall()
    return rows[0][0] == 1


def download(zoom):
    max_size = 2 ** zoom
    current = 0
    total = max_size * max_size
    for x in range(max_size):
        for y in range(max_size):
            if is_tile_exsists(x, y, zoom):
                continue

            percent = current / total * 100.0
            print(f"Downloading [{zoom}]:{x}x{y}  [{percent}%]")
            URL = f"https://core-sat.maps.yandex.net/tiles?l=sat&x={x}&y={y}&z={zoom}"

            try:
                response = requests.get(URL)
                save_in_db(x, y, zoom, response.content)
            except Error as e:
                print(e)

            current += 1


if __name__ == "__main__":
    db_file = "World.db3"

    zoom = 4

    try:
        conn = sqlite3.connect(db_file)
        create_table(conn, zoom)
        download(zoom)
    except Error as e:
        print(e)
