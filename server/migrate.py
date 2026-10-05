"""Миграция legacy-базы tiles.db3 (все зумы в одном файле) в per-zoom tiles_{zoom}.db3.

Новый сервер (см. db.py) открывает только tiles_{zoom}.db3 и полностью
игнорирует tiles.db3, поэтому тайлы из него нужно перенести.

Перенос идемпотентен и возобновляем: данные вставляются чанками по x
(PK — (x, y), поэтому диапазон по x идёт по индексу), и чанк пропускается,
если в приёмнике уже столько же строк, сколько в источнике. Повторный запуск
ничего не дублирует и не перечитывает готовое.

Схема и раскладка результата совпадают с тем, что создаёт db.Repository.
"""

import argparse
import os
import sqlite3
import sys

CREATE_TABLE_SQL = """CREATE TABLE IF NOT EXISTS z{zoom} (
    x     INTEGER NOT NULL,
    y     INTEGER NOT NULL,
    image BLOB    NOT NULL,
    ext   TEXT    NOT NULL,
    PRIMARY KEY (x, y)
)"""

CHUNK_X = 256


def parse_zooms(text):
    zooms = set()
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            lo, hi = part.split("-", 1)
            zooms.update(range(int(lo), int(hi) + 1))
        else:
            zooms.add(int(part))
    return sorted(zooms)


def legacy_has_table(conn, zoom):
    row = conn.execute(
        "SELECT 1 FROM legacy.sqlite_master WHERE type='table' AND name=?",
        (f"z{zoom}",)).fetchone()
    return row is not None


def count_range(conn, table, x0, x1):
    return conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE x >= ? AND x < ?",
        (x0, x1)).fetchone()[0]


def migrate_zoom(legacy_path, output_dir, zoom, journal_mode):
    target_path = os.path.join(output_dir, f"tiles_{zoom}.db3")
    conn = sqlite3.connect(target_path, isolation_level=None)
    try:
        conn.execute(f"pragma journal_mode={journal_mode}")
        conn.execute("pragma synchronous=off")
        conn.execute(CREATE_TABLE_SQL.format(zoom=zoom))
        conn.execute("ATTACH DATABASE ? AS legacy", (legacy_path,))

        if not legacy_has_table(conn, zoom):
            return None

        size = 2 ** zoom
        added = 0
        for x0 in range(0, size, CHUNK_X):
            x1 = min(x0 + CHUNK_X, size)
            percent = x1 / size * 100
            src = count_range(conn, f"legacy.z{zoom}", x0, x1)
            dst = count_range(conn, f"z{zoom}", x0, x1)
            if src == dst:
                print(f"  z{zoom}: x {x0}..{x1 - 1} ({percent:5.1f}%) "
                      f"— уже есть, пропуск", flush=True)
                continue

            before = conn.execute("SELECT total_changes()").fetchone()[0]
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    f"INSERT OR IGNORE INTO z{zoom}(x, y, image, ext) "
                    f"SELECT x, y, image, ext FROM legacy.z{zoom} "
                    f"WHERE x >= ? AND x < ?", (x0, x1))
                conn.execute("COMMIT")
            except sqlite3.Error:
                conn.execute("ROLLBACK")
                raise
            added += conn.execute("SELECT total_changes()").fetchone()[0] - before
            print(f"  z{zoom}: x {x0}..{x1 - 1} ({percent:5.1f}%)", flush=True)

        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_z{zoom}_y ON z{zoom}(y)")
        return added
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-s", "--source", default="tiles.db3",
                        help="legacy-база со всеми зумами (по умолчанию: tiles.db3)")
    parser.add_argument("-o", "--output-dir", default=".",
                        help="каталог с tiles_{zoom}.db3 (по умолчанию: .)")
    parser.add_argument("--zooms", default="6-11",
                        help="зумы, напр. '6-11' или '6,7,10' (по умолчанию: 6-11)")
    parser.add_argument("--journal", default="off", choices=("off", "wal", "memory"),
                        help="режим журнала приёмника (по умолчанию: off — быстрее, "
                             "меньше запись; защита от сбоя не нужна, источник не меняется)")
    args = parser.parse_args()

    if not os.path.exists(args.source):
        print(f"Источник не найден: {args.source}", file=sys.stderr)
        return 1
    os.makedirs(args.output_dir, exist_ok=True)

    zooms = parse_zooms(args.zooms)
    print(f"Источник: {os.path.abspath(args.source)}")
    print(f"Каталог:  {os.path.abspath(args.output_dir)}")
    print(f"Зумы:     {zooms}   журнал: {args.journal}")
    total_added = 0
    for zoom in zooms:
        print(f"z{zoom}: перенос...", flush=True)
        added = migrate_zoom(args.source, args.output_dir, zoom, args.journal)
        if added is None:
            print(f"z{zoom}: таблицы z{zoom} нет в источнике — пропуск")
            continue
        total_added += added
        print(f"z{zoom}: добавлено {added}")
    print(f"Итого добавлено: {total_added}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
