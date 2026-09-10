import asyncio
import os
import sqlite3

import db


def open_repo(tmp_path, zoom):
    r = db.Repository(str(tmp_path))
    r.open(zoom)
    r.create_table(zoom)
    return r


def tiles_file(tmp_path, zoom):
    return str(tmp_path / f"tiles_{zoom}.db3")


def test_two_repositories_do_not_mix_buffers(tmp_path):
    r1 = open_repo(tmp_path, 1)
    r2 = open_repo(tmp_path, 2)

    async def go():
        await r1.save(0, 0, 1, b"tile-z1")
        await r2.save(0, 0, 2, b"tile-z2")
        await r1.flush_buffer()
        await r2.flush_buffer()

    asyncio.run(go())

    assert r1.is_exists(0, 0, 1)
    assert r2.is_exists(0, 0, 2)
    assert r1.row_count(0, 1) == 1
    assert r2.row_count(0, 2) == 1

    conn1 = sqlite3.connect(tiles_file(tmp_path, 1))
    assert conn1.execute("SELECT image FROM z1 WHERE x=0 AND y=0").fetchone()[0] == b"tile-z1"
    conn1.close()
    conn2 = sqlite3.connect(tiles_file(tmp_path, 2))
    assert conn2.execute("SELECT image FROM z2 WHERE x=0 AND y=0").fetchone()[0] == b"tile-z2"
    conn2.close()


def test_buffer_does_not_leak_to_other_zoom_file(tmp_path):
    # Старый латентный баг: глобальный буфер флешился через чужое соединение.
    r1 = open_repo(tmp_path, 1)
    r2 = open_repo(tmp_path, 2)

    async def go():
        await r1.save(0, 0, 1, b"tile-z1")
        await r1.save(1, 0, 1, b"tile-z1b")
        await r2.save(0, 0, 2, b"tile-z2")
        # Флешим только буфер r1: тайлы z2 обязаны остаться в буфере r2
        await r1.flush_buffer()

    asyncio.run(go())

    assert r1.row_count(0, 1) == 2
    assert len(r2.buffer) == 1
    conn2 = sqlite3.connect(tiles_file(tmp_path, 2))
    assert conn2.execute("SELECT COUNT(*) FROM z2").fetchone()[0] == 0
    conn2.close()


def test_insert_ignore_many_counts_and_is_idempotent(tmp_path):
    r = open_repo(tmp_path, 2)

    batch1 = [(x, y, f"img-{x}-{y}".encode(), "png") for y in range(3) for x in range(4)]
    assert r.insert_ignore_many(2, batch1) == 12
    # Повторная вставка тех же тайлов — 0 изменений
    assert r.insert_ignore_many(2, batch1) == 0

    batch2 = [(x, 3, f"img-{x}-3".encode(), "png") for x in range(4)]
    assert r.insert_ignore_many(2, batch2) == 4
    assert r.row_count(3, 2) == 4

    # Дубликат с другими данными не перетирает оригинал
    assert r.insert_ignore_many(2, [(0, 0, b"overwritten", "png")]) == 0
    row = r.conn.execute("SELECT image FROM z2 WHERE x=0 AND y=0").fetchone()
    assert row[0] == b"img-0-0"

    assert r.insert_ignore_many(2, []) == 0


def test_row_count_and_is_full_row_on_partial_table(tmp_path):
    r = open_repo(tmp_path, 2)

    assert r.row_count(0, 2) == 0
    assert r.is_full_row(0, 2) is False

    r.insert_ignore_many(2, [(x, 0, b"a", "png") for x in range(2)])
    assert r.row_count(0, 2) == 2
    assert r.is_full_row(0, 2) is False

    r.insert_ignore_many(2, [(x, 0, b"a", "png") for x in range(2, 4)])
    assert r.row_count(0, 2) == 4
    assert r.is_full_row(0, 2) is True


def test_add_y_index_created_and_used_in_query_plan(tmp_path):
    r = open_repo(tmp_path, 2)  # create_table уже создаёт индекс для новых БД
    r.add_y_index(2)  # идемпотентно

    plan = " ".join(
        row[3]
        for row in r.conn.execute(
            "EXPLAIN QUERY PLAN SELECT COUNT(x) FROM z2 WHERE y = ?", (0,)
        ).fetchall()
    )
    assert "idx_z2_y" in plan


def test_add_y_index_on_existing_db_without_index(tmp_path):
    # Имитация старой БД: таблица без индекса
    conn = db.make_connection(2, str(tmp_path))
    conn.execute(
        "CREATE TABLE z2 (x INTEGER NOT NULL, y INTEGER NOT NULL, "
        "image BLOB NOT NULL, ext TEXT NOT NULL, PRIMARY KEY (x, y))"
    )
    conn.close()

    # create_table добавит индекс, поэтому открываем репозиторий без него
    r = db.Repository(str(tmp_path))
    r.open(2)

    plan_before = " ".join(
        row[3]
        for row in r.conn.execute(
            "EXPLAIN QUERY PLAN SELECT COUNT(x) FROM z2 WHERE y = ?", (0,)
        ).fetchall()
    )
    assert "idx_z2_y" not in plan_before

    r.add_y_index(2)

    plan_after = " ".join(
        row[3]
        for row in r.conn.execute(
            "EXPLAIN QUERY PLAN SELECT COUNT(x) FROM z2 WHERE y = ?", (0,)
        ).fetchall()
    )
    assert "idx_z2_y" in plan_after


def test_wal_enabled(tmp_path):
    r = open_repo(tmp_path, 1)
    mode = r.conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal"


def test_close_is_idempotent(tmp_path):
    r = open_repo(tmp_path, 1)
    r.close()
    r.close()  # не должен падать
    assert r.conn is None
    assert r.buffer == []
