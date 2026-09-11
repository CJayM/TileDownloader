"""Тесты локального буфера клиента (buffer.py).

По ТЗ §9/§10: put/чтение/очистка; переживание «рестарта» — новый инстанс
на том же файле видит данные.
"""

import os
import sys

import pytest

current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

import buffer  # noqa: E402


@pytest.fixture
def buf(tmp_path):
    b = buffer.ClientBuffer(str(tmp_path), 'c1')
    yield b
    b.close()


def test_put_and_all(buf):
    buf.put(2, 0, 0, b'img0', 'png', 5)
    buf.put(2, 1, 0, b'img1', 'jpg', 5)
    buf.put(2, 0, 1, b'img2', 'webp', 5)
    rows = buf.all_for_task(5)
    assert len(rows) == 3
    got = {(z, x, y) for (z, x, y, _d, _e) in rows}
    assert got == {(2, 0, 0), (2, 1, 0), (2, 0, 1)}
    d = {(z, x, y): (img, ext) for (z, x, y, img, ext) in rows}
    assert d[(2, 1, 0)] == (b'img1', 'jpg')


def test_count_and_meta(buf):
    assert buf.load_meta() is None
    buf.save_task_meta(7, 3, 10, 4)
    buf.put(3, 1, 2, b'a', 'png', 7)
    buf.put(3, 2, 2, b'b', 'png', 7)
    assert buf.count_for_task(7) == 2
    meta = buf.load_meta()
    assert meta == {'task_id': 7, 'zoom': 3, 'start_idx': 10, 'count': 4}


def test_put_many_upserts(buf):
    rows = [(2, 0, 0, b'x', 'png', 1), (2, 1, 0, b'y', 'png', 1)]
    buf.put_many(rows)
    # дубль по (zoom,x,y) — перезапись, а не ошибка
    buf.put_many([(2, 0, 0, b'x2', 'png', 1)])
    assert buf.count_for_task(1) == 2
    got = {(z, x, y): img for (z, x, y, img, _e) in buf.all_for_task(1)}
    assert got[(2, 0, 0)] == b'x2'


def test_clear_task(buf):
    buf.save_task_meta(9, 2, 0, 4)
    buf.put(2, 0, 0, b'a', 'png', 9)
    buf.put(2, 1, 0, b'b', 'png', 9)
    assert buf.count_for_task(9) == 2
    buf.clear_task(9)
    assert buf.count_for_task(9) == 0
    assert buf.all_for_task(9) == []


def test_survives_restart(tmp_path):
    b1 = buffer.ClientBuffer(str(tmp_path), 'c9')
    b1.save_task_meta(3, 2, 0, 4)
    b1.put(2, 0, 0, b'persist', 'png', 3)
    b1.close()

    # Новый инстанс на том же файле видит данные.
    b2 = buffer.ClientBuffer(str(tmp_path), 'c9')
    try:
        assert b2.load_meta() == {'task_id': 3, 'zoom': 2, 'start_idx': 0, 'count': 4}
        rows = b2.all_for_task(3)
        assert [(x, y, img, ext) for (_z, x, y, img, ext) in rows] == [(0, 0, b'persist', 'png')]
    finally:
        b2.close()


def test_isolation_by_client(tmp_path):
    a = buffer.ClientBuffer(str(tmp_path), 'cA')
    b = buffer.ClientBuffer(str(tmp_path), 'cB')
    a.put(2, 0, 0, b'A', 'png', 1)
    b.put(2, 0, 0, b'B', 'png', 1)
    assert a.count_for_task(1) == 1
    assert b.count_for_task(1) == 1
    assert a.all_for_task(1)[0][3] == b'A'
    assert b.all_for_task(1)[0][3] == b'B'
    a.close()
    b.close()
