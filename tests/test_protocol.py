"""Этап 2. Тесты бинарного фрейма сабмита (protocol)."""

import os
import random

import protocol
from protocol import ProtocolError


def _random_tiles(n, rng):
    tiles = []
    for i in range(n):
        x = rng.randint(0, 65535)
        y = rng.randint(0, 65535)
        ext = rng.choice([b'png', b'jpg', b'webp', b''])
        data = bytes(rng.getrandbits(8) for _ in range(rng.randint(1, 2048)))
        tiles.append((x, y, ext, data))
    return tiles


# --------------------------------------------------------------- round-trip

def test_round_trip_various_sizes():
    rng = random.Random(42)
    for n in (0, 1, 5, 100):
        tiles = _random_tiles(n, rng)
        zoom = rng.randint(0, 14)
        body = protocol.encode_submit(zoom, tiles)
        z2, tiles2 = protocol.decode_submit(body)
        assert z2 == zoom
        assert len(tiles2) == n
        for (x, y, ext, data), (x2, y2, ext2, data2) in zip(tiles, tiles2):
            assert (x, y) == (x2, y2)
            assert ext == ext2        # bytes байт-в-байт
            assert data == data2      # bytes байт-в-байт


def test_round_trip_accepts_str_ext():
    tiles = [(0, 0, 'png', b'img')]
    body = protocol.encode_submit(3, tiles)
    _, tiles2 = protocol.decode_submit(body)
    assert tiles2[0][2] == b'png'
    assert tiles2[0][3] == b'img'


# ----------------------------------------------------------------- битые входы

def _expect_error(body, **kw):
    try:
        protocol.decode_submit(body, **kw)
    except ProtocolError:
        return
    raise AssertionError("expected ProtocolError")


def test_bad_magic():
    _expect_error(b'XXXX' + b'\x00' * 16)


def test_truncated_frame():
    tiles = [(0, 0, b'png', b'img')]
    body = protocol.encode_submit(1, tiles)
    for cut in range(1, len(body)):
        _expect_error(body[:cut])


def test_count_exceeds_limit():
    tiles = [(0, 0, b'png', b'img')]
    body = protocol.encode_submit(1, tiles)
    _expect_error(body, max_tiles=0)


def test_trailing_bytes():
    body = protocol.encode_submit(1, [(0, 0, b'png', b'img')])
    _expect_error(body + b'EXTRA')


def test_empty_data_rejected_on_encode():
    try:
        protocol.encode_submit(1, [(0, 0, b'png', b'')])
    except ProtocolError:
        return
    raise AssertionError("expected ProtocolError for empty data")


def test_ext_too_long():
    # ext_len = 17 > MAX_EXT_LEN
    body = protocol.MAGIC + (1).to_bytes(4, 'big') + (1).to_bytes(4, 'big')
    body += (0).to_bytes(4, 'big') + (0).to_bytes(4, 'big') + (17).to_bytes(4, 'big')
    body += b'x' * 17 + (4).to_bytes(4, 'big') + b'img'
    _expect_error(body)


def test_zero_data_len():
    body = protocol.MAGIC + (1).to_bytes(4, 'big') + (1).to_bytes(4, 'big')
    body += (0).to_bytes(4, 'big') + (0).to_bytes(4, 'big') + (3).to_bytes(4, 'big')
    body += b'png' + (0).to_bytes(4, 'big')   # data_len = 0
    _expect_error(body)


def test_tile_too_large():
    # data_len > MAX_TILE_BYTES (декларируем, но байты не дописываем — лимит сработает раньше)
    body = protocol.MAGIC + (1).to_bytes(4, 'big') + (1).to_bytes(4, 'big')
    body += (0).to_bytes(4, 'big') + (0).to_bytes(4, 'big') + (3).to_bytes(4, 'big')
    body += b'png' + (protocol.MAX_TILE_BYTES + 1).to_bytes(4, 'big')
    _expect_error(body)


def test_max_request_bytes_limit():
    body = protocol.encode_submit(1, [(0, 0, b'png', b'img')])
    _expect_error(body, max_request_bytes=len(body) - 1)


def test_empty_tiles_round_trip():
    body = protocol.encode_submit(2, [])
    z, tiles = protocol.decode_submit(body)
    assert z == 2
    assert tiles == []
