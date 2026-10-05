"""Бинарный фрейм сабмита тайлов: encode/decode + лимиты.

Общий для клиента, сервера и тестов. Формат (big-endian):

    MAGIC     4 байта
    zoom      u32
    count     u32
    ── count раз ──
    x         u32
    y         u32
    ext_len   u32
    ext       ext_len байт (ASCII)
    data_len  u32      (> 0)
    data      data_len байт (image)

ext возвращается из decode как bytes (round-trip байт-в-байт).
"""

import struct

MAGIC = b'TLM1'
MAX_EXT_LEN = 16
MAX_TILE_BYTES = 4 * 1024 * 1024            # 4 МБ на тайл
DEFAULT_MAX_TILES = 1_000_000
DEFAULT_MAX_REQUEST_BYTES = 256 * 1024 * 1024  # 256 МБ


class ProtocolError(Exception):
    """Некорректный фрейм (сервер отдаёт 400 invalid frame)."""


def encode_submit(zoom, tiles):
    """tiles: список (x, y, ext, data). ext — str или bytes. Возвращает bytes."""
    buf = bytearray()
    buf += MAGIC
    buf += struct.pack('>II', zoom, len(tiles))
    for (x, y, ext, data) in tiles:
        if isinstance(ext, str):
            ext = ext.encode('ascii')
        if not data:
            raise ProtocolError('empty tile data')
        buf += struct.pack('>III', x, y, len(ext))
        buf += ext
        buf += struct.pack('>I', len(data))
        buf += data
    return bytes(buf)


def decode_submit(body, max_tiles=DEFAULT_MAX_TILES,
                  max_request_bytes=DEFAULT_MAX_REQUEST_BYTES):
    """Возвращает (zoom, tiles). tiles: список (x, y, ext_bytes, data)."""
    if len(body) > max_request_bytes:
        raise ProtocolError('request too large')
    if len(body) < 4 or body[:4] != MAGIC:
        raise ProtocolError('bad magic')

    off = 4
    if len(body) < off + 8:
        raise ProtocolError('truncated zoom/count')
    zoom, count = struct.unpack_from('>II', body, off)
    off += 8
    if count > max_tiles:
        raise ProtocolError('too many tiles')

    tiles = []
    for _ in range(count):
        if len(body) < off + 12:
            raise ProtocolError('truncated tile header')
        x, y, ext_len = struct.unpack_from('>III', body, off)
        off += 12
        if ext_len > MAX_EXT_LEN:
            raise ProtocolError('ext too long')
        if len(body) < off + ext_len:
            raise ProtocolError('truncated ext')
        ext = body[off:off + ext_len]
        off += ext_len
        if len(body) < off + 4:
            raise ProtocolError('truncated data_len')
        (data_len,) = struct.unpack_from('>I', body, off)
        off += 4
        if data_len == 0:
            raise ProtocolError('empty data')
        if data_len > MAX_TILE_BYTES:
            raise ProtocolError('tile too large')
        if len(body) < off + data_len:
            raise ProtocolError('truncated data')
        data = body[off:off + data_len]
        off += data_len
        tiles.append((x, y, ext, data))

    if off != len(body):
        raise ProtocolError('trailing bytes')
    return zoom, tiles
