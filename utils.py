
def get_index(x, y, zoom):
    max_size = 2 ** zoom
    return y * max_size + x


MINUTE = 60 * 60
HOUR = 60 * MINUTE
DAY = HOUR * 24


def humanized_time(secs):
    return f"{secs:.2f} sec."