
def get_index(x, y, zoom):
    size = 2 ** zoom
    return y * size + x


def get_xy(index, zoom):
    """Обратная к get_index: линейный индекс -> (x, y)."""
    size = 2 ** zoom
    return index % size, index // size


MINUTE = 60
HOUR = 60 * MINUTE
DAY = HOUR * 24


def humanized_time(secs):
    if secs < (5 * MINUTE):
        return f"{secs:.2f} sec."
    if secs <  HOUR:
        mins = int(secs // MINUTE)
        sec = int(secs % MINUTE)
        txt = "{mins:02}:{sec:02}".format(mins=mins,sec=sec)
        return txt
    if secs < DAY:
        hour = int(secs // HOUR)
        min = int((secs % HOUR) // MINUTE)
        sec = int(secs % MINUTE)
        return "{hour:02}:{min:02}:{sec:02}".format(hour=hour,min=min,sec=sec)

    if secs < 7 * DAY:
        days = int(secs // DAY)
        hour = int((secs % DAY) // HOUR)
        return "{days} days {hour:02} hours".format(days=days, hour=hour)

    weeks = int (secs // (7*DAY))
    remaining_days = int((secs % (7*DAY)) // DAY)
    return "{weeks} weeks {days} days".format(weeks=weeks, days=remaining_days)
