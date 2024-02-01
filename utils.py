
def get_index(x, y, zoom):
    size = 2 ** zoom
    return y * size + x


MINUTE = 60
HOUR = 60 * MINUTE
DAY = HOUR * 24


def humanized_time(secs):
    if secs < (5 * MINUTE):
        return f"{secs:.2f} sec."
    if secs <  HOUR:
        min = int(secs // MINUTE)
        sec = int(secs % MINUTE)
        return f"00:{min:0d2}:{sec:02d}"
    if secs < DAY:
        hour = int(secs // HOUR)
        min = int((secs % HOUR) // MINUTE)
        sec = int(secs % MINUTE)
        return f"{hour:02d}:{min:02d}:{sec:02d}"

    if secs < 7 * DAY:
        days = int(secs // DAY)
        hour = int((secs % DAY) // HOUR)
        return f"{days} days {hour:02d} hours"

    weeks = int (secs // (7*DAY))
    days = int((secs % DAY) // HOUR)
    return f"{weeks} weeks {days:02d} days"
