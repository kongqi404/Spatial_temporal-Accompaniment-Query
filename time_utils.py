def expand_time_range(time_range, delta_milli):
    return time_range[0] - delta_milli, time_range[1] + delta_milli


def is_intersects(range1, range2):
    return not (range1[0].after(range2[1]) or range1[1].before(range2[0]))


def time_refer_point(range1, range2):
    if is_intersects(range1, range2):
        if range1[0].before(range2[0]):
            return range2[0]
        else:
            return range1[0]
    else:
        return None
