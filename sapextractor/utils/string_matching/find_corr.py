from sapextractor.utils.string_matching import distances


def find_least_dist(col, col_set):
    col_set_keys = list(col_set.keys())
    ret = col_set[col_set_keys[0]]
    curr_dist = distances.apply(col, ret)
    i = 1
    while i < len(col_set):
        new_dist = distances.apply(col, col_set[col_set_keys[i]])
        if new_dist < curr_dist:
            ret = col_set[col_set_keys[i]]
            curr_dist = new_dist
        i = i + 1
    return ret


def apply(wanted_columns, existing_columns):
    wanted_columns = {x: x.lower() for x in wanted_columns}
    existing_columns = {x.lower(): x for x in existing_columns}

    returned_list = []

    for c1 in wanted_columns:
        returned_list.append(find_least_dist(wanted_columns[c1], existing_columns))

    return returned_list
