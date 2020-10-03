from sapextractor.algo.o2c import o2c_1d_log_extractor, o2c_1d_dataframe_extractor


def apply(con, ext_type, ext_arg):
    if ext_type == "o2c_1d_log_extractor":
        return o2c_1d_log_extractor.apply(con, **ext_arg)
    elif ext_type == "o2c_1d_dataframe_extractor":
        return o2c_1d_dataframe_extractor.apply(con, **ext_arg)
