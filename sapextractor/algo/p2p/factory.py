from sapextractor.algo.p2p import p2p_1d_dataframe, p2p_1d_log


def apply(con, ext_type, ext_arg):
    if ext_type == "p2p_1d_dataframe":
        return p2p_1d_dataframe.apply(con, **ext_arg)
    elif ext_type == "p2p_1d_log":
        return p2p_1d_log.apply(con, **ext_arg)
