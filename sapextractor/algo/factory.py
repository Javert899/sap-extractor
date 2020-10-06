from sapextractor.algo.ap_ar import factory as ap_ar_factory
from sapextractor.algo.o2c import factory as o2c_factory
from sapextractor.algo.p2p import factory as p2p_factory


def apply(con, process, ext_type, ext_arg):
    if process == "ap_ar":
        return ap_ar_factory.apply(con, ext_type, ext_arg)
    elif process == "o2c":
        return o2c_factory.apply(con, ext_type, ext_arg)
    elif process == "p2p":
        return p2p_factory.apply(con, ext_type, ext_arg)
