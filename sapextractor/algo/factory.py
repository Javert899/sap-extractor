from sapextractor.algo.ap_ar import factory as ap_ar_factory
from sapextractor.algo.o2c import factory as o2c_factory


def apply(con, process, ext_type, ext_arg):
    if process == "ap_ar":
        return ap_ar_factory.apply(con, ext_type, ext_arg)
    elif process == "o2c":
        return o2c_factory.apply(con, ext_type, ext_arg)
