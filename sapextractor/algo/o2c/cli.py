from sapextractor.algo.o2c import o2c_1d_dataframe_extractor, o2c_1d_log_extractor
from sapextractor.algo.o2c import obj_centr_log


def cli(con):
    print("\n\nO2C extraction\n")
    print("available extraction types:")
    print("1) O2C classic log (XES log)")
    print("2) O2C classic log (dataframe)")
    print("3) O2C object-centric log")
    print()
    ext_type = input("insert your choice (default: 1): ")
    if not ext_type:
        ext_type = "1"
    if ext_type == "1":
        return o2c_1d_log_extractor.cli(con)
    elif ext_type == "2":
        return o2c_1d_dataframe_extractor.cli(con)
    elif ext_type == "3":
        return obj_centr_log.cli(con)
