from sapextractor.algo.p2p import p2p_1d_dataframe, p2p_1d_log
from sapextractor.algo.p2p import obj_centr_log


def cli(con):
    print("\n\nP2P extraction\n")
    print("available extraction types:")
    print("1) P2P classic log (XES log)")
    print("2) P2P classic log (dataframe)")
    print("3) P2P object-centric log")
    print()
    ext_type = input("insert your choice (default: 1): ")
    if not ext_type:
        ext_type = "1"
    if ext_type == "1":
        return p2p_1d_log.cli(con)
    elif ext_type == "2":
        return p2p_1d_dataframe.cli(con)
    elif ext_type == "3":
        return obj_centr_log.cli(con)

