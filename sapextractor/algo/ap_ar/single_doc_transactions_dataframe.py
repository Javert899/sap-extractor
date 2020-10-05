from sapextractor.algo.ap_ar import ap_ar_common
from sapextractor.utils.filters import case_filter
from sapextractor.utils import constants


def apply(con, **ext_arg):
    bkpf = ap_ar_common.get_full_dataframe(con, filter_columns=True)
    bkpf["event_activity"] = bkpf["event_ONLYACT"]
    ren_cols = {"event_activity": "concept:name", "event_timestamp": "time:timestamp", "event_USNAM": "org:resource"}
    bkpf = bkpf.rename(columns=ren_cols)
    ren_cols = {x: x.split("event_")[-1] for x in bkpf.columns}
    bkpf = bkpf.rename(columns=ren_cols)
    bkpf["case:concept:name"] = bkpf["BELNR"]
    bkpf = bkpf.dropna(subset=["concept:name", "time:timestamp"], how="any")
    bkpf = bkpf.sort_values("time:timestamp")
    bkpf = case_filter.filter_on_case_size(bkpf, "case:concept:name", min_case_size=1, max_case_size=constants.MAX_CASE_SIZE)
    return bkpf


def cli(con):
    print("\n\nAccounting - Transactions for the single document (dataframe)\n")
    dataframe = apply(con)
    path = input("Insert the path where the dataframe should be saved (default: bkpf.csv):")
    if not path:
        path = "bkpf.csv"
    dataframe.to_csv(path, sep=",", quotechar="\"", index=False)
