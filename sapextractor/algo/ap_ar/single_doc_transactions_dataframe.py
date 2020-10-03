from sapextractor.algo.ap_ar import ap_ar_common
import pandas as pd


def apply(con, **ext_arg):
    bkpf, doc_first_dates, doc_types = ap_ar_common.extract_bkpf(con)
    bseg = ap_ar_common.extract_bseg(con, doc_first_dates, doc_types)
    bkpf = pd.concat([bkpf, bseg])
    bkpf["event_activity"] = bkpf["event_ONLYACT"]
    ren_cols = {"event_activity": "concept:name", "event_timestamp": "time:timestamp"}
    bkpf = bkpf.rename(columns=ren_cols)
    ren_cols = {x: x.split("event_")[-1] for x in bkpf.columns}
    bkpf = bkpf.rename(columns=ren_cols)
    bkpf["case:concept:name"] = bkpf["BELNR"]
    bkpf = bkpf.sort_values("time:timestamp")
    return bkpf


def cli(con):
    print("\n\nAccounting - Transactions for the single document (dataframe)\n")
    dataframe = apply(con)
    path = input("Insert the path where the dataframe should be saved (default: bkpf.csv):")
    if not path:
        path = "bkpf.csv"
    dataframe.to_csv(path, index=False)
