from sapextractor.algo.ap_ar import ap_ar_common
import pandas as pd


def apply(con):
    bkpf, doc_first_dates, doc_types = ap_ar_common.extract_bkpf(con)
    bseg = ap_ar_common.extract_bseg(con, doc_first_dates, doc_types)
    bkpf = pd.concat([bkpf, bseg])
    ren_cols = {"event_activity": "concept:name", "event_timestamp": "time:timestamp"}
    bkpf = bkpf.rename(columns=ren_cols)
    ren_cols = {x: x.split("event_")[-1] for x in bkpf.columns}
    bkpf = bkpf.rename(columns=ren_cols)
    bkpf["case:concept:name"] = bkpf["BELNR"]
    bkpf = bkpf.sort_values("time:timestamp")
    return bkpf
