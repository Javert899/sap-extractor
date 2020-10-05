from sapextractor.algo.ap_ar import ap_ar_common
from sapextractor.utils.graph_building import build_graph
import pandas as pd


def apply(con, ref_type="Goods receipt"):
    bkpf, doc_first_dates, doc_types = ap_ar_common.extract_bkpf(con)
    bseg = ap_ar_common.extract_bseg(con, doc_first_dates, doc_types)
    anc_succ = build_graph.get_ancestors_successors(bseg, "event_AUGBL", "event_BELNR", "event_AUGBL_TYPE", "event_BELNR_TYPE", ref_type)
    bkpf = bkpf.merge(anc_succ, left_on="event_BELNR", right_on="node", suffixes=('', '_r'), how="right")
    bkpf = bkpf.reset_index()
    bkpf["event_activity"] = bkpf["event_BLART"]
    ren_cols = {"event_activity": "concept:name", "event_timestamp": "time:timestamp", "event_USNAM": "org:resource"}
    bkpf = bkpf.rename(columns=ren_cols)
    ren_cols = {x: x.split("event_")[-1] for x in bkpf.columns}
    bkpf = bkpf.rename(columns=ren_cols)
    bkpf = bkpf.groupby(["case:concept:name", "BELNR"]).first().reset_index()
    bkpf = bkpf.sort_values("time:timestamp")
    return bkpf
