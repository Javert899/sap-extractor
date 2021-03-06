from sapextractor.algo.ap_ar import ap_ar_common
from sapextractor.utils.graph_building import build_graph
from sapextractor.utils.filters import case_filter
from sapextractor.utils import constants


def apply(con, gjahr="1997", mandt="800", bukrs="1000", ref_type="Goods receipt"):
    bkpf, bseg = ap_ar_common.get_single_dataframes(con, gjahr=gjahr, bukrs=bukrs, mandt=mandt, filter_columns=True)
    all_docs = set(bkpf[bkpf["event_BLART"] == ref_type]["event_BELNR"].unique())
    anc_succ = build_graph.get_ancestors_successors(bseg, "event_BELNR", "event_AUGBL", "event_BELNR_TYPE", "event_AUGBL_TYPE", ref_type, all_docs=all_docs)
    bkpf = bkpf.merge(anc_succ, left_on="event_BELNR", right_on="node", suffixes=('', '_r'), how="right")
    bkpf = bkpf.reset_index()
    bkpf["event_activity"] = bkpf["event_BLART"]
    ren_cols = {"event_activity": "concept:name", "event_timestamp": "time:timestamp", "event_USNAM": "org:resource"}
    bkpf = bkpf.rename(columns=ren_cols)
    ren_cols = {x: x.split("event_")[-1] for x in bkpf.columns}
    bkpf = bkpf.rename(columns=ren_cols)
    bkpf = bkpf.groupby(["case:concept:name", "BELNR"]).first().reset_index()
    bkpf = bkpf.dropna(subset=["concept:name", "time:timestamp"], how="any")
    bkpf = bkpf.sort_values("time:timestamp")
    bkpf = case_filter.filter_on_case_size(bkpf, "case:concept:name", min_case_size=1, max_case_size=constants.MAX_CASE_SIZE)
    return bkpf


def cli(con):
    print("\n\nAccounting Doc Flow dataframe extractor\n\n")
    ref_type = input("Insert the central document type of the extraction (default: Goods receipt): ")
    if not ref_type:
        ref_type = "Goods receipt"
    dataframe = apply(con, ref_type=ref_type)
    path = input("Insert the path where the dataframe should be saved (default: doc_flow.csv): ")
    if not path:
        path = "doc_flow.csv"
    dataframe.to_csv(path, sep=",", quotechar="\"", index=False)
