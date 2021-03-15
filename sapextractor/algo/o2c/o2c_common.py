from datetime import datetime

import pandas as pd
from dateutil import parser

from sapextractor.utils import constants
from sapextractor.utils.dates import timestamp_column_from_dt_tm
from sapextractor.utils.vbtyp import extract_vbtyp
from pm4pymdl.algo.mvp.utils import exploded_mdl_to_succint_mdl

def extract_vbak(con, min_extr_date="2020-01-01 00:00:00", mandt="800"):
    vbak = {}
    try:
        vbak = con.prepare_and_execute_query("VBAK", ["VBELN", "ERDAT", "ERZET"], additional_query_part=" WHERE MANDT = '"+mandt+"'")
        timestamp_column_from_dt_tm.apply(vbak, "ERDAT", "ERZET", "event_timestamp")
        vbak = vbak[vbak["event_timestamp"] > min_extr_date]
        vbak = vbak[["VBELN", "event_timestamp"]].to_dict("r")
        vbak = {x["VBELN"]: x["event_timestamp"] for x in vbak}
    except:
        pass
    return vbak


def vbfa_closure(con, vbfa, min_extr_date, mandt="800"):
    vbak = extract_vbak(con, min_extr_date=min_extr_date, mandt=mandt)
    vbeln_unique = set(vbfa["VBELN"].unique())
    vbfa_vbelv_types = list((x["VBELV"], x["VBTYP_V"]) for x in vbfa[["VBELV", "VBTYP_V"]].to_dict("records") if
                            x["VBELV"] not in vbeln_unique)
    # default the timestamp
    closure_events = {"VBELN": [x[0] for x in vbfa_vbelv_types], "VBTYP_N": [x[1] for x in vbfa_vbelv_types],
                      "event_timestamp": [vbak[x[0]] if x[0] in vbak else datetime.fromtimestamp(10000000) for x in vbfa_vbelv_types]}
    closure_df = pd.DataFrame(closure_events)
    return pd.concat([vbfa, closure_df]).sort_values("event_timestamp")


def apply(con, keep_first=True, min_extr_date="2020-01-01 00:00:00", mandt="800"):
    # RFMNG, MEINS, RFWRT, WAERS, MATNR, BWART
    try:
        vbfa = con.prepare_and_execute_query("VBFA", ["ERDAT", "ERZET", "VBELN", "VBELV", "VBTYP_N", "VBTYP_V", "RFMNG",
                                                      "MEINS", "RFWRT", "WAERS", "MATNR", "BWART", "VRKME", "FKTYP"], additional_query_part=" WHERE MANDT = '"+mandt+"'")
    except:
        vbfa = con.prepare_and_execute_query("VBFA", ["ERDAT", "ERZET", "VBELN", "VBELV", "VBTYP_N", "VBTYP_V"], additional_query_part=" WHERE MANDT = '"+mandt+"'")
    timestamp_column_from_dt_tm.apply(vbfa, "ERDAT", "ERZET", "event_timestamp")
    min_extr_date = parser.parse(min_extr_date)
    vbfa = vbfa[vbfa["event_timestamp"] > min_extr_date]
    doc_types = set(vbfa["VBTYP_N"].unique()).union(set(vbfa["VBTYP_V"].unique()))
    vbtyp = extract_vbtyp.apply_static(con, doc_types=doc_types)
    vbfa["VBTYP_N"] = vbfa["VBTYP_N"].map(vbtyp)
    vbfa["VBTYP_V"] = vbfa["VBTYP_V"].map(vbtyp)
    vbfa = vbfa_closure(con, vbfa, min_extr_date, mandt=mandt)
    vbfa = vbfa[vbfa["event_timestamp"] >= min_extr_date]
    vbfa = vbfa.reset_index()
    vbfa["event_id"] = vbfa.index.astype(str)
    cols = {}
    for x in vbfa.columns:
        if x.startswith("event_"):
            cols[x] = x
        else:
            cols[x] = "event_" + x
    vbfa = vbfa.rename(columns=cols)
    vbfa["event_activity"] = "Create " + vbfa["event_VBTYP_N"]
    if not keep_first:
        vbfa["event_activity"] = vbfa["event_activity"] + " Item"

    vbfa["event_id"] = vbfa["event_id"].astype(int)
    vbfa = vbfa.sort_values("event_id")

    #vbfa["INVOLVED_DOCUMENTS"] = vbfa["event_VBELV"].astype(str) + constants.DOC_SEP + vbfa["event_VBELN"].astype(str)
    #vbfa["INVOLVED_DOCUMENTS"] = vbfa["INVOLVED_DOCUMENTS"].apply(constants.set_documents)
    #vbfa.to_csv("prova1.csv", index=False)
    #del vbfa["INVOLVED_DOCUMENTS"]

    doctypes_n = vbfa["event_VBTYP_N"].unique()
    doctypes_v = vbfa["event_VBTYP_V"].unique()
    list_dfs = []
    for value in doctypes_n:
        df = vbfa[vbfa["event_VBTYP_N"] == value]
        df["DOCTYPE_"+str(value)] = df["event_VBELN"]
        list_dfs.append(df)
    for value in doctypes_v:
        df = vbfa[vbfa["event_VBTYP_V"] == value]
        df["DOCTYPE_"+str(value)] = df["event_VBELV"]
        list_dfs.append(df)
    if list_dfs:
        vbfa = pd.concat(list_dfs)
        vbfa = exploded_mdl_to_succint_mdl.apply(vbfa)
        vbfa = vbfa.reset_index()
        vbfa = vbfa.sort_values("event_id")
        vbfa["event_id"] = vbfa["event_id"].astype(str)
    else:
        vbfa = pd.DataFrame({"case:concept:name": [], "VBELN": []})

    #vbfa.to_csv("prova2.csv", index=False)
    return vbfa
