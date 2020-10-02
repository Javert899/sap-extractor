import pandas as pd
from datetime import datetime
from sapextractor.utils.vbtyp import extract_vbtyp


ORDER_DOC_SEP = "@#@#@#"


def vbfa_closure(vbfa):
    vbeln_unique = set(vbfa["VBELN"].unique())
    vbfa_vbelv_types = list((x["VBELV"], x["VBTYP_V"]) for x in vbfa[["VBELV", "VBTYP_V"]].to_dict("r") if
                            x["VBELV"] not in vbeln_unique)
    # default the timestamp
    closure_events = {"VBELN": [x[0] for x in vbfa_vbelv_types], "VBTYP_N": [x[1] for x in vbfa_vbelv_types],
                      "event_timestamp": datetime.fromtimestamp(10000000)}
    closure_df = pd.DataFrame(closure_events)
    return pd.concat([vbfa, closure_df]).sort_values("event_timestamp")


def set_order_documents(content):
    ret = []
    content = content.split(ORDER_DOC_SEP)
    for x in content:
        if str(x).lower() != "nan":
            ret.append(x)
    return ret


def apply(con):
    vbfa = con.execute_read_sql("SELECT ERDAT, ERZET, VBELN, VBELV, VBTYP_N, VBTYP_V FROM VBFA")
    vbfa["ERDAT"] = pd.to_datetime(vbfa["ERDAT"]).apply(lambda x: x.timestamp())
    vbfa["ERZET"] = pd.to_datetime(vbfa["ERZET"]).apply(lambda x: x.timestamp())
    vbfa["event_timestamp"] = vbfa["ERDAT"] + vbfa["ERZET"]
    vbfa["event_timestamp"] = vbfa["event_timestamp"].apply(lambda x: datetime.fromtimestamp(x))
    doc_types = set(vbfa["VBTYP_N"].unique()).union(set(vbfa["VBTYP_V"].unique()))
    vbtyp = extract_vbtyp.apply_static(con, doc_types=doc_types)
    vbfa["VBTYP_N"] = vbfa["VBTYP_N"].map(vbtyp)
    vbfa["VBTYP_V"] = vbfa["VBTYP_V"].map(vbtyp)
    vbfa = vbfa_closure(vbfa)
    vbfa["event_id"] = vbfa.index.astype(str)
    cols = {}
    for x in vbfa.columns:
        if x.startswith("event_"):
            cols[x] = x
        else:
            cols[x] = "event_" + x
    vbfa = vbfa.rename(columns=cols)
    vbfa["ORDER_DOCUMENTS"] = vbfa["event_VBELV"].astype(str) + ORDER_DOC_SEP + vbfa["event_VBELN"].astype(str)
    vbfa["ORDER_DOCUMENTS"] = vbfa["ORDER_DOCUMENTS"].apply(set_order_documents)
    vbfa = vbfa.reset_index()
    return vbfa

