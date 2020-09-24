import pandas as pd
from datetime import datetime
from sapextractor.utils.vbtyp import extract_vbtyp


def vbfa_closure(vbfa):
    vbeln_unique = set(vbfa["VBELN"].unique())
    vbfa_vbelv_types = list((x["VBELV"], x["VBTYP_V"]) for x in vbfa[["VBELV", "VBTYP_V"]].to_dict("r") if
                            x["VBELV"] not in vbeln_unique)
    # default the timestamp
    closure_events = {"VBELN": [x[0] for x in vbfa_vbelv_types], "VBTYP_N": [x[1] for x in vbfa_vbelv_types],
                      "time:timestamp": datetime.fromtimestamp(10000000)}
    closure_df = pd.DataFrame(closure_events)
    return pd.concat([vbfa, closure_df]).sort_values("time:timestamp")


def apply(con):
    vbfa = con.execute_read_sql("SELECT ERDAT, ERZET, VBELN, VBELV, VBTYP_N, VBTYP_V FROM VBFA")
    vbfa["ERDAT"] = pd.to_datetime(vbfa["ERDAT"]).apply(lambda x: x.timestamp())
    vbfa["ERZET"] = pd.to_datetime(vbfa["ERZET"]).apply(lambda x: x.timestamp())
    vbfa["time:timestamp"] = vbfa["ERDAT"] + vbfa["ERZET"]
    vbfa["time:timestamp"] = vbfa["time:timestamp"].apply(lambda x: datetime.fromtimestamp(x))
    doc_types = set(vbfa["VBTYP_N"].unique()).union(set(vbfa["VBTYP_V"].unique()))
    vbtyp = extract_vbtyp.apply_static(con, doc_types=doc_types)
    vbfa["VBTYP_N"] = vbfa["VBTYP_N"].map(vbtyp)
    vbfa["VBTYP_V"] = vbfa["VBTYP_V"].map(vbtyp)
    vbfa = vbfa_closure(vbfa)
    print(vbfa)
