from sapextractor.algo.p2p import p2p_common
from pm4pymdl.objects.ocel.exporter import exporter as jmd_exporter
from pm4pymdl.objects.mdl.exporter import exporter as mdl_exporter
from sapextractor.utils.change_tables import extract_change
import pandas as pd
from sapextractor.utils import constants


def get_changes(con, dataframe, mandt="800", bukrs="1000"):
    object_values = dict()
    if len(dataframe) > 0:
        object_values0 = dataframe.dropna(subset=["event_OBJECTID"])[["node", "event_OBJECTID"]].to_dict("records")
        object_values = {x["event_OBJECTID"]: x["node"] for x in object_values0}
    ret = []
    for tup in [("EINKBELEG", None), ("INCOMINGINVOICE", None)]:
        changes = extract_change.apply(con, objectclas=tup[0], tabname=tup[1], mandt=mandt)
        changes = {x: y for x, y in changes.items() if x in object_values}
        for x, y in changes.items():
            try:
                change_df = y.copy()
                change_df["event_activity"] = y["event_CHANGEDESC"]
                change_df["INVOLVED_DOCUMENTS"] = object_values[x]
                change_df["INVOLVED_DOCUMENTS"] = change_df["INVOLVED_DOCUMENTS"].apply(constants.set_documents)
                ret.append(change_df)
            except:
                pass

    if ret:
        ret = pd.concat(ret)
    else:
        ret = pd.DataFrame()

    return ret


def apply(con, gjahr="2014", min_extr_date="2014-01-01 00:00:00", mandt="800", bukrs="1000"):
    dataframe, G, nodes_types = p2p_common.extract_tables_and_graph(con, gjahr=gjahr, min_extr_date=min_extr_date, mandt=mandt, bukrs=bukrs)
    if len(dataframe) > 0:
        changes = get_changes(con, dataframe, mandt=mandt, bukrs=bukrs)
        dataframe = pd.concat([dataframe, changes])
        dataframe["event_id"] = dataframe.index.astype(str)
        dataframe = dataframe.sort_values("event_timestamp")
    dataframe.type = "succint"
    return dataframe


def cli(con):
    print("\n\nP2P - Object-Centric Log\n")
    dataframe = apply(con)
    path = input("Insert the path where the log should be saved (default: p2p.mdl): ")
    if not path:
        path = "p2p.xmlocel"
    if path.endswith("mdl"):
        mdl_exporter.apply(dataframe, path)
    elif path.endswith("jsonocel") or path.endswith("xmlocel"):
        jmd_exporter.apply(dataframe, path)
