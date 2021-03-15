import pandas as pd
from pm4pymdl.objects.mdl.exporter import exporter as mdl_exporter
from pm4pymdl.objects.ocel.exporter import exporter as ocel_exporter

from sapextractor.algo.o2c import o2c_common
from sapextractor.utils import constants
from sapextractor.utils.change_tables import extract_change


def get_changes(con, dataframe, mandt="800"):
    vbeln_values = dataframe[["event_VBELN", "event_VBTYP_N"]].to_dict("r")
    vbeln_values = {x["event_VBELN"]: x["event_VBTYP_N"] for x in vbeln_values}
    ret = []
    for tup in [("VERKBELEG", "VBAK"), ("VERKBELEG", "VBAP"), ("VERKBELEG", "VBUK"), ("LIEFERUNG", "LIKP"),
                ("LIEFERUNG", "LIPS"), ("LIEFERUNG", "VBUK")]:
        changes = extract_change.apply(con, objectclas=tup[0], tabname=tup[1], mandt=mandt)
        changes = {x: y for x, y in changes.items() if x in vbeln_values}
        for x, y in changes.items():
            awkey = list(y["event_AWKEY"])[0]
            if awkey in vbeln_values:
                y["event_VBELN"] = y["event_AWKEY"]
                y["event_activity"] = y["event_CHANGEDESC"]
                k = "DOCTYPE_" + vbeln_values[awkey]
                y[k] = y["event_AWKEY"]
                y[k] = y[k].apply(constants.set_documents)

                #y["INVOLVED_DOCUMENTS"] = y["event_AWKEY"]
                #y["INVOLVED_DOCUMENTS"] = y["INVOLVED_DOCUMENTS"].apply(constants.set_documents)
                ret.append(y)

    if ret:
        ret = pd.concat(ret)
    else:
        ret = pd.DataFrame()

    return ret


def apply(con, keep_first=True, min_extr_date="2020-01-01 00:00:00", gjahr="2020", enable_changes=True,
          enable_payments=True, allowed_act_doc_types=None, allowed_act_changes=None, mandt="800"):
    dataframe = o2c_common.apply(con, keep_first=keep_first, min_extr_date=min_extr_date, mandt=mandt)
    if keep_first:
        dataframe = dataframe.groupby("event_VBELN").first().reset_index()
    if allowed_act_doc_types is not None:
        allowed_act_doc_types = set(allowed_act_doc_types)
        dataframe = dataframe[dataframe["event_activity"].isin(allowed_act_doc_types)]
    dataframe = dataframe.sort_values("event_timestamp")
    if enable_changes:
        changes = get_changes(con, dataframe, mandt=mandt)
    else:
        changes = pd.DataFrame()
    if len(changes) > 0:
        changes = changes.sort_values("event_timestamp")
    if allowed_act_changes is not None:
        allowed_act_changes = set(allowed_act_changes)
        changes = changes[changes["event_activity"].isin(allowed_act_changes)]
    dataframe = pd.concat([dataframe, changes])
    dataframe["event_id"] = dataframe.index.astype(str)
    dataframe = dataframe.sort_values("event_timestamp")
    dataframe.type = "succint"
    return dataframe


def cli(con):
    print("\n\nO2C Object-Centric Log Extractor\n\n")
    min_extr_date = input("Insert the minimum extraction date (default: 2020-01-01 00:00:00): ")
    if not min_extr_date:
        min_extr_date = "2020-01-01 00:00:00"
    gjahr = input("Insert the fiscal year (default: 2020):")
    if not gjahr:
        gjahr = "2020"
    dataframe = apply(con, min_extr_date=min_extr_date, gjahr=gjahr)
    path = input("Insert the path where the log should be saved (default: o2c.xmlocel): ")
    if not path:
        path = "o2c.xmlocel"
    if path.endswith("mdl"):
        mdl_exporter.apply(dataframe, path)
    elif path.endswith("jsonocel") or path.endswith("xmlocel"):
        ocel_exporter.apply(dataframe, path)
