import numpy as np
import pandas as pd

from sapextractor.algo.o2c import o2c_common
from sapextractor.utils import constants
from sapextractor.utils.change_tables import extract_change
from sapextractor.utils.filters import case_filter
from sapextractor.utils.graph_building import build_graph
from sapextractor.algo.o2c import payment_part


def extract_changes_vbfa(con, dataframe):
    case_vbeln = dataframe[["case:concept:name", "VBELN"]].to_dict("records")
    case_vbeln_dict = {}
    for x in case_vbeln:
        caseid = x["case:concept:name"]
        vbeln = x["VBELN"]
        if vbeln not in case_vbeln_dict:
            case_vbeln_dict[vbeln] = set()
        case_vbeln_dict[vbeln].add(caseid)
    ret = []
    for tup in [("VERKBELEG", "VBAK"), ("VERKBELEG", "VBAP"), ("VERKBELEG", "VBUK"), ("LIEFERUNG", "LIKP"),
                ("LIEFERUNG", "LIPS"), ("LIEFERUNG", "VBUK")]:
        changes = extract_change.apply(con, objectclas=tup[0], tabname=tup[1])
        changes = {x: y for x, y in changes.items() if x in case_vbeln_dict}
        for x, y in changes.items():
            y = y[[xx for xx in y.columns if xx.startswith("event_")]]
            cols = {x: x.split("event_")[-1] for x in y.columns}
            cols["event_timestamp"] = "time:timestamp"
            y = y.rename(columns=cols)
            y["VBELN"] = y["AWKEY"]
            y["concept:name"] = y["CHANGEDESC"]
            for cc in case_vbeln_dict[x]:
                z = y.copy()
                z["case:concept:name"] = cc
                ret.append(z)

    if ret:
        ret = pd.concat(ret)
    else:
        ret = pd.DataFrame()

    return ret


def extract_bkpf_bsak(con, dataframe, gjahr="2020"):
    case_vbeln = dataframe[["case:concept:name", "VBELN"]].to_dict("records")
    case_vbeln_dict = {}
    for x in case_vbeln:
        caseid = x["case:concept:name"]
        vbeln = x["VBELN"]
        if vbeln not in case_vbeln_dict:
            case_vbeln_dict[vbeln] = set()
        case_vbeln_dict[vbeln].add(caseid)

    dict_awkey, clearance_docs_dates, blart_vals = payment_part.apply(con, gjahr=gjahr)

    intersect = set(case_vbeln_dict.keys()).intersection(dict_awkey.keys())

    ret = []
    for k in intersect:
        for belnr in dict_awkey[k]:
            if belnr in clearance_docs_dates:
                for clearingdoc in clearance_docs_dates[belnr]:
                    for cas in case_vbeln_dict[k]:
                        ret.append(
                            {"case:concept:name": cas, "concept:name": "Clearance (" + blart_vals[clearingdoc[2]] + ")",
                             "AUGBL": clearingdoc[0], "time:timestamp": clearingdoc[1]})
    ret = pd.DataFrame(ret)

    if len(ret) > 0:
        if "time:timestamp" in ret.columns:
            ret["time:timestamp"] = ret["time:timestamp"] + pd.Timedelta(np.timedelta64(86399, 's'))

            ret = ret.groupby(["case:concept:name", "AUGBL"]).first().reset_index()

    return ret


def apply(con, ref_type="Order", keep_first=True, min_extr_date="2020-01-01 00:00:00", gjahr="2020", enable_changes=True, enable_payments=True, allowed_activities=None):
    dataframe = o2c_common.apply(con, keep_first=keep_first, min_extr_date=min_extr_date)
    dataframe = dataframe[[x for x in dataframe.columns if x.startswith("event_")]]
    cols = {x: x.split("event_")[-1] for x in dataframe.columns}
    cols["event_activity"] = "concept:name"
    cols["event_timestamp"] = "time:timestamp"
    dataframe = dataframe.rename(columns=cols)
    ancest_succ = build_graph.get_ancestors_successors(dataframe, "VBELV", "VBELN", "VBTYP_V", "VBTYP_N",
                                                       ref_type=ref_type)
    # ancest_succ = build_graph.get_conn_comp(dataframe, "VBELV", "VBELN", "VBTYP_V", "VBTYP_N", ref_type=ref_type)
    dataframe = dataframe.merge(ancest_succ, left_on="VBELN", right_on="node", suffixes=('', '_r'), how="right")
    dataframe = dataframe.reset_index()
    if enable_changes:
        changes = extract_changes_vbfa(con, dataframe)
    else:
        changes = pd.DataFrame()
    if enable_payments:
        payments = extract_bkpf_bsak(con, dataframe, gjahr=gjahr)
    else:
        payments = pd.DataFrame()
    if keep_first:
        dataframe = dataframe.groupby("VBELN").first()
    dataframe = pd.concat([dataframe, changes, payments])
    if allowed_activities is not None:
        allowed_activities = set(allowed_activities)
        dataframe["concept:name"] = dataframe["concept:name"].isin(allowed_activities)
    dataframe = dataframe.sort_values("time:timestamp")
    dataframe = case_filter.filter_on_case_size(dataframe, "case:concept:name", min_case_size=1,
                                                max_case_size=constants.MAX_CASE_SIZE)
    return dataframe


def cli(con):
    print("\n\nO2C dataframe extractor\n")
    ref_type = input("Insert the central document type of the extraction (default: Invoice): ")
    if not ref_type:
        ref_type = "Invoice"
    ext_type = input("Do you want to extract the document log, or the items log (default: document):")
    if not ext_type:
        ext_type = "document"
    keep_first = True
    if ext_type == "document":
        keep_first = True
    elif ext_type == "items":
        keep_first = False
    min_extr_date = input("Insert the minimum extraction date (default: 2020-01-01 00:00:00): ")
    if not min_extr_date:
        min_extr_date = "2020-01-01 00:00:00"
    gjahr = input("Insert the fiscal year (default: 2020):")
    if not gjahr:
        gjahr = "2020"
    dataframe = apply(con, ref_type=ref_type, keep_first=keep_first, min_extr_date=min_extr_date, gjahr=gjahr)
    path = input("Insert the path where the dataframe should be saved (default: o2c.csv):")
    if not path:
        path = "o2c.csv"
    dataframe.to_csv(path, sep=",", quotechar="\"", index=False)
