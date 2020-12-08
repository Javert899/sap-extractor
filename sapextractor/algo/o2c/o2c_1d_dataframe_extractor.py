from sapextractor.algo.o2c import o2c_common
from sapextractor.utils.graph_building import build_graph
from sapextractor.utils.filters import case_filter
from sapextractor.utils import constants
from sapextractor.utils.change_tables import extract_change
from sapextractor.utils.fields_corresp import extract_dd03t
from sapextractor.utils.blart import extract_blart
import pandas as pd
import numpy as np


def extract_changes_vbfa(con, dataframe):
    case_vbeln = dataframe[["case:concept:name", "VBELN"]].to_dict("r")
    case_vbeln_dict = {}
    for x in case_vbeln:
        caseid = x["case:concept:name"]
        vbeln = x["VBELN"]
        if vbeln not in case_vbeln_dict:
            case_vbeln_dict[vbeln] = set()
        case_vbeln_dict[vbeln].add(caseid)
    ret = []
    dict_corr = extract_dd03t.apply(con)
    for tup in [("VERKBELEG", "VBAK"), ("VERKBELEG", "VBAP"), ("VERKBELEG", "VBUK"), ("LIEFERUNG", "LIKP"),
                ("LIEFERUNG", "LIPS"), ("LIEFERUNG", "VBUK")]:
        changes = extract_change.apply(con, objectclas=tup[0], tabname=tup[1])
        changes = {x: y for x, y in changes.items() if x in case_vbeln_dict}
        for x, y in changes.items():
            y = y[[xx for xx in y.columns if xx.startswith("event_")]]
            cols = {x: x.split("event_")[-1] for x in y.columns}
            cols["event_timestamp"] = "time:timestamp"
            y = y.rename(columns=cols)
            fnames = set(y["FNAME"].unique())
            for fname in fnames:
                if fname not in dict_corr:
                    dict_corr[fname] = fname
            y["VBELN"] = y["AWKEY"]
            y["FNAME_CORR"] = y["FNAME"].map(dict_corr)
            y = y.dropna(subset=["FNAME_CORR"])
            if len(y) > 0:
                y["concept:name"] = "Change " + y["FNAME_CORR"]
                for cc in case_vbeln_dict[x]:
                    z = y.copy()
                    z["case:concept:name"] = cc
                    ret.append(z)

    if ret:
        ret = pd.concat(ret)
    else:
        ret = pd.DataFrame()

    return ret


def extract_bkpf_bsak(con, dataframe):

    case_vbeln = dataframe[["case:concept:name", "VBELN"]].to_dict("r")
    case_vbeln_dict = {}
    for x in case_vbeln:
        caseid = x["case:concept:name"]
        vbeln = x["VBELN"]
        if vbeln not in case_vbeln_dict:
            case_vbeln_dict[vbeln] = set()
        case_vbeln_dict[vbeln].add(caseid)

    bkpf = con.prepare_and_execute_query("BKPF", ["BELNR", "GJAHR", "AWKEY", "BLART"])
    blart_vals = set(bkpf["BLART"].unique())
    blart_vals = {x: x for x in blart_vals}
    blart_vals = extract_blart.apply_static(con, doc_types=blart_vals)
    bkpf = bkpf.to_dict("r")
    try:
        bseg = con.prepare_and_execute_query("BSAK", ["BELNR", "BUZEI", "AUGDT", "AUGBL"])
    except:
        bseg = con.prepare_and_execute_query("BSEG", ["BELNR", "BUZEI", "AUGDT", "AUGBL"])
    bseg = bseg.dropna(subset=["AUGBL"])
    try:
        bseg["AUGDT"] = pd.to_datetime(bseg["AUGDT"], format="%d.%m.%Y")
    except:
        bseg["AUGDT"] = pd.to_datetime(bseg["AUGDT"])
    bseg = bseg.to_dict("r")
    dict_awkey = {}
    clearance_docs_dates = {}
    blart_dict = {}
    for el in bkpf:
        awkey = el["AWKEY"]
        belnr = el["BELNR"]
        blart = el["BLART"]
        if awkey is not None:
            if len(awkey) == 18:
                awkey = awkey[4:-4]
                if awkey not in dict_awkey:
                    dict_awkey[awkey] = []
                dict_awkey[awkey].append(belnr)
        if not belnr in blart_dict:
            blart_dict[belnr] = blart
    for el in bseg:
        belnr = el["BELNR"]
        augbl = el["AUGBL"]
        augdt = el["AUGDT"]
        if augbl in blart_dict:
            blart = blart_dict[augbl]
            if not belnr in clearance_docs_dates:
                clearance_docs_dates[belnr] = set()
            clearance_docs_dates[belnr].add((augbl, augdt, blart))

    intersect = set(case_vbeln_dict.keys()).intersection(dict_awkey.keys())

    ret = []
    for k in intersect:
        for belnr in dict_awkey[k]:
            if belnr in clearance_docs_dates:
                for clearingdoc in clearance_docs_dates[belnr]:
                    for cas in case_vbeln_dict[k]:
                        ret.append({"case:concept:name": cas, "concept:name": "Clearance ("+blart_vals[clearingdoc[2]]+")", "AUGBL": clearingdoc[0], "time:timestamp": clearingdoc[1]})
    ret = pd.DataFrame(ret)

    if len(ret) > 0:
        if "time:timestamp" in ret.columns:
            ret["time:timestamp"] = ret["time:timestamp"] + pd.Timedelta(np.timedelta64(86399, 's'))

            ret = ret.groupby(["case:concept:name", "AUGBL"]).first().reset_index()

    return ret



def apply(con, ref_type="Order", keep_first=True):
    dataframe = o2c_common.apply(con, keep_first=keep_first)
    dataframe = dataframe[[x for x in dataframe.columns if x.startswith("event_")]]
    cols = {x: x.split("event_")[-1] for x in dataframe.columns}
    cols["event_activity"] = "concept:name"
    cols["event_timestamp"] = "time:timestamp"
    dataframe = dataframe.rename(columns=cols)
    ancest_succ = build_graph.get_ancestors_successors(dataframe, "VBELV", "VBELN", "VBTYP_V", "VBTYP_N", ref_type=ref_type)
    # ancest_succ = build_graph.get_conn_comp(dataframe, "VBELV", "VBELN", "VBTYP_V", "VBTYP_N", ref_type=ref_type)
    dataframe = dataframe.merge(ancest_succ, left_on="VBELN", right_on="node", suffixes=('', '_r'), how="right")
    dataframe = dataframe.reset_index()
    changes = extract_changes_vbfa(con, dataframe)
    payments = extract_bkpf_bsak(con, dataframe)
    if keep_first:
        dataframe = dataframe.groupby("VBELN").first()
    dataframe = pd.concat([dataframe, changes, payments])
    dataframe = dataframe.sort_values("time:timestamp")
    dataframe = case_filter.filter_on_case_size(dataframe, "case:concept:name", min_case_size=1, max_case_size=constants.MAX_CASE_SIZE)
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
    dataframe = apply(con, ref_type=ref_type, keep_first=keep_first)
    path = input("Insert the path where the dataframe should be saved (default: o2c.csv):")
    if not path:
        path = "o2c.csv"
    dataframe.to_csv(path, sep=",", quotechar="\"",  index=False)
