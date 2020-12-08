from sapextractor.algo.o2c import o2c_common
from sapextractor.utils.graph_building import build_graph
from sapextractor.utils.filters import case_filter
from sapextractor.utils import constants
from sapextractor.utils.change_tables import extract_change
from sapextractor.utils.fields_corresp import extract_dd03t
import pandas as pd


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
            y["concept:name"] = "Change "+y["FNAME_CORR"]
            for cc in case_vbeln_dict[x]:
                z = y.copy()
                z["case:concept:name"] = cc
                ret.append(z)
    ret = pd.concat(ret)
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
    if keep_first:
        dataframe = dataframe.groupby("VBELN").first()
    dataframe = pd.concat([dataframe, changes])
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
