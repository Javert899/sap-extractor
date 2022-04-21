from sapextractor.algo.p2p import p2p_common
from sapextractor.utils.graph_building import build_graph
from sapextractor.utils.filters import case_filter
from sapextractor.utils import constants
import pandas as pd
from sapextractor.utils.change_tables import extract_change
from sapextractor.utils.usr02 import extract_usr02


def extract_changes_p2p(con, dataframe, ekko_query, rbkp_query, mandt="800", bukrs="1000"):
    if len(dataframe) > 0:
        case_vbeln = dataframe.dropna(subset=["OBJECTID"])[["case:concept:name", "OBJECTID"]].to_dict("records")
    else:
        case_vbeln = []
    case_vbeln_dict = {}
    for x in case_vbeln:
        caseid = x["case:concept:name"]
        objectid = x["OBJECTID"]
        if objectid not in case_vbeln_dict:
            case_vbeln_dict[objectid] = set()
        case_vbeln_dict[objectid].add(caseid)
    ret = []
    for tup in [("EINKBELEG", None, "SELECT EBELN FROM "+ekko_query.split("FROM ")[1]), ("INCOMINGINVOICE", None, "SELECT CONCAT(BELNR, GJAHR) AS BELNRGJAHR FROM "+rbkp_query.split("FROM ")[1])]:
        changes = extract_change.apply(con, objectclas=tup[0], tabname=tup[1], mandt=mandt, additional_query_part=tup[2])
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

    print("obtained dataframe")

    return ret


def apply(con, ref_type="EKKO", gjahr="2014", min_extr_date="2014-01-01 00:00:00", mandt="800", bukrs="1000", extract_changes=True, extra_els_query=None, enable_po_time_from_changes=True, include_resource=True, enable_grouping=True):
    dataframe, G, nodes_types, ekko_query, rbkp_query = p2p_common.extract_tables_and_graph(con, gjahr=gjahr, min_extr_date=min_extr_date, mandt=mandt, bukrs=bukrs, return_ekko_query=True, extra_els_query=extra_els_query)
    print(ekko_query)
    print(rbkp_query)
    if len(dataframe) > 0:
        dataframe = dataframe[[x for x in dataframe.columns if x.startswith("event_")]]
        anc_succ = build_graph.get_ancestors_successors_from_graph(G, nodes_types, ref_type=ref_type)
        dataframe = dataframe.merge(anc_succ, left_on="event_node", right_on="node", suffixes=('', '_r'), how="right")
        dataframe = dataframe.dropna(subset=["event_activity", "event_timestamp"])
        cols = {x: x.split("event_")[-1] for x in dataframe.columns}
        cols["event_activity"] = "concept:name"
        cols["event_timestamp"] = "time:timestamp"
        cols["event_USERNAME"] = "org:resource"
        dataframe = dataframe.rename(columns=cols)
        if extract_changes:
            changes = extract_changes_p2p(con, dataframe, ekko_query, rbkp_query, mandt, bukrs)
            dataframe = dataframe.loc[:,~dataframe.columns.duplicated()]
            dataframe = dataframe.reset_index(drop=True)
            changes = changes.loc[:,~changes.columns.duplicated()]
            changes = changes.reset_index(drop=True)
            changes = changes.sort_values(["case:concept:name", "time:timestamp", "concept:name"])
            if enable_po_time_from_changes:
                dictio_dates = changes.groupby("case:concept:name").first().reset_index()[["case:concept:name", "time:timestamp"]].to_dict("r")
                dictio_dates = {x["case:concept:name"]: x["time:timestamp"] for x in dictio_dates}
                dataframe_cpo = dataframe[dataframe["concept:name"] == "Create Purchase Order"]
                dataframe_notcpo = dataframe[dataframe["concept:name"] != "Create Purchase Order"]
                dataframe_cpo1 = dataframe_cpo[dataframe_cpo["case:concept:name"].isin(dictio_dates)]
                dataframe_cpo2 = dataframe_cpo[~dataframe_cpo["case:concept:name"].isin(dictio_dates)]
                dataframe_cpo1["time:timestamp"] = dataframe_cpo1["case:concept:name"].map(dictio_dates)
                dataframe = pd.concat([dataframe_cpo1, dataframe_cpo2, dataframe_notcpo, changes])
            else:
                dataframe = pd.concat([dataframe, changes])
        dataframe = dataframe.sort_values(["case:concept:name", "time:timestamp", "concept:name"])
        dataframe = dataframe.loc[:,~dataframe.columns.duplicated()]
        # drop duplicates
        print("before dropping", len(dataframe))
        dataframe = dataframe.drop_duplicates()
        print("after dropping", len(dataframe))
        dataframe = case_filter.filter_on_case_size(dataframe, "case:concept:name", min_case_size=1, max_case_size=constants.MAX_CASE_SIZE)
        dataframe["org:resource"] = dataframe["org:resource"].fillna(dataframe["USERNAME"])
        dictio_ustyp, dictio_class = extract_usr02.apply(con)
        dataframe["USERNAME_USTYP"] = dataframe["org:resource"].map(dictio_ustyp)
        dataframe["USERNAME_CLASS"] = dataframe["org:resource"].map(dictio_class)
        # postprocessing
        if enable_grouping:
            grouping_columns = ["case:concept:name", "time:timestamp", "org:resource", "FROMTABLE"]
            dct = dataframe.groupby(grouping_columns)["concept:name"].apply(set).to_dict()
            for k in dct:
                dct[k] = ",".join(sorted(list(dct[k])))
            dataframe["NEW_ACTIVITY_COLUMN"] = dataframe[grouping_columns].apply(tuple, axis=1).map(dct)
            dataframe["NEW_ACTIVITY_COLUMN"] = dataframe["NEW_ACTIVITY_COLUMN"].fillna(dataframe["concept:name"])
            dataframe["concept:name"] = dataframe["NEW_ACTIVITY_COLUMN"]
            del dataframe["NEW_ACTIVITY_COLUMN"]
            dataframe = dataframe.groupby(grouping_columns).first().reset_index()
            dataframe = dataframe.sort_values(["case:concept:name", "time:timestamp", "concept:name"])

            print("after grouping", len(dataframe))

    if not include_resource:
        if "USNAM" in dataframe.columns:
            del dataframe["USNAM"]
        if "ERNAM" in dataframe.columns:
            del dataframe["ERNAM"]
        if "USERNAME" in dataframe.columns:
            del dataframe["USERNAME"]
        if "org:resource" in dataframe.columns:
            del dataframe["org:resource"]

    print(dataframe.columns)

    return dataframe


def cli(con):
    print("\n\nP2P - CSV log\n")
    ref_type = input("Provide the central table for the extraction (default: EKKO):")
    if not ref_type:
        ref_type = "EKKO"
    dataframe = apply(con, ref_type=ref_type)
    path = input("Insert the path where the log should be saved (default: p2p.csv): ")
    if not path:
        path = "p2p.csv"
    dataframe.to_csv(path, sep=",", quotechar="\"",  index=False)
