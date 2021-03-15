from sapextractor.algo.p2p import p2p_common
from pm4pymdl.objects.ocel.exporter import exporter as jmd_exporter
from pm4pymdl.objects.mdl.exporter import exporter as mdl_exporter


def apply(con, gjahr="2014", min_extr_date="2014-01-01 00:00:00", mandt="800", bukrs="1000"):
    dataframe, G, nodes_types = p2p_common.extract_tables_and_graph(con, gjahr=gjahr, min_extr_date=min_extr_date, mandt=mandt, bukrs=bukrs)
    if len(dataframe) > 0:
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
