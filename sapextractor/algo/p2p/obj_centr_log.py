from sapextractor.algo.p2p import p2p_common
from pm4pymdl.objects.jmd.exporter import exporter as jmd_exporter
from pm4pymdl.objects.mdl.exporter import exporter as mdl_exporter


def apply(con):
    dataframe, G, nodes_types = p2p_common.extract_tables_and_graph(con)
    dataframe["event_id"] = dataframe.index.astype(str)
    dataframe = dataframe.sort_values("event_timestamp")
    dataframe.type = "succint"
    return dataframe


def cli(con):
    print("\n\nP2P - Object-Centric Log\n")
    dataframe = apply(con)
    path = input("Insert the path where the log should be saved (default: p2p.mdl): ")
    if not path:
        path = "p2p.mdl"
    if path.endswith("mdl"):
        mdl_exporter.apply(dataframe, path)
    elif path.endswith("jmd"):
        jmd_exporter.apply(dataframe, path)
