from sapextractor.algo.p2p import p2p_common
from sapextractor.utils.graph_building import build_graph
from sapextractor.utils.filters import case_filter
from sapextractor.utils import constants


def apply(con, ref_type="EKKO"):
    dataframe, G, nodes_types = p2p_common.extract_tables_and_graph(con)
    dataframe = dataframe[[x for x in dataframe.columns if x.startswith("event_")]]
    anc_succ = build_graph.get_ancestors_successors_from_graph(G, nodes_types, ref_type=ref_type)
    dataframe = dataframe.merge(anc_succ, left_on="event_node", right_on="node", suffixes=('', '_r'), how="right")
    dataframe = dataframe.dropna(subset=["event_activity", "event_timestamp"])
    cols = {x: x.split("event_")[-1] for x in dataframe.columns}
    cols["event_activity"] = "concept:name"
    cols["event_timestamp"] = "time:timestamp"
    cols["event_USERNAME"] = "org:resource"
    dataframe = dataframe.rename(columns=cols)
    dataframe = dataframe.sort_values("time:timestamp")
    dataframe = dataframe.loc[:,~dataframe.columns.duplicated()]
    dataframe = case_filter.filter_on_case_size(dataframe, "case:concept:name", min_case_size=1, max_case_size=constants.MAX_CASE_SIZE)
    return dataframe


def cli(con):
    print("\n\nP2P - XES log\n")
    ref_type = input("Provide the central table for the extraction (default: EKKO):")
    if not ref_type:
        ref_type = "EKKO"
    dataframe = apply(con, ref_type=ref_type)
    path = input("Insert the path where the log should be saved (default: p2p.csv): ")
    if not path:
        path = "p2p.csv"
    dataframe.to_csv(path, sep=",", quotechar="\"",  index=False)
