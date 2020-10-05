from sapextractor.algo.p2p import p2p_common
from sapextractor.utils.graph_building import build_graph


def apply(con, ref_type="EKKO"):
    dataframe, G, nodes_types = p2p_common.extract_tables_and_graph(con)
    anc_succ = build_graph.get_ancestors_successors_from_graph(G, nodes_types, ref_type=ref_type)
    dataframe = dataframe.merge(anc_succ, left_on="event_node", right_on="node", suffixes=('', '_r'), how="right")
    dataframe = dataframe.dropna(subset=["event_activity", "event_timestamp"])
    dataframe = dataframe.rename(columns={"event_activity": "concept:name", "event_timestamp": "time:timestamp"})
    return dataframe
