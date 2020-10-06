from sapextractor.algo.p2p.tab_processing import eban_processing, ekko_processing, ekpo_processing
from sapextractor.algo.p2p.tab_processing import mkpf_processing, mseg_processing
import networkx as nx
import pandas as pd


def add_edges_to_graph(edges, nodes_connections, G):
    for edge in edges:
        if edge[0] in G.nodes and edge[1] in G.nodes:
            G.add_edge(edge[0], edge[1])
        if not edge[0] in nodes_connections:
            nodes_connections[edge[0]] = {edge[0]}
        if not edge[1] in nodes_connections:
            nodes_connections[edge[1]] = {edge[1]}
        nodes_connections[edge[0]].add(edge[1])
    return nodes_connections, G


def extract_tables_and_graph(con):
    G = nx.DiGraph()
    nodes_types = {}
    nodes_connections = {}
    eban, eban_nodes_types = eban_processing.apply(con)
    for n in eban_nodes_types:
        G.add_node(n)
    nodes_types.update(eban_nodes_types)
    ekko, ekko_nodes_types = ekko_processing.apply(con)
    for n in ekko_nodes_types:
        G.add_node(n)
    nodes_types.update(ekko_nodes_types)
    eban_ekko_connection = ekpo_processing.eban_ekko_connection(con)
    nodes_connections, G = add_edges_to_graph(eban_ekko_connection, nodes_connections, G)

    mkpf, mkpf_nodes_types = mkpf_processing.apply(con)
    for n in mkpf_nodes_types:
        G.add_node(n)
    nodes_types.update(mkpf_nodes_types)
    ekko_mkpf_connection = mseg_processing.ekko_mkpf_connection(con)
    nodes_connections, G = add_edges_to_graph(ekko_mkpf_connection, nodes_connections, G)

    nodes_connections = pd.DataFrame([{"node": x, "RELATED_DOCUMENTS": list(y)} for x, y in nodes_connections.items()])
    dataframe = pd.concat([eban, ekko, mkpf])
    dataframe = dataframe.sort_values("event_timestamp")
    dataframe = dataframe.merge(nodes_connections, left_on="event_node", right_on="node", suffixes=('', '_r'), how="left")
    return dataframe, G, nodes_types
