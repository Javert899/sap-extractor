from sapextractor.algo.p2p.tab_processing import eban_processing, ekko_processing, ekpo_processing
import networkx as nx
import pandas as pd


def extract_tables_and_graph(con):
    G = nx.DiGraph()
    nodes_types = {}
    eban, eban_nodes_types = eban_processing.apply(con)
    for n in eban_nodes_types:
        G.add_node(n)
    nodes_types.update(eban_nodes_types)
    ekko, ekko_nodes_types = ekko_processing.apply(con)
    for n in ekko_nodes_types:
        G.add_node(n)
    nodes_types.update(ekko_nodes_types)
    eban_ekpo_connection = ekpo_processing.eban_ekko_connection(con)
    for edge in eban_ekpo_connection:
        if edge[0] in G.nodes and edge[1] in G.nodes:
            G.add_edge(edge[0], edge[1])
    dataframe = pd.concat([eban, ekko])
    dataframe = dataframe.sort_values("event_timestamp")
    return dataframe, G, nodes_types
