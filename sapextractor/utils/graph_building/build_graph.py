import networkx as nx
import pandas as pd


def apply(df, prev, curr, prev_type, curr_type):
    prev_nodes = set(df.dropna(subset=[prev], how="any")[prev].unique())
    succ_nodes = set(df.dropna(subset=[curr], how="any")[curr].unique())
    all_nodes = prev_nodes.union(succ_nodes)
    edges = set()
    df = df.dropna(subset=[prev, curr], how="any")
    stream = df[[prev, curr, prev_type, curr_type]].to_dict('records')
    types = {}
    for el in stream:
        source = el[prev]
        target = el[curr]
        type_source = el[prev_type]
        type_target = el[curr_type]
        types[source] = type_source
        types[target] = type_target
        if source is not None and target is not None:
            if str(source).lower() != "nan" and str(target).lower():
                edges.add((source, target))
    G = nx.DiGraph()
    for node in all_nodes:
        G.add_node(node)
    for edge in edges:
        G.add_edge(edge[0], edge[1])
    return G, types


def get_conn_comp(df, prev, curr, prev_type, curr_type, ref_type=""):
    G, types = apply(df, prev, curr, prev_type, curr_type)

    conn_comp = nx.connected_components(nx.Graph(G))

    list_corresp = []
    for index, cc in enumerate(conn_comp):
        for n2 in cc:
            this_type = types[n2] if n2 in types else ""
            list_corresp.append({"node": n2, "type": this_type, "case:concept:name": str(index)})

    dataframe = pd.DataFrame(list_corresp).sort_values("node")

    return dataframe


def get_ancestors_successors(df, prev, curr, prev_type, curr_type, ref_type="", all_docs=None):
    G, types = apply(df, prev, curr, prev_type, curr_type)

    return get_ancestors_successors_from_graph(G, types, ref_type=ref_type, all_docs=all_docs)


def get_ancestors_successors_from_graph(G, types, ref_type="", all_docs=None):
    list_corresp = []

    nodes_ref_type = {x for x,y in types.items() if y == ref_type}

    index = 0
    for node in nodes_ref_type:
        all_ancestors = set(nx.ancestors(G, node))
        all_descendants = set(nx.descendants(G, node))
        all_nodes = all_ancestors.union(all_descendants).union({node})
        for n2 in all_nodes:
            new_el = {"node": n2, "type": types[n2], "case:concept:name": str(index)}
            list_corresp.append(new_el)
        index = index + 1

    if all_docs is not None:
        all_docs = all_docs.difference(nodes_ref_type)
        for node in all_docs:
            new_el = {"node": node, "type": "", "case:concept:name": str(index)}
            list_corresp.append(new_el)
            index = index + 1

    if list_corresp:
        dataframe = pd.DataFrame(list_corresp).sort_values("node")
    else:
        dataframe = pd.DataFrame({"node": []})
    return dataframe
