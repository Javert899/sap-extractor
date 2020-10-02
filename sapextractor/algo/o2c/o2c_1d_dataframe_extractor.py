from sapextractor.algo.o2c import o2c_common
from sapextractor.utils.graph_building import build_graph


def apply(con, ref_type="Order", keep_first=True):
    dataframe = o2c_common.apply(con, keep_first=keep_first)
    dataframe = dataframe[[x for x in dataframe.columns if x.startswith("event_")]]
    cols = {x: x.split("event_")[1] for x in dataframe.columns}
    cols["event_activity"] = "concept:name"
    cols["event_timestamp"] = "time:timestamp"
    dataframe = dataframe.rename(columns=cols)
    ancest_succ = build_graph.get_ancestors_successors(dataframe, "VBELV", "VBELN", "VBTYP_V", "VBTYP_N",
                                                       ref_type=ref_type)
    dataframe = dataframe.merge(ancest_succ, left_on="VBELN", right_on="node", suffixes=('', '_r'), how="right")
    dataframe = dataframe.reset_index()
    if keep_first:
        dataframe = dataframe.groupby("VBELN").first()
    dataframe = dataframe.sort_values("time:timestamp")
    return dataframe
