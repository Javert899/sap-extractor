from sapextractor.algo.o2c import o2c_common
from sapextractor.utils.graph_building import build_graph


def apply(con, ref_type="Invoice", keep_first=True):
    dataframe = o2c_common.apply(con, keep_first=keep_first)
    dataframe = dataframe[[x for x in dataframe.columns if x.startswith("event_")]]
    cols = {x: x.split("event_")[-1] for x in dataframe.columns}
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
    dataframe.to_csv(path, index=False)
