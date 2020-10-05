from sapextractor.algo.o2c import o2c_common
from pm4pymdl.objects.jmd.exporter import exporter as jmd_exporter
from pm4pymdl.objects.mdl.exporter import exporter as mdl_exporter


def apply(con, keep_first=True):
    dataframe = o2c_common.apply(con, keep_first=keep_first)
    dataframe["event_id"] = dataframe.index.astype(str)
    dataframe = dataframe.sort_values("event_timestamp")
    dataframe.type = "succint"
    return dataframe


def cli(con):
    print("\n\nO2C Object-Centric Log Extractor\n\n")
    dataframe = apply(con)
    path = input("Insert the path where the log should be saved (default: o2c.mdl): ")
    if not path:
        path = "o2c.mdl"
    if path.endswith("mdl"):
        mdl_exporter.apply(dataframe, path)
    elif path.endswith("jmd"):
        jmd_exporter.apply(dataframe, path)
