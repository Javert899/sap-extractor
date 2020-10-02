from sapextractor.algo.o2c import o2c_common


def apply(con):
    dataframe = o2c_common.apply(con)
    dataframe = dataframe[[x for x in dataframe.columns if x.startswith("event_")]]
    cols = {x: x.split("event_")[1] for x in dataframe.columns}
    cols["event_activity"] = "concept:name"
    cols["event_timestamp"] = "time:timestamp"
    dataframe = dataframe.rename(columns=cols)
    print(dataframe)
