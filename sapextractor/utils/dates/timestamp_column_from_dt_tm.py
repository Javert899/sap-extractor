import pandas as pd
from datetime import datetime
from sapextractor.utils import constants


def apply(dataframe, dt_column, tm_column, target_column):
    try:
        if str(dataframe[dt_column].dtype) != "object":
            print("a")
            dataframe[dt_column] = dataframe[dt_column].apply(lambda x: x.strftime(constants.DATE_FORMAT_INTERNAL))
        if str(dataframe[tm_column].dtype) != "object":
            print("b")
            dataframe[tm_column] = dataframe[tm_column].apply(lambda x: x.strftime(constants.HOUR_FORMAT_INTERNAL))
        dataframe[target_column] = dataframe[dt_column] + " " + dataframe[tm_column]
        print("c")
        dataframe[target_column] = pd.to_datetime(dataframe[target_column], format=constants.TIMESTAMP_FORMAT)
        print("d")
        dataframe = dataframe.sort_values("event_timestamp")
        dataframe = dataframe.dropna(subset=["event_timestamp"], how="any")
    except:
        print("e")
        dataframe[dt_column] = pd.to_datetime(dataframe[dt_column], format=constants.DATE_FORMAT_INTERNAL)
        print("f")
        dataframe[tm_column] = pd.to_datetime(dataframe[tm_column], format=constants.HOUR_FORMAT_INTERNAL)
        print("g")
        dataframe[dt_column] = dataframe[dt_column].apply(lambda x: x.timestamp())
        print("h")
        dataframe[tm_column] = dataframe[tm_column].apply(lambda x: x.timestamp())
        print("i")

        if len(dataframe) > 0:
            dataframe[target_column] = dataframe[dt_column] + dataframe[tm_column]
            dataframe[target_column] = dataframe[target_column].apply(lambda x: datetime.fromtimestamp(x))
            dataframe = dataframe.sort_values("event_timestamp")
            dataframe = dataframe.dropna(subset=["event_timestamp"], how="any")

    return dataframe
