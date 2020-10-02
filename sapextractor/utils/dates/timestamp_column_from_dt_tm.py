import pandas as pd
from datetime import datetime


def apply(dataframe, dt_column, tm_column, target_column):
    dataframe[dt_column] = pd.to_datetime(dataframe[dt_column])
    dataframe[tm_column] = pd.to_datetime(dataframe[tm_column])
    dataframe[dt_column] = dataframe[dt_column].apply(lambda x: x.timestamp())
    dataframe[tm_column] = dataframe[tm_column].apply(lambda x: x.timestamp())
    dataframe[target_column] = dataframe[dt_column] + dataframe[tm_column]
    dataframe[target_column] = dataframe[target_column].apply(lambda x: datetime.fromtimestamp(x))
    dataframe = dataframe.sort_values("event_timestamp")
    dataframe = dataframe.dropna(subset=["event_timestamp"], how="any")
    return dataframe
