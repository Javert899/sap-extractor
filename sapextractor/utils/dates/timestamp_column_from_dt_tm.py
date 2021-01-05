import pandas as pd
from datetime import datetime
from sapextractor.utils import constants


def apply(dataframe, dt_column, tm_column, target_column):
    try:
        dataframe[target_column] = dataframe[dt_column] + " " + dataframe[tm_column]
        dataframe[target_column] = pd.to_datetime(dataframe[target_column], format=constants.TIMESTAMP_FORMAT)
        dataframe = dataframe.sort_values("event_timestamp")
        dataframe = dataframe.dropna(subset=["event_timestamp"], how="any")
    except:
        dataframe[dt_column] = pd.to_datetime(dataframe[dt_column])
        dataframe[tm_column] = pd.to_datetime(dataframe[tm_column])
        dataframe[dt_column] = dataframe[dt_column].apply(lambda x: x.timestamp())
        dataframe[tm_column] = dataframe[tm_column].apply(lambda x: x.timestamp())

        if len(dataframe) > 0:
            dataframe[target_column] = dataframe[dt_column] + dataframe[tm_column]
            dataframe[target_column] = dataframe[target_column].apply(lambda x: datetime.fromtimestamp(x))
            dataframe = dataframe.sort_values("event_timestamp")
            dataframe = dataframe.dropna(subset=["event_timestamp"], how="any")

    return dataframe
