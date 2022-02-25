import pandas as pd
from datetime import datetime
from sapextractor.utils import constants
import traceback


def apply(dataframe, dt_column, tm_column, target_column):
    if "str" in str(dataframe[dt_column].dtype) or "obj" in str(dataframe[dt_column].dtype):
        #dataframe[dt_column] = pd.to_datetime(dataframe[dt_column], format=constants.DATE_FORMAT_INTERNAL, errors='coerce')
        dataframe[dt_column] = pd.to_datetime(dataframe[dt_column], errors='coerce')
        dataframe = dataframe.dropna(subset=[dt_column])

    if "str" in str(dataframe[tm_column].dtype) or "obj" in str(dataframe[tm_column].dtype):
        dataframe[tm_column] = pd.to_datetime(dataframe[tm_column], format=constants.HOUR_FORMAT_INTERNAL, errors='coerce')
        dataframe = dataframe.dropna(subset=[tm_column])

    print(dataframe)
    if len(dataframe) > 0:
        dataframe[dt_column] = dataframe[dt_column].apply(
            lambda x: datetime(year=x.year, month=x.month, day=x.day).timestamp())
        dataframe[tm_column] = dataframe[tm_column].apply(lambda x: x.timestamp())

        dataframe[tm_column] = dataframe[tm_column] + 2208988800

        dataframe[target_column] = dataframe[dt_column] + dataframe[tm_column]
        dataframe[target_column] = dataframe[target_column].apply(lambda x: datetime.fromtimestamp(x))
        dataframe = dataframe.sort_values("event_timestamp")
        dataframe = dataframe.dropna(subset=["event_timestamp"], how="any")

    return dataframe
