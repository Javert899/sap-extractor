import pandas as pd
from datetime import datetime
from sapextractor.utils import constants
import traceback


def apply(dataframe, dt_column, tm_column, target_column):
    try:
        dataframe[dt_column] = pd.to_datetime(dataframe[dt_column], format=constants.DATE_FORMAT_INTERNAL, errors='coerce')
        dataframe = dataframe.dropna(subset=[dt_column])
    except:
        print("a failed")
        print(dataframe[dt_column])
        traceback.print_exc()

    try:
        dataframe[tm_column] = pd.to_datetime(dataframe[tm_column], format=constants.HOUR_FORMAT_INTERNAL, errors='coerce')
        dataframe = dataframe.dropna(subset=[tm_column])
    except:
        print("b failed")
        print(dataframe[tm_column])
        traceback.print_exc()


    dataframe[dt_column] = dataframe[dt_column].apply(
        lambda x: datetime(year=x.year, month=x.month, day=x.day).timestamp())
    dataframe[tm_column] = dataframe[tm_column].apply(lambda x: x.timestamp())

    dataframe[tm_column] = dataframe[tm_column] + 2208988800

    if len(dataframe) > 0:
        dataframe[target_column] = dataframe[dt_column] + dataframe[tm_column]
        dataframe[target_column] = dataframe[target_column].apply(lambda x: datetime.fromtimestamp(x))
        dataframe = dataframe.sort_values("event_timestamp")
        dataframe = dataframe.dropna(subset=["event_timestamp"], how="any")

    return dataframe
