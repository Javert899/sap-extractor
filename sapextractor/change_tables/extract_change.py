import pandas as pd
from datetime import datetime


def read_cdhdr(con):
    df = pd.read_sql("SELECT CHANGENR, USERNAME, UDATE, UTIME, TCODE FROM CDHDR", con)
    df.columns = [x.upper() for x in df.columns]
    df["UDATE"] = pd.to_datetime(df["UDATE"]).apply(lambda x: x.timestamp())
    df["UTIME"] = pd.to_datetime(df["UTIME"]).apply(lambda x: x.timestamp())
    df["time:timestamp"] = df["UDATE"] + df["UTIME"]
    df["time:timestamp"] = df["time:timestamp"].apply(lambda x: datetime.fromtimestamp(x))
    return df


def read_cdpos(con):
    df = pd.read_sql("SELECT CHANGENR, OBJECTID FROM CDPOS", con)
    df.columns = [x.upper() for x in df.columns]
    return df


def apply(con):
    cdhdr = read_cdhdr(con)
    cdpos = read_cdpos(con)
    grouped_cdhdr = cdhdr.groupby("CHANGENR")
    final_dictio = {}
    for name, group in grouped_cdhdr:
        final_dictio[name] = group
    del grouped_cdhdr
    cdpos = cdpos.to_dict('r')
    for el in cdpos:
        changenr = el["CHANGENR"]
        objectid = el["OBJECTID"]
        if changenr in final_dictio:
            if objectid not in final_dictio:
                final_dictio[objectid] = []
            final_dictio[objectid].append(final_dictio[changenr])
    return final_dictio
