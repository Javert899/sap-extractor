import pandas as pd
from datetime import datetime


class Shared:
    change_dictio = {}


def read_cdhdr(con):
    df = con.execute_sql("SELECT CHANGENR, USERNAME, UDATE, UTIME, TCODE FROM CDHDR")
    df["UDATE"] = pd.to_datetime(df["UDATE"]).apply(lambda x: x.timestamp())
    df["UTIME"] = pd.to_datetime(df["UTIME"]).apply(lambda x: x.timestamp())
    df["time:timestamp"] = df["UDATE"] + df["UTIME"]
    df["time:timestamp"] = df["time:timestamp"].apply(lambda x: datetime.fromtimestamp(x))
    return df


def read_cdpos(con):
    df = con.execute_sql("SELECT CHANGENR, OBJECTID FROM CDPOS")
    return df


def apply(con):
    cdhdr = read_cdhdr(con)
    cdpos = read_cdpos(con)
    grouped_cdhdr = cdhdr.groupby("CHANGENR")
    change_dictio = {}
    for name, group in grouped_cdhdr:
        change_dictio[name] = group
    del grouped_cdhdr
    cdpos = cdpos.to_dict('r')
    for el in cdpos:
        changenr = el["CHANGENR"]
        objectid = el["OBJECTID"]
        if changenr in change_dictio:
            if objectid not in change_dictio:
                change_dictio[objectid] = []
            change_dictio[objectid].append(change_dictio[changenr])
    return change_dictio


def apply_static(con):
    if not Shared.change_dictio:
        Shared.change_dictio = apply(con)
    return Shared.change_dictio
