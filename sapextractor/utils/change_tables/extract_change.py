import pandas as pd
from datetime import datetime
from sapextractor.utils.tstct import extract_tstct
from sapextractor.utils.dates import timestamp_column_from_dt_tm

class Shared:
    change_dictio = {}


def read_cdhdr(con):
    df = con.prepare_and_execute_query("CDHDR", ["CHANGENR", "USERNAME", "UDATE", "UTIME", "TCODE"])
    df = timestamp_column_from_dt_tm.apply(df, "UDATE", "UTIME", "event_timestamp")
    df = df.sort_values("event_timestamp")
    transactions = set(df["TCODE"].unique())
    tstct = extract_tstct.apply_static(con, transactions=transactions)
    df["event_activity"] = df["TCODE"].map(tstct)
    return df


def read_cdpos(con):
    df = con.prepare_and_execute_query("CDPOS", ["CHANGENR", "OBJECTID"])
    return df


def apply(con):
    cdhdr = read_cdhdr(con)
    cdpos = read_cdpos(con)
    grouped_cdhdr = cdhdr.groupby("CHANGENR")
    change_dictio = {}
    for name, group in grouped_cdhdr:
        change_dictio[name] = group
    del grouped_cdhdr
    cdpos = cdpos.to_dict('records')
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
