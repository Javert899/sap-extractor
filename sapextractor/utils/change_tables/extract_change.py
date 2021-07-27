import pandas as pd

from sapextractor.utils.dates import timestamp_column_from_dt_tm
from sapextractor.utils.tstct import extract_tstct
from sapextractor.utils.fields_corresp import extract_dd03t
from sapextractor.utils.change_tables import mapping


def read_cdhdr(con, objectclas=None, mandt="800", ap=""):
    additional_query_part = " WHERE OBJECTCLAS = '" + objectclas + "' AND MANDANT = '"+mandt+"'" if objectclas is not None else "WHERE MANDANT = '"+mandt+"'"
    if ap:
        additional_query_part += " AND OBJECTID IN ("+ap+")"
    df = con.prepare_and_execute_query("CDHDR", ["CHANGENR", "USERNAME", "UDATE", "UTIME", "TCODE"],
                                       additional_query_part=additional_query_part)
    df.columns = ["event_" + x for x in df.columns]
    if len(df) > 0:
        df = timestamp_column_from_dt_tm.apply(df, "event_UDATE", "event_UTIME", "event_timestamp")
        df = df.sort_values("event_timestamp")
    transactions = set(df["event_TCODE"].unique())
    tstct = extract_tstct.apply_static(con, transactions=transactions)
    df["event_ONLYACT"] = df["event_TCODE"].map(tstct)
    return df


def read_cdpos(con, objectclas=None, tabname=None, mandt="800", ap=""):
    additional_query_part = " WHERE OBJECTCLAS = '" + objectclas + "' AND MANDANT = '"+mandt+"'" if objectclas is not None else "WHERE MANDANT = '"+mandt+"'"
    if tabname is not None and additional_query_part:
        additional_query_part += " AND TABNAME = '" + tabname + "'"
    if ap:
        additional_query_part += " AND OBJECTID IN ("+ap+")"
    df = con.prepare_and_execute_query("CDPOS", ["CHANGENR", "OBJECTID", "TABNAME", "FNAME", "VALUE_NEW", "VALUE_OLD", "CHNGIND"],
                                       additional_query_part=additional_query_part)
    return df


def give_field_desc(con, cdpos_dict):
    #fnames = extract_dd03t.apply(con)
    fnames = {}
    for c in cdpos_dict:
        fname = c["FNAME"]
        tabname = c["TABNAME"]
        value_new = c["VALUE_NEW"]
        value_old = c["VALUE_OLD"]
        chngind = c["CHNGIND"]

        change, typ = mapping.perform_mapping(tabname, fname, value_old, value_new, chngind, fnames)
        c["CHANGEDESC"] = change


    return cdpos_dict


def apply(con, objectclas=None, tabname=None, mandt="800", additional_query_part=""):
    cdhdr = read_cdhdr(con, objectclas=objectclas, mandt=mandt, ap=additional_query_part)
    cdpos = read_cdpos(con, objectclas=objectclas, tabname=tabname, mandt=mandt, ap=additional_query_part)
    grouped_cdhdr = cdhdr.groupby("event_CHANGENR")
    change_dictio = {}
    for name, group in grouped_cdhdr:
        change_dictio[name] = group
    del grouped_cdhdr
    cdpos = cdpos.to_dict('records')
    cdpos = give_field_desc(con, cdpos)
    ret = {}
    for el in cdpos:
        changenr = el["CHANGENR"]
        objectid = el["OBJECTID"]
        tabname = el["TABNAME"]
        fname = el["FNAME"]
        value_new = el["VALUE_NEW"]
        changedesc = el["CHANGEDESC"]
        chngind = el["CHNGIND"]
        if changenr in change_dictio:
            if objectid not in ret:
                ret[objectid] = []
            df = change_dictio[changenr].copy()
            df["event_AWKEY"] = objectid
            df["event_TABNAME"] = tabname
            df["event_FNAME"] = fname
            df["event_VALUE_NEW"] = value_new
            df["event_CHANGEDESC"] = changedesc
            df["event_CHNGIND"] = chngind
            ret[objectid].append(df)
    for objectid in ret:
        ret[objectid] = pd.concat(ret[objectid])
    return ret


def get_changes_dataframe_two_mapping(con, orig_df, change_key, target_key, resource_column=None, objectclas=None,
                                      tabname=None, mandt="800"):
    changes = apply(con, objectclas=objectclas, tabname=tabname, mandt=mandt)
    mapping = {x[change_key]: x[target_key] for x in
               orig_df[[change_key, target_key]].dropna(subset=[change_key, target_key], how="any").to_dict("records")}
    changes = {x: y for x, y in changes.items() if x in mapping}
    changes = [y for y in changes.values()]
    if changes:
        changes = pd.concat(changes)
        changes[target_key] = changes[change_key].map(mapping)
        changes = changes.dropna(subset=[change_key, target_key, "event_ONLYACT"], how="any")
        if resource_column is not None:
            changes = changes.rename(columns={"event_USERNAME": resource_column})
    else:
        changes = pd.DataFrame()
    return changes
