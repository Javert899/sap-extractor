import pandas as pd

from sapextractor.utils.dates import timestamp_column_from_dt_tm
from sapextractor.utils.tstct import extract_tstct
from sapextractor.utils.fields_corresp import extract_dd03t
from dateutil import parser
import time, datetime

def read_cdhdr(con, objectclas=None):
    additional_query_part = " WHERE OBJECTCLAS = '" + objectclas + "'" if objectclas is not None else ""
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


def read_cdpos(con, objectclas=None, tabname=None):
    additional_query_part = " WHERE OBJECTCLAS = '" + objectclas + "'" if objectclas is not None else ""
    if tabname is not None and additional_query_part:
        additional_query_part += " AND TABNAME = '" + tabname + "'"
    df = con.prepare_and_execute_query("CDPOS", ["CHANGENR", "OBJECTID", "TABNAME", "FNAME", "VALUE_NEW", "VALUE_OLD", "CHNGIND"],
                                       additional_query_part=additional_query_part)
    return df


def give_field_desc(con, cdpos_dict):
    fnames = extract_dd03t.apply(con)
    for c in cdpos_dict:
        fname = c["FNAME"]
        tabname = c["TABNAME"]
        value_new = c["VALUE_NEW"]
        value_old = c["VALUE_OLD"]
        chngind = c["CHNGIND"]
        if fname not in fnames or fnames[fname] is None:
            fnames[fname] = fname
        if fname == "KOSTK" and value_new == "B":
            c["CHANGEDESC"] = "Picking: Partially Processed"
        elif fname == "KOSTK" and value_new == "C":
            c["CHANGEDESC"] = "Picking: Completely Processed"
        elif fname == "GBSTK" and value_new == "B":
            c["CHANGEDESC"] = "Set Order Status: Partially Processed"
        elif fname == "GBSTK" and value_new == "C":
            c["CHANGEDESC"] = "Set Order Status: Completely Processed"
        elif fname == "KOQUK" and value_new == "B":
            c["CHANGEDESC"] = "Pick Confirmation: Partially Processed"
        elif fname == "KOQUK" and value_new == "C":
            c["CHANGEDESC"] = "Pick Confirmation: Completely Processed"
        elif fname == "LVSTK" and value_new == "B":
            c["CHANGEDESC"] = "Warehouse Management: Partially Processed"
        elif fname == "LVSTK" and value_new == "C":
            c["CHANGEDESC"] = "Warehouse Management: Completely Processed"
        elif fname == "WBSTK" and value_new == "B":
            c["CHANGEDESC"] = "Total Goods Movement: Partially processed"
        elif fname == "WBSTK" and value_new == "C":
            c["CHANGEDESC"] = "Total Goods Movement: Completely processed"
        elif fname == "TRSTA" and value_new == "B":
            c["CHANGEDESC"] = "Transportation Status: Partially processed"
        elif fname == "TRSTA" and value_new == "C":
            c["CHANGEDESC"] = "Transportation Status: Completely processed"
        elif fname == "SPE_IMWRK" and value_new == "X":
            c["CHANGEDESC"] = "Item in Plant"
        elif fname == "MPROK" and value_new == "A":
            c["CHANGEDESC"] = "Manual price change carried out"
        elif fname == "MPROK" and value_new == "B":
            c["CHANGEDESC"] = "Condition manually deleted"
        elif fname == "MPROK" and value_new == "C":
            c["CHANGEDESC"] = "Manual price change released"
        elif fname == "SPE_REV_VLSTK" and value_new == "A":
            c["CHANGEDESC"] = "Distribution Status: Relevant"
        elif fname == "SPE_REV_VLSTK" and value_new == "B":
            c["CHANGEDESC"] = "Distribution Status: Distributed"
        elif fname == "SPE_REV_VLSTK" and value_new == "C":
            c["CHANGEDESC"] = "Distribution Status: Confirmed"
        elif fname == "SPE_REV_VLSTK" and value_new == "D":
            c["CHANGEDESC"] = "Distribution Status: Planned for Distribution"
        elif fname == "SPE_REV_VLSTK" and value_new == "E":
            c["CHANGEDESC"] = "Distribution Status: Delivery split was performed locally"
        elif fname == "SPE_REV_VLSTK" and value_new == "F":
            c["CHANGEDESC"] = "Distribution Status: Change Management Switched Off"
        elif fname == "VLSTK" and value_new == "A":
            c["CHANGEDESC"] = "Distribution Status: Relevant"
        elif fname == "VLSTK" and value_new == "B":
            c["CHANGEDESC"] = "Distribution Status: Distributed"
        elif fname == "VLSTK" and value_new == "C":
            c["CHANGEDESC"] = "Distribution Status: Confirmed"
        elif fname == "VLSTK" and value_new == "D":
            c["CHANGEDESC"] = "Distribution Status: Planned for Distribution"
        elif fname == "VLSTK" and value_new == "E":
            c["CHANGEDESC"] = "Distribution Status: Delivery split was performed locally"
        elif fname == "VLSTK" and value_new == "F":
            c["CHANGEDESC"] = "Distribution Status: Change Management Switched Off"
        elif fname == "SPE_GEN_ELIKZ" and value_new == "X":
            c["CHANGEDESC"] = "Delivery Completed"
        elif fname == "IMWRK" and value_new == "X":
            c["CHANGEDESC"] = "Delivery in Plant"
        elif fname == "UVVLS" and value_new == "A":
            c["CHANGEDESC"] = "Delivery Uncompletion: Not yet processed"
        elif fname == "UVVLS" and value_new == "B":
            c["CHANGEDESC"] = "Delivery Uncompletion: Partially processed"
        elif fname == "UVVLS" and value_new == "C":
            c["CHANGEDESC"] = "Delivery Uncompletion: Completely processed"
        elif fname == "FKDAT":
            try:
                value_new = parser.parse(value_new).timestamp()
            except:
                value_new = 0
            try:
                value_old = parser.parse(value_old).timestamp()
            except:
                value_old = 0
            if value_old == 0:
                c["CHANGEDESC"] = "Set Billing Date"
            elif value_new == 0:
                c["CHANGEDESC"] = "Remove Billing Date"
            elif value_new <= value_old:
                c["CHANGEDESC"] = "Anticipate Billing Date"
            else:
                c["CHANGEDESC"] = "Postpone Billing Date"
        elif fname == "FAKSK":
            if value_new is None:
                c["CHANGEDESC"] = "Remove Billing Block"
            else:
                c["CHANGEDESC"] = "Set Billing Block"
        elif fname == "SKFBP":
            value_new = float(value_new)
            value_old = float(value_old)
            if value_new <= value_old:
                c["CHANGEDESC"] = "Decrease Cash Discount"
            else:
                c["CHANGEDESC"] = "Increase Cash Discount"
        elif fname == "UVALL" and value_new == "A":
            c["CHANGEDESC"] = "Order Uncompletion: Not yet processed"
        elif fname == "UVALL" and value_new == "B":
            c["CHANGEDESC"] = "Order Uncompletion: Partially processed"
        elif fname == "UVALL" and value_new == "C":
            c["CHANGEDESC"] = "Order Uncompletion: Completely processed"
        elif fname == "LISPL" and value_new == "A":
            c["CHANGEDESC"] = "Delivery is for single warehouse"
        elif fname == "LISPL" and value_new == "B":
            c["CHANGEDESC"] = "Delivery is not for single warehouse"
        elif fname == "WADAT_IST":
            try:
                value_new = parser.parse(value_new).timestamp()
            except:
                value_new = 0
            try:
                value_old = parser.parse(value_old).timestamp()
            except:
                value_old = 0
            if value_old == 0:
                c["CHANGEDESC"] = "Set Actual Goods Movement Date"
            elif value_new == 0:
                c["CHANGEDESC"] = "Remove Actual Goods Movement Date"
            elif value_new <= value_old:
                c["CHANGEDESC"] = "Anticipate Actual Goods Movement Date"
            else:
                c["CHANGEDESC"] = "Postpone Actual Goods Movement Date"
        elif fname == "KEY" and tabname == "FPLT" and chngind == "I":
            c["CHANGEDESC"] = "Insert Billing Plan"
        elif fname == "KEY" and tabname == "FPLT" and chngind == "D":
            c["CHANGEDESC"] = "Remove Billing Plan"
        elif fname == "KEY" and tabname == "VBEP" and chngind == "D":
            c["CHANGEDESC"] = "Remove Schedule Line"
        elif fname == "KEY" and tabname == "VBEP" and chngind == "I":
            c["CHANGEDESC"] = "Insert Schedule Line"
        elif fname == "KEY" and tabname == "VBAK" and chngind == "D":
            c["CHANGEDESC"] = "Cancel Order"
        elif fname == "KEY" and tabname == "VBAP" and chngind == "D":
            c["CHANGEDESC"] = "Remove Order Item"
        elif fname == "KEY" and tabname == "VBAP" and chngind == "I":
            c["CHANGEDESC"] = "Insert Order Item"
        elif fname == "KEY" and tabname == "LIPS" and chngind == "D":
            c["CHANGEDESC"] = "Remove Delivery Item"
        elif fname == "KEY" and tabname == "LIPS" and chngind == "I":
            c["CHANGEDESC"] = "Insert Delivery Item"
        else:
            c["CHANGEDESC"] = "Change "+fnames[fname]

    return cdpos_dict


def apply(con, objectclas=None, tabname=None):
    cdhdr = read_cdhdr(con, objectclas=objectclas)
    cdpos = read_cdpos(con, objectclas=objectclas, tabname=tabname)
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
                                      tabname=None):
    changes = apply(con, objectclas=objectclas, tabname=tabname)
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
