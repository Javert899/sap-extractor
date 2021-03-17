from datetime import datetime

import pandas as pd

from sapextractor.utils import constants
from sapextractor.utils.blart import extract_blart
from sapextractor.utils.change_tables import extract_change
from sapextractor.utils.dates import timestamp_column_from_dt_tm
from sapextractor.utils.tstct import extract_tstct


def extract_bkpf(con, gjahr="1997", mandt="800", bukrs="1000"):
    additional_query_part = " WHERE GJAHR = '"+gjahr+"' AND MANDT = '"+mandt+"' AND BUKRS = '"+bukrs+"'"
    bkpf = con.prepare_and_execute_query("BKPF", ["BELNR", "BLART", "CPUDT", "CPUTM", "USNAM", "TCODE", "AWKEY"], additional_query_part=additional_query_part)
    bkpf = bkpf.dropna(subset=["BELNR", "TCODE", "BLART"], how="any")
    transactions = set(bkpf["TCODE"].unique())
    doc_types = set(bkpf["BLART"].unique())
    tcodes = extract_tstct.apply_static(con, transactions=transactions)
    blart = extract_blart.apply_static(con, doc_types=doc_types)
    cols = {x: "event_" + x for x in bkpf.columns}
    bkpf = bkpf.rename(columns=cols)
    bkpf["event_ONLYACT"] = bkpf["event_TCODE"].map(tcodes)
    bkpf["event_BLART"] = bkpf["event_BLART"].map(blart)
    bkpf = bkpf.dropna(subset=["event_ONLYACT", "event_BLART"], how="any")
    bkpf["INVOLVED_DOCUMENTS"] = bkpf["event_BELNR"].astype(str)
    bkpf["INVOLVED_DOCUMENTS"] = bkpf["INVOLVED_DOCUMENTS"].apply(constants.set_documents)
    bkpf = timestamp_column_from_dt_tm.apply(bkpf, "event_CPUDT", "event_CPUTM", "event_timestamp")
    bkpf_first = bkpf.groupby("event_BELNR").first().reset_index()
    first_stream = bkpf_first[["event_BELNR", "event_timestamp", "event_BLART"]].to_dict("records")
    doc_first_dates = {x["event_BELNR"]: x["event_timestamp"] for x in first_stream}
    doc_types = {x["event_BELNR"]: x["event_BLART"] for x in first_stream}

    return bkpf, doc_first_dates, doc_types


def extract_bseg(con, doc_first_dates, doc_types, gjahr="1997", mandt="800", bukrs="1000"):
    additional_query_part = " WHERE GJAHR = '"+gjahr+"' AND MANDT = '"+mandt+"' AND BUKRS = '"+bukrs+"'"
    bseg = con.prepare_and_execute_query("BSAK", ["BELNR", "GJAHR", "BUZEI", "AUGDT", "AUGBL"], additional_query_part=additional_query_part)
    bseg = bseg.dropna(subset=["BELNR", "AUGBL", "AUGDT"], how="any")
    bseg["BLART"] = bseg["BELNR"].map(doc_types)
    bseg["BELNR_TYPE"] = bseg["BELNR"].map(doc_types)
    bseg["AUGBL_TYPE"] = bseg["AUGBL"].map(doc_types)
    bseg = bseg.dropna(subset=["BELNR_TYPE", "AUGBL_TYPE"])
    if len(bseg) > 0:
        bseg["AUGDT"] = pd.to_datetime(bseg["AUGDT"], format=con.DATE_FORMAT)
        bseg["AUGDT"] = bseg["AUGDT"].apply(lambda x: x.timestamp())
        bseg["AUGDT"] = bseg["AUGDT"] + 86399
        bseg["AUGDT"] = bseg["AUGDT"].apply(lambda x: datetime.fromtimestamp(x))
    cols = {x: "event_" + x for x in bseg.columns}
    bseg = bseg.rename(columns=cols)
    bseg["INVOLVED_DOCUMENTS"] = bseg["event_BELNR"].astype(str) + constants.DOC_SEP + bseg["event_AUGBL"].astype(str)
    bseg["INVOLVED_DOCUMENTS"] = bseg["INVOLVED_DOCUMENTS"].apply(constants.set_documents)
    if len(bseg) > 0:
        bseg["event_timestamp"] = bseg["event_AUGDT"]
        bseg = bseg.dropna(subset=["event_timestamp"], how="any")
        bseg["event_ONLYACT"] = "Clear Document"
        bseg["event_DOCTYPE"] = bseg["event_BELNR"].map(doc_types)

    return bseg


def extract_changes_bkpf(con, bkpf, doc_types, gjahr="1997", mandt="800", bukrs="1000"):
    changes = extract_change.get_changes_dataframe_two_mapping(con, bkpf, "event_AWKEY", "event_BELNR",
                                                               resource_column="event_USNAM", objectclas="BELEG",
                                                               tabname="BKPF", mandt=mandt)
    if len(changes) > 0:
        changes["event_BLART"] = changes["event_BELNR"].map(doc_types)
        changes = changes.dropna(subset=["event_BLART"], how="any")
        changes["INVOLVED_DOCUMENTS"] = changes["event_BELNR"].astype(str)
        changes["INVOLVED_DOCUMENTS"] = changes["INVOLVED_DOCUMENTS"].apply(constants.set_documents)
    return changes


def get_single_dataframes(con, gjahr="1997", mandt="800", bukrs="1000", filter_columns=False):
    bkpf, doc_first_dates, doc_types = extract_bkpf(con, gjahr=gjahr, bukrs=bukrs, mandt=mandt)
    bseg = extract_bseg(con, doc_first_dates, doc_types, gjahr=gjahr, bukrs=bukrs, mandt=mandt)
    if filter_columns:
        bkpf = bkpf[[x for x in bkpf.columns if x.startswith("event_")]]
        bseg = bseg[[x for x in bseg.columns if x.startswith("event_")]]
    return bkpf, bseg


def get_full_dataframe(con, gjahr="1997", mandt="800", bukrs="1000", filter_columns=False):
    bkpf, bseg = get_single_dataframes(con, gjahr=gjahr, mandt=mandt, bukrs=bukrs, filter_columns=filter_columns)
    ret = pd.concat([bkpf, bseg])
    return ret
