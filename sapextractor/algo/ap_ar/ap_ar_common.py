from sapextractor.utils.tstct import extract_tstct
from sapextractor.utils.blart import extract_blart
from sapextractor.utils.change_tables import extract_change
import pandas as pd
from sapextractor.utils.dates import timestamp_column_from_dt_tm


def extract_bkpf(con):
    bkpf = con.prepare_and_execute_query("BKPF", ["BELNR", "BLART", "CPUDT", "CPUTM", "USNAM", "TCODE", "AWKEY"])
    bkpf = bkpf.dropna(subset=["BELNR", "TCODE", "BLART"], how="any")
    transactions = set(bkpf["TCODE"].unique())
    doc_types = set(bkpf["BLART"].unique())
    #changes = extract_change.apply_static(con)
    tcodes = extract_tstct.apply_static(con, transactions=transactions)
    blart = extract_blart.apply_static(con, doc_types=doc_types)
    cols = {x: "event_" + x for x in bkpf.columns}
    bkpf = bkpf.rename(columns=cols)
    bkpf["event_ONLYACT"] = bkpf["event_TCODE"].map(tcodes)
    bkpf["event_BLART"] = bkpf["event_BLART"].map(blart)
    bkpf = timestamp_column_from_dt_tm.apply(bkpf, "event_CPUDT", "event_CPUTM", "event_timestamp")
    bkpf_first = bkpf.groupby("event_BELNR").first().reset_index()
    first_stream = bkpf_first[["event_BELNR", "event_timestamp", "event_BLART"]].to_dict("records")
    doc_first_dates = {x["event_BELNR"]: x["event_timestamp"] for x in first_stream}
    doc_types = {x["event_BELNR"]: x["event_BLART"] for x in first_stream}

    return bkpf, doc_first_dates, doc_types


def extract_bseg(con, doc_first_dates, doc_types):
    bseg = con.prepare_and_execute_query("BSEG", ["BELNR", "GJAHR", "BUZEI", "AUGDT", "AUGBL"])
    bseg = bseg.dropna(subset=["BELNR", "AUGBL"], how="any")
    cols = {x: "event_" + x for x in bseg.columns}
    bseg = bseg.rename(columns=cols)
    bseg["event_timestamp"] = bseg["event_AUGBL"].map(doc_first_dates)
    bseg = bseg.dropna(subset=["event_timestamp"], how="any")
    bseg["event_ONLYACT"] = "Clear Document"
    bseg["event_DOCTYPE"] = bseg["event_BELNR"].map(doc_types)
    return bseg
