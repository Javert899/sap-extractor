from sapextractor.algo.ap_ar import ap_ar_common
import pandas as pd
from pm4pymdl.objects.jmd.exporter import exporter as jmd_exporter
from pm4pymdl.objects.mdl.exporter import exporter as mdl_exporter


def apply(con, **ext_arg):
    bkpf, doc_first_dates, doc_types = ap_ar_common.extract_bkpf(con)
    bseg = ap_ar_common.extract_bseg(con, doc_first_dates, doc_types)
    bkpf = pd.concat([bkpf, bseg])
    bkpf["event_activity"] = bkpf["event_ONLYACT"] + " (" + bkpf["event_BLART"] + ")"
    bkpf["event_id"] = bkpf.index.astype(str)
    bkpf = bkpf.sort_values("event_timestamp")
    bkpf.type = "succint"
    return bkpf


def cli(con):
    print("\n\nAccounting Object-Centric Log Extractor\n\n")
    dataframe = apply(con)
    path = input("Insert the path where the log should be saved (default: accounting.mdl): ")
    if not path:
        path = "accounting.mdl"
    mdl_exporter.apply(dataframe, path)
