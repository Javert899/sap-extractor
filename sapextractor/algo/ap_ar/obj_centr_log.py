from sapextractor.algo.ap_ar import ap_ar_common
from pm4pymdl.objects.jmd.exporter import exporter as jmd_exporter
from pm4pymdl.objects.mdl.exporter import exporter as mdl_exporter


def apply(con, **ext_arg):
    bkpf = ap_ar_common.get_full_dataframe(con, filter_columns=False)
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
    if path.endswith("mdl"):
        mdl_exporter.apply(dataframe, path)
    elif path.endswith("jmd"):
        jmd_exporter.apply(dataframe, path)

