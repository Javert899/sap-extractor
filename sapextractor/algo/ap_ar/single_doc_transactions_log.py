from sapextractor.algo.ap_ar import single_doc_transactions_dataframe
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.util import sorting
from pm4py.objects.log.exporter.xes import exporter as xes_exporter


def apply(con, **ext_arg):
    dataframe = single_doc_transactions_dataframe.apply(con)
    log = log_converter.apply(dataframe, parameters={"stream_postprocessing": True})
    log = sorting.sort_timestamp(log, "time:timestamp")
    return log


def cli(con):
    print("\n\nAccounting - Transactions for the single document (XES log)\n")
    log = apply(con)
    path = input("Insert the path where the log should be saved (default: bkpf.xes):")
    if not path:
        path = "bkpf.xes"
    xes_exporter.apply(log, path)
