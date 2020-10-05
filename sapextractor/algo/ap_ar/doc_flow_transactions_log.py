from sapextractor.algo.ap_ar import doc_flow_transactions_dataframe
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.util import sorting
from pm4py.objects.log.exporter.xes import exporter as xes_exporter


def apply(con, ref_type="Goods receipt"):
    dataframe = doc_flow_transactions_dataframe.apply(con, ref_type=ref_type)
    log = log_converter.apply(dataframe, parameters={"stream_postprocessing": True})
    log = sorting.sort_timestamp(log, "time:timestamp")
    return log


def cli(con):
    print("\n\nAccounting Doc Flow Transactions XES log extractor\n\n")
    ref_type = input("Insert the central document type of the extraction (default: Goods receipt): ")
    if not ref_type:
        ref_type = "Goods receipt"
    log = apply(con, ref_type=ref_type)
    path = input("Insert the path where the log should be saved (default: doc_flow_transactions.xes): ")
    if not path:
        path = "doc_flow_transactions.xes"
    xes_exporter.apply(log, path)
