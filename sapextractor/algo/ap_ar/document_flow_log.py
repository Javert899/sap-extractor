from sapextractor.algo.ap_ar import document_flow_dataframe
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.util import sorting
from pm4py.objects.log.exporter.xes import exporter as xes_exporter


def apply(con, ref_type="Goods receipt"):
    dataframe = document_flow_dataframe.apply(con, ref_type=ref_type)
    log = log_converter.apply(dataframe, parameters={"stream_postprocessing": True})
    log = sorting.sort_timestamp(log, "time:timestamp")
    return log


def cli(con):
    print("\n\nAccounting Doc Flow XES log extractor\n\n")
    ref_type = input("Insert the central document type of the extraction (default: Goods receipt): ")
    if not ref_type:
        ref_type = "Goods receipt"
    log = apply(con, ref_type=ref_type)
    path = input("Insert the path where the log should be saved (default: doc_flow.xes): ")
    if not path:
        path = "doc_flow.xes"
    xes_exporter.apply(log, path)
