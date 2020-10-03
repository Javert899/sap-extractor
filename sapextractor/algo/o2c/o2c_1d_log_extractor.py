from sapextractor.algo.o2c import o2c_1d_dataframe_extractor
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.util import sorting
from pm4py.objects.log.exporter.xes import exporter as xes_exporter


def apply(con, ref_type="Invoice", keep_first=True):
    dataframe = o2c_1d_dataframe_extractor.apply(con, ref_type=ref_type, keep_first=keep_first)
    log = log_converter.apply(dataframe, parameters={"stream_postprocessing": True})
    log = sorting.sort_timestamp(log, "time:timestamp")
    return log


def cli(con):
    print("\n\nO2C XES log extractor\n\n")
    ref_type = input("Insert the central document type of the extraction (default: Invoice): ")
    if not ref_type:
        ref_type = "Invoice"
    ext_type = input("Do you want to extract the document log, or the items log (default: document):")
    if not ext_type:
        ext_type = "document"
    if ext_type == "document":
        keep_first = True
    else:
        keep_first = False
    log = apply(con, ref_type=ref_type, keep_first=keep_first)
    path = input("Insert the path where the log should be saved (default: o2c.xes):")
    if not path:
        path = "o2c.xes"
    xes_exporter.apply(log, path)
