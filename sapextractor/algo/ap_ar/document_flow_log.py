from sapextractor.algo.ap_ar import document_flow_dataframe
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.util import sorting


def apply(con, ref_type="Goods receipt"):
    dataframe = document_flow_dataframe.apply(con, ref_type=ref_type)
    log = log_converter.apply(dataframe, parameters={"stream_postprocessing": True})
    log = sorting.sort_timestamp(log, "time:timestamp")
    return log
