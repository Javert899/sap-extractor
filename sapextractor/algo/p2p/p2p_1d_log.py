from sapextractor.algo.p2p import p2p_1d_dataframe
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.util import sorting


def apply(con, ref_type="EKKO"):
    dataframe = p2p_1d_dataframe.apply(con, ref_type=ref_type)
    log = log_converter.apply(dataframe, parameters={"stream_postprocessing": True})
    log = sorting.sort_timestamp(log, "time:timestamp")
    return log
