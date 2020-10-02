from sapextractor.algo.o2c import o2c_1d_dataframe_extractor
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.util import sorting


def apply(con, ref_type="Order", keep_first=True):
    dataframe = o2c_1d_dataframe_extractor.apply(con, ref_type=ref_type, keep_first=keep_first)
    log = log_converter.apply(dataframe)
    log = sorting.sort_timestamp(log, "time:timestamp")
    return log
