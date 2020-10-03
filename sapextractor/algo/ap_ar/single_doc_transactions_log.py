from sapextractor.algo.ap_ar import single_doc_transactions_dataframe
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.util import sorting


def apply(con):
    dataframe = single_doc_transactions_dataframe.apply(con)
    log = log_converter.apply(dataframe)
    log = sorting.sort_timestamp(log, "time:timestamp")
    return log


def cli(con):
    print("\n\nAccounting - Transactions for the single document (XES log)\n")
    return apply(con)
