from sapextractor.algo.ap_ar import single_doc_transactions_dataframe, single_doc_transactions_log


def apply(con, ext_type, ext_arg):
    if ext_type == "single_doc_transactions_dataframe":
        return single_doc_transactions_dataframe.apply(con, **ext_arg)
    elif ext_type == "single_doc_transactions_log":
        return single_doc_transactions_log.apply(con, **ext_arg)
