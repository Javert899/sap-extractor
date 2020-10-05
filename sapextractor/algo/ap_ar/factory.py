from sapextractor.algo.ap_ar import single_doc_transactions_dataframe, single_doc_transactions_log, document_flow_dataframe, document_flow_log


def apply(con, ext_type, ext_arg):
    if ext_type == "single_doc_transactions_dataframe":
        return single_doc_transactions_dataframe.apply(con, **ext_arg)
    elif ext_type == "single_doc_transactions_log":
        return single_doc_transactions_log.apply(con, **ext_arg)
    elif ext_type == "document_flow_dataframe":
        return document_flow_dataframe.apply(con, **ext_arg)
    elif ext_type == "document_flow_log":
        return document_flow_log.apply(con, **ext_arg)
