from sapextractor.algo.ap_ar import single_doc_transactions_dataframe, single_doc_transactions_log
from sapextractor.algo.ap_ar import document_flow_dataframe, document_flow_log
from sapextractor.algo.ap_ar import doc_flow_transactions_dataframe, doc_flow_transactions_log
from sapextractor.algo.ap_ar import obj_centr_log


def apply(con, ext_type, ext_arg):
    if ext_type == "single_doc_transactions_dataframe":
        return single_doc_transactions_dataframe.apply(con, **ext_arg)
    elif ext_type == "single_doc_transactions_log":
        return single_doc_transactions_log.apply(con, **ext_arg)
    elif ext_type == "document_flow_dataframe":
        return document_flow_dataframe.apply(con, **ext_arg)
    elif ext_type == "document_flow_log":
        return document_flow_log.apply(con, **ext_arg)
    elif ext_type == "doc_flow_transactions_dataframe":
        return doc_flow_transactions_dataframe.apply(con, **ext_arg)
    elif ext_type == "doc_flow_transactions_xes":
        return doc_flow_transactions_log.apply(con, **ext_arg)
    elif ext_type == "apar_obj_centr_log":
        return obj_centr_log.apply(con, **ext_arg)
