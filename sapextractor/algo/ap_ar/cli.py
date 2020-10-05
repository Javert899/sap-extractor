from sapextractor.algo.ap_ar import single_doc_transactions_dataframe, single_doc_transactions_log
from sapextractor.algo.ap_ar import document_flow_dataframe, document_flow_log
from sapextractor.algo.ap_ar import doc_flow_transactions_dataframe, doc_flow_transactions_log
from sapextractor.algo.ap_ar import obj_centr_log


def cli(con):
    print("\n\nAccounting (AP/AR) extraction\n")
    print("available extraction types:")
    print("1) Transactions for the single document (XES log)")
    print("2) Transactions for the single document (dataframe)")
    print("3) Document Flow (XES log)")
    print("4) Document Flow (dataframe)")
    print("5) Document Flow Transactions (XES log)")
    print("6) Document Flow Transactions (dataframe)")
    print("7) Accounting Object-Centric Event Log")
    print()
    ext_type = input("insert your choice (default: 1):")
    if not ext_type:
        ext_type = "1"
    if ext_type == "1":
        return single_doc_transactions_log.cli(con)
    elif ext_type == "2":
        return single_doc_transactions_dataframe.cli(con)
    elif ext_type == "3":
        return document_flow_log.cli(con)
    elif ext_type == "4":
        return document_flow_dataframe.cli(con)
    elif ext_type == "5":
        return doc_flow_transactions_log.cli(con)
    elif ext_type == "6":
        return doc_flow_transactions_dataframe.cli(con)
    elif ext_type == "7":
        return obj_centr_log.cli(con)
