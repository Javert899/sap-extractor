from sapextractor.algo.ap_ar import single_doc_transactions_dataframe, single_doc_transactions_log


def cli(con):
    print("\n\nAccounting (AP/AR) extraction\n")
    print("available extraction types:")
    print("1) Transactions for the single document (XES log)")
    print("2) Transactions for the single document (dataframe)")
    print()
    ext_type = input("insert your choice (default: 1):")
    if not ext_type:
        ext_type = "1"
    if ext_type == "1":
        return single_doc_transactions_log.cli(con)
    elif ext_type == "2":
        return single_doc_transactions_dataframe.cli(con)
