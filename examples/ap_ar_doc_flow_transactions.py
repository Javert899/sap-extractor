import sapextractor


def execute_script():
    con = sapextractor.connect_sqlite("../sap.sqlite")
    dataframe = sapextractor.get_ap_ar_doc_flow_transactions_dataframe(con, ref_type="Goods receipt")
    dataframe.to_csv("ap_ar_document_flow_transactions.csv", sep=",", quotechar="\"", index=False)


if __name__ == "__main__":
    execute_script()
