import sapextractor
from examples import example_connection


def execute_script():
    con = example_connection.get_con()
    dataframe = sapextractor.get_ap_ar_doc_flow_transactions_dataframe(con, ref_type="Goods receipt")
    dataframe.to_csv("ap_ar_doc_flow_transactions.csv", sep=",", quotechar="\"", index=False)


if __name__ == "__main__":
    execute_script()
