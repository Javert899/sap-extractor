import sapextractor
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from examples import example_connection


def execute_script():
    con = example_connection.get_con()
    log = sapextractor.get_ap_ar_single_doc_transactions_log(con)
    xes_exporter.apply(log, "bkpf.xes")


if __name__ == "__main__":
    execute_script()
