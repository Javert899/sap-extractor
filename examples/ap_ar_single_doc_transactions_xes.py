import sapextractor
from pm4py.objects.log.exporter.xes import exporter as xes_exporter


def execute_script():
    con = sapextractor.connect_sqlite("../sap.sqlite")
    log = sapextractor.get_ap_ar_single_doc_transactions_log(con)
    xes_exporter.apply(log, "bkpf.xes")


if __name__ == "__main__":
    execute_script()
