import sapextractor
from pm4py.objects.log.exporter.xes import exporter as xes_exporter


def execute_script():
    con = sapextractor.connect_sqlite("../sap.sqlite")
    log = sapextractor.get_ap_ar_document_flow_log(con, ref_type="Goods receipt")
    xes_exporter.apply(log, "ap_ar_document_flow.xes")


if __name__ == "__main__":
    execute_script()
