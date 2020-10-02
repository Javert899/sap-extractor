import sapextractor
from pm4py.objects.log.exporter.xes import exporter as xes_exporter


def execute_script():
    con = sapextractor.connect_sqlite("../sap.sqlite")
    log = sapextractor.get_o2c_classic_event_log(con, ref_type="Invoice", keep_first=True)
    xes_exporter.apply(log, "o2c.xes")


if __name__ == "__main__":
    execute_script()
