import sapextractor
from pm4py.objects.log.exporter.xes import exporter as xes_exporter


def execute_script():
    con = sapextractor.connect_sqlite('../sap.sqlite')
    log = sapextractor.get_p2p_classic_event_log(con, ref_type="MKPF")
    xes_exporter.apply(log, "p2p.xes")


if __name__ == "__main__":
    execute_script()
