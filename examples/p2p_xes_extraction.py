import sapextractor
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from examples import example_connection


def execute_script():
    con = example_connection.get_con()
    log = sapextractor.get_p2p_classic_event_log(con, ref_type="EKKO")
    xes_exporter.apply(log, "p2p.xes")


if __name__ == "__main__":
    execute_script()
