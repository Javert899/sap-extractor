import sapextractor
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from examples import example_connection


def execute_script():
    con = example_connection.get_con()
    log = sapextractor.get_o2c_classic_event_log(con, ref_type="Invoice", keep_first=True)
    xes_exporter.apply(log, "o2c.xes")


if __name__ == "__main__":
    execute_script()
