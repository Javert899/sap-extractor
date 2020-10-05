import sapextractor
from pm4pymdl.objects.mdl.exporter import exporter as mdl_exporter


def execute_script():
    con = sapextractor.connect_sqlite("../sap.sqlite")
    ol = sapextractor.get_o2c_obj_centr_log(con, keep_first=True)
    mdl_exporter.apply(ol, "o2c.mdl")


if __name__ == "__main__":
    execute_script()
