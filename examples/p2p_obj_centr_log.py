import sapextractor
from pm4pymdl.objects.mdl.exporter import exporter as mdl_exporter


def execute_script():
    con = sapextractor.connect_sqlite('../sap.sqlite')
    ol = sapextractor.get_p2p_obj_centr_log(con)
    mdl_exporter.apply(ol, "p2p.mdl")


if __name__ == "__main__":
    execute_script()
