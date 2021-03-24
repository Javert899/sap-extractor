from copy import copy


class Shared:
    dictio = {}


def apply(con, tab_name):
    df = con.execute_read_sql(
        "SELECT FIELDNAME, DOMNAME, KEYFLAG, CHECKTABLE FROM "+con.table_prefix+"DD03VV WHERE TABNAME = '"+tab_name+"' ORDER BY POSITION",
        ["FIELDNAME", "DOMNAME", "KEYFLAG", "CHECKTABLE"])


def apply_static(con, tab_name):
    if tab_name not in Shared.dictio:
        Shared.dictio[tab_name] = apply(con, tab_name)
    return copy(Shared.dictio[tab_name])
