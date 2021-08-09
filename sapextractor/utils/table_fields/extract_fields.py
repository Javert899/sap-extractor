from copy import copy


class Shared:
    dictio = {}


def apply(con, tab_name):
    df = con.execute_read_sql(
        "SELECT FIELDNAME, DOMNAME, KEYFLAG, CHECKTABLE, ROLLNAME FROM "+con.table_prefix+"DD03VV WHERE TABNAME = '"+tab_name+"' ORDER BY POSITION",
        ["FIELDNAME", "DOMNAME", "KEYFLAG", "CHECKTABLE", "ROLLNAME"])
    df = df[df["DOMNAME"] != " "]
    df["FIELDNAME"] = "event_" + df["FIELDNAME"]
    return df


def apply_static(con, tab_name):
    if tab_name not in Shared.dictio:
        Shared.dictio[tab_name] = apply(con, tab_name)
    return copy(Shared.dictio[tab_name])


def classify_table(con, tab_name, all_tables=None):
    if all_tables is None:
        all_tables = dict()
    primary_keys = {}
    for x in all_tables:
        fields2 = apply_static(x, tab_name=tab_name).to_dict("r")
        pk = set()
        for el in fields2:
            if el["KEYFLAG"] == "X":
                pk.add(el["FIELDNAME"])
        primary_keys[x] = pk
    fields = apply_static(con, tab_name=tab_name)
    stream = fields.to_dict("r")
    fields_dict = {x["FIELDNAME"]: x["DOMNAME"] for x in stream}
    for key in fields_dict:
        if "TCODE" in key:
            return "Transaction"
    for key in fields_dict:
        if "VBTYP" in key:
            return "Flow"
    return "Unknown"
