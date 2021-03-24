from copy import copy
from sapextractor.utils.main_domnames import extract_main_domnames
from sapextractor.utils.table_fields import extract_fields
import pandas as pd


class Shared:
    meaningful_dictio = {}


def apply(con, tab_name):
    dom_name = extract_main_domnames.apply_static(con)
    table = extract_fields.apply_static(con, tab_name)
    table = table[table["DOMNAME"].isin(dom_name)]
    table_datumresource = table[table["DOMNAME"].isin(["DATUM", "USNAM"])]
    table_datumresource = table_datumresource.groupby("DOMNAME").first().reset_index()
    table_other = table[~table["DOMNAME"].isin(["DATUM", "USNAM"])]
    table = pd.concat([table_datumresource, table_other])
    primary_keys = [x for x in table[table["KEYFLAG"] == "X"]["FIELDNAME"].unique() if x != "event_MANDT"]
    foreign_keys = [x for x in table[table["CHECKTABLE"] != " "]["FIELDNAME"].unique() if x != "event_MANDT"]
    table_dict = table.to_dict("r")
    fields_with_type = {x["FIELDNAME"]: x["DOMNAME"] for x in table_dict}
    timestamp_resource = table_datumresource.to_dict("r")
    timestamp_resource = {x["DOMNAME"]: x["FIELDNAME"] for x in timestamp_resource}
    fname_checktable = {x["FIELDNAME"]: x["CHECKTABLE"] for x in table_dict}
    if "event_AWKEY" in fields_with_type:
        fields_with_type["event_AWKEY"] = "AWKEY"
    if "event_OBJECTID" in fields_with_type:
        fields_with_type["event_OBJECTID"] = "AWKEY"
    return primary_keys, foreign_keys, timestamp_resource, fields_with_type, fname_checktable


def apply_static(con, tab_name):
    if not tab_name in Shared.meaningful_dictio:
        Shared.meaningful_dictio[tab_name] = apply(con, tab_name)
    return copy(Shared.meaningful_dictio[tab_name])
