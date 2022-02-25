import pandas as pd


def apply(con, mandt="800", bukrs="1000", gjahr=None, extra_els_query=None):
    additional_query_part = " WHERE MANDT ='"+mandt+"'"
    additional_query_part += " AND AEDAT >= '"+con.yyyy_mm_dd(str(gjahr), "01", "01") + "' AND AEDAT <= '"+con.yyyy_mm_dd(str(gjahr), "12", "31")+"' AND EBELN IN (select ebeln from "+con.table_prefix+"ekpo where mandt = '"+mandt+"' and bukrs = '"+bukrs+"')"
    if "EKKO" in extra_els_query:
        additional_query_part += " " + extra_els_query["EKKO"]
    ekko, ekko_query = con.prepare_and_execute_query("EKKO", ["EBELN", "ERNAM", "AEDAT", "LIFNR", "ZTERM"], additional_query_part=additional_query_part, return_query=True)
    ekko_nodes_types = {}
    if len(ekko) > 0:
        ekko["OBJECTID"] = ekko["EBELN"]
        ekko.columns = ["event_"+x for x in ekko.columns]
        ekko = ekko.rename(columns={"event_ERNAM": "event_USERNAME", "event_AEDAT": "event_timestamp"})
        ekko = ekko.sort_values("event_timestamp")
        ekko = ekko.groupby("event_EBELN").first().reset_index()
        ekko["event_timestamp"] = pd.to_datetime(ekko["event_timestamp"], errors="coerce", format=con.DATE_FORMAT)
        ekko["event_activity"] = "Create Purchase Order"
        ekko["event_FROMTABLE"] = "EKKO"
        ekko["event_node"] = "EKKO_"+ekko["event_EBELN"]
        ekko = ekko.dropna(subset=["event_node", "event_timestamp"], how="any")
        if len(ekko) > 0:
            ekko_nodes_types = {x: "EKKO" for x in ekko["event_node"].unique()}
    return ekko, ekko_nodes_types, ekko_query
