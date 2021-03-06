import pandas as pd


def apply(con, mandt="800", bukrs="1000"):
    ekko = con.prepare_and_execute_query("EKKO", ["EBELN", "ERNAM", "AEDAT", "LIFNR", "ZTERM"], additional_query_part=" WHERE MANDT ='"+mandt+"'")
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
    ekko_nodes_types = {x: "EKKO" for x in ekko["event_node"].unique()}
    return ekko, ekko_nodes_types
