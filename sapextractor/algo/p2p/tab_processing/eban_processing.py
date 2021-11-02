import pandas as pd


def apply(con, mandt="800", bukrs="1000", gjahr=None):
    additional_query_part = " WHERE MANDT = '"+mandt+"'"
    additional_query_part += " AND BADAT >= '"+con.yyyy_mm_dd(str(gjahr), "01", "01") + "' AND BADAT <= '"+con.yyyy_mm_dd(str(gjahr), "12", "31")+"'"
    eban = con.prepare_and_execute_query("EBAN", ["BANFN", "ERNAM", "BADAT"], additional_query_part=additional_query_part)
    eban["OBJECTID"] = eban["BANFN"]
    eban.columns = ["event_"+x for x in eban.columns]
    eban = eban.rename(columns={"event_ERNAM": "event_USERNAME", "event_BADAT": "event_timestamp"})
    eban = eban.sort_values("event_timestamp")
    eban = eban.groupby("event_BANFN").first().reset_index()
    eban["event_timestamp"] = pd.to_datetime(eban["event_timestamp"], format=con.DATE_FORMAT)
    eban["event_activity"] = "Create Purchase Requisition"
    eban["event_FROMTABLE"] = "EBAN"
    eban["event_node"] = "EBAN_"+eban["event_BANFN"]
    eban = eban.dropna(subset=["event_node"])
    eban_nodes_types = {x: "EBAN" for x in eban["event_node"].unique()}
    return eban, eban_nodes_types
