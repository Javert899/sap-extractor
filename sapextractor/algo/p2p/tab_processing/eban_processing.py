import pandas as pd


def apply(con):
    eban = con.prepare_and_execute_query("EBAN", ["BANFN", "ERNAM", "ERDAT"])
    eban.columns = ["event_"+x for x in eban.columns]
    eban = eban.rename(columns={"event_ERNAM": "event_USERNAME", "event_ERDAT": "event_timestamp"})
    eban = eban.sort_values("event_timestamp")
    eban = eban.groupby("event_BANFN").first().reset_index()
    eban["event_timestamp"] = pd.to_datetime(eban["event_timestamp"])
    eban["event_activity"] = "Create Purchase Requisition"
    eban["event_FROMTABLE"] = "EBAN"
    eban["event_node"] = "EBAN_"+eban["event_BANFN"]
    eban = eban.dropna(subset=["event_node"])
    eban_nodes_types = {x: "EBAN" for x in eban["event_node"].unique()}
    return eban, eban_nodes_types
