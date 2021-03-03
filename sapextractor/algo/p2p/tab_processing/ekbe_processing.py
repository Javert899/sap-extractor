import pandas as pd


def goods_receipt(con, gjahr=None):
    additional_query_part = " WHERE VGABE = '1'"
    if gjahr is not None:
        additional_query_part += " AND GJAHR = '"+gjahr+"'"
    ekbe = con.prepare_and_execute_query("EKBE", ["EBELN", "EBELP", "BELNR", "BUZEI", "BUDAT", "GJAHR"], additional_query_part=additional_query_part)
    ekbe["event_timestamp"] = pd.to_datetime(ekbe["BUDAT"], errors="coerce")
    ekbe = ekbe.dropna(subset=["event_timestamp"])
    ekbe["event_FROMTABLE"] = "EKBE"
    ekbe["event_node"] = "EKBEGR_"+ekbe["BELNR"]+ekbe["GJAHR"]
    ekbe["event_activity"] = "Goods Receipt"
    ekbe["event_timestamp"] = ekbe["event_timestamp"] + pd.Timedelta("1 second")
    ekbe_nodes_types = {x: "EKBEGR" for x in ekbe["event_node"].unique()}
    return ekbe, ekbe_nodes_types


def invoice_receipt(con, gjahr=None):
    additional_query_part = " WHERE VGABE = '2'"
    if gjahr is not None:
        additional_query_part += " AND GJAHR = '"+gjahr+"'"
    ekbe = con.prepare_and_execute_query("EKBE", ["EBELN", "EBELP", "BELNR", "BUZEI", "BUDAT", "GJAHR"], additional_query_part=additional_query_part)
    ekbe["event_timestamp"] = pd.to_datetime(ekbe["BUDAT"], errors="coerce")
    ekbe = ekbe.dropna(subset=["event_timestamp"])
    ekbe["event_FROMTABLE"] = "EKBE"
    ekbe["event_node"] = "EKBEIR_"+ekbe["BELNR"]+ekbe["GJAHR"]
    ekbe["event_activity"] = "Invoice Receipt"
    ekbe["event_timestamp"] = ekbe["event_timestamp"] + pd.Timedelta("2 seconds")
    ekbe_nodes_types = {x: "EKBEIR" for x in ekbe["event_node"].unique()}
    return ekbe, ekbe_nodes_types


def goods_receipt_ekko_connection(gr):
    gr = gr[["EBELN", "BELNR", "GJAHR"]].to_dict("r")
    cons = list(set(("EKKO_"+x["EBELN"], "EKBEGR_"+x["BELNR"]+x["GJAHR"]) for x in gr))
    return cons


def invoice_receipt_ekko_connection(ir):
    ir = ir[["EBELN", "BELNR", "GJAHR"]].to_dict("r")
    cons = list(set(("EKKO_"+x["EBELN"], "EKBEIR_"+x["BELNR"]+x["GJAHR"]) for x in ir))
    return cons
