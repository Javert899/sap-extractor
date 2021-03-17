import pandas as pd


def goods_receipt(con, gjahr=None, mandt="800", bukrs="1000"):
    additional_query_part = " WHERE VGABE = '1' AND MANDT = '"+mandt+"'"
    if gjahr is not None:
        additional_query_part += " AND GJAHR = '"+gjahr+"'"
    ekbe = con.prepare_and_execute_query("EKBE", ["EBELN", "EBELP", "BELNR", "BUZEI", "BUDAT", "GJAHR"], additional_query_part=additional_query_part)
    ekbe["OBJECTID"] = ekbe["EBELN"] + ekbe["EBELP"]
    ekbe.columns = ["event_"+x for x in ekbe.columns]
    ekbe["event_timestamp"] = pd.to_datetime(ekbe["event_BUDAT"], errors="coerce", format=con.DATE_FORMAT)
    ekbe = ekbe.dropna(subset=["event_timestamp"])
    ekbe["event_FROMTABLE"] = "EKBE"
    ekbe["event_node"] = "EKBEGR_"+ekbe["event_BELNR"]+ekbe["event_GJAHR"]
    ekbe["event_activity"] = "Goods Receipt"
    ekbe["event_timestamp"] = ekbe["event_timestamp"] + pd.Timedelta("1 second")
    ekbe_nodes_types = {x: "EKBEGR" for x in ekbe["event_node"].unique()}
    return ekbe, ekbe_nodes_types


def invoice_receipt(con, gjahr=None, mandt="800", bukrs="1000"):
    additional_query_part = " WHERE VGABE = '2' AND MANDT ='"+mandt+"'"
    if gjahr is not None:
        additional_query_part += " AND GJAHR = '"+gjahr+"'"
    ekbe = con.prepare_and_execute_query("EKBE", ["EBELN", "EBELP", "BELNR", "BUZEI", "BUDAT", "GJAHR"], additional_query_part=additional_query_part)
    ekbe["OBJECTID"] = ekbe["BELNR"] + ekbe["GJAHR"]
    ekbe.columns = ["event_"+x for x in ekbe.columns]
    ekbe["event_timestamp"] = pd.to_datetime(ekbe["event_BUDAT"], errors="coerce", format=con.DATE_FORMAT)
    ekbe = ekbe.dropna(subset=["event_timestamp"])
    ekbe["event_FROMTABLE"] = "EKBE"
    ekbe["event_node"] = "EKBEIR_"+ekbe["event_BELNR"]+ekbe["event_GJAHR"]
    ekbe["event_activity"] = "Invoice Receipt"
    ekbe["event_timestamp"] = ekbe["event_timestamp"] + pd.Timedelta("2 seconds")
    ekbe_nodes_types = {x: "EKBEIR" for x in ekbe["event_node"].unique()}
    return ekbe, ekbe_nodes_types


def goods_receipt_ekko_connection(gr):
    gr = gr[["event_EBELN", "event_BELNR", "event_GJAHR"]].to_dict("r")
    cons = list(set(("EKKO_"+x["event_EBELN"], "EKBEGR_"+x["event_BELNR"]+x["event_GJAHR"]) for x in gr))
    return cons


def invoice_receipt_ekko_connection(ir):
    ir = ir[["event_EBELN", "event_BELNR", "event_GJAHR"]].to_dict("r")
    cons = list(set(("EKKO_"+x["event_EBELN"], "EKBEIR_"+x["event_BELNR"]+x["event_GJAHR"]) for x in ir))
    return cons
