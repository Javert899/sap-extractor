import pandas as pd
from sapextractor.utils.dates import timestamp_column_from_dt_tm


def goods_receipt(con, gjahr=None, mandt="800", bukrs="1000", extra_els_query=None):
    additional_query_part = " WHERE VGABE = '1' AND MANDT = '"+mandt+"'"
    if gjahr is not None:
        additional_query_part += " AND GJAHR = '"+gjahr+"'"
    if "EKBE" in extra_els_query:
        additional_query_part += " " + extra_els_query["EKBE"]
    ekbe = con.prepare_and_execute_query("EKBE", ["EBELN", "EBELP", "BELNR", "BUZEI", "BUDAT", "GJAHR", "CPUDT", "CPUTM", "ERNAM"], additional_query_part=additional_query_part)
    ekbe_nodes_types = {}
    if len(ekbe) > 0:
        ekbe["OBJECTID"] = ekbe["EBELN"] + ekbe["EBELP"]
        ekbe.columns = ["event_"+x for x in ekbe.columns]
        ekbe = timestamp_column_from_dt_tm.apply(ekbe, "event_CPUDT", "event_CPUTM", "event_timestamp")
        #ekbe["event_CPUDTTM"] = ekbe["event_CPUDT"] + " " + ekbe["event_CPUTM"]
        #ekbe["event_timestamp"] = pd.to_datetime(ekbe["event_CPUDT"].dt.strftime(con.DATE_FORMAT_INTERNAL) + " " + ekbe["event_CPUTM"], errors="coerce", format=con.DATE_FORMAT_INTERNAL + " " + con.HOUR_FORMAT_INTERNAL)
        #ekbe["event_timestamp"] = pd.to_datetime(ekbe["event_BUDAT"], errors="coerce", format=con.DATE_FORMAT)
        ekbe = ekbe.dropna(subset=["event_timestamp"])
        if len(ekbe) > 0:
            ekbe["event_FROMTABLE"] = "EKBE"
            ekbe["event_node"] = "EKBEGR_"+ekbe["event_BELNR"]+ekbe["event_GJAHR"]
            ekbe["event_activity"] = "Goods Receipt"
            ekbe["event_timestamp"] = ekbe["event_timestamp"] + pd.Timedelta("1 second")
            ekbe["event_USERNAME"] = ekbe["event_ERNAM"]
            ekbe_nodes_types = {x: "EKBEGR" for x in ekbe["event_node"].unique()}
    return ekbe, ekbe_nodes_types


def invoice_receipt(con, gjahr=None, mandt="800", bukrs="1000", extra_els_query=None):
    additional_query_part = " WHERE VGABE = '2' AND MANDT ='"+mandt+"'"
    if gjahr is not None:
        additional_query_part += " AND GJAHR = '"+gjahr+"'"
    if "EKBE" in extra_els_query:
        additional_query_part += " " + extra_els_query["EKBE"]
    ekbe = con.prepare_and_execute_query("EKBE", ["EBELN", "EBELP", "BELNR", "BUZEI", "BUDAT", "GJAHR", "CPUDT", "CPUTM", "ERNAM"], additional_query_part=additional_query_part)
    ekbe_nodes_types = {}
    if len(ekbe) > 0:
        ekbe["OBJECTID"] = ekbe["BELNR"] + ekbe["GJAHR"]
        ekbe.columns = ["event_"+x for x in ekbe.columns]
        ekbe = timestamp_column_from_dt_tm.apply(ekbe, "event_CPUDT", "event_CPUTM", "event_timestamp")
        #ekbe["event_timestamp"] = pd.to_datetime(ekbe["event_BUDAT"], errors="coerce", format=con.DATE_FORMAT)
        ekbe = ekbe.dropna(subset=["event_timestamp"])
        if len(ekbe) > 0:
            ekbe["event_FROMTABLE"] = "EKBE"
            ekbe["event_node"] = "EKBEIR_"+ekbe["event_BELNR"]+ekbe["event_GJAHR"]
            ekbe["event_activity"] = "Invoice Receipt"
            ekbe["event_timestamp"] = ekbe["event_timestamp"] + pd.Timedelta("2 seconds")
            ekbe["event_USERNAME"] = ekbe["event_ERNAM"]
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
