import pandas as pd


def apply(con, gjahr=None, mandt="800", bukrs="1000"):
    additional_query_part = " WHERE MANDT = '"+mandt+"' AND BUKRS = '"+bukrs+"'"
    if gjahr is not None:
        additional_query_part += " AND GJAHR = '"+gjahr+"'"
    rseg = con.prepare_and_execute_query("RSEG", ["BELNR", "GJAHR", "EBELN"], additional_query_part=additional_query_part)
    rseg = rseg.dropna(subset=["EBELN"])
    rseg = rseg.to_dict("records")
    rseg = list(set(("EKKO_"+x["EBELN"], "RBKP_"+x["BELNR"]+x["GJAHR"]) for x in rseg))
    return rseg
