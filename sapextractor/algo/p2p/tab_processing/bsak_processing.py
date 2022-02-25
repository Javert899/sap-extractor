import pandas as pd


def apply(con, gjahr=None, mandt="800", bukrs="1000", extra_els_query=None):
    additional_query_part = " WHERE MANDT = '"+mandt+"' AND BUKRS = '"+bukrs+"'"
    if gjahr is not None:
        additional_query_part += " AND GJAHR = '"+gjahr+"' AND AUGGJ = '"+gjahr+"'"
    if "BSAK" in extra_els_query:
        additional_query_part += " " + extra_els_query["BSAK"]
    bsak = con.prepare_and_execute_query("BSAK", ["BELNR", "BUZEI", "GJAHR", "AUGBL", "AUGGJ"], additional_query_part=additional_query_part)
    bsak = bsak.to_dict("r")
    clearances = {}
    for x in bsak:
        key = x["BELNR"]+x["GJAHR"]
        value = x["AUGBL"]+x["AUGGJ"]
        if not key in clearances:
            clearances[key] = list()
        clearances[key].append(value)
    return clearances
