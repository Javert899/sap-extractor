import pandas as pd


def apply(con, gjahr=None):
    additional_query_part = ""
    if gjahr is not None:
        additional_query_part = " WHERE GJAHR = '"+gjahr+"' AND AUGGJ = '"+gjahr+"'"
    bsak = con.prepare_and_execute_query("BSAK", ["BELNR", "BUZEI", "GJAHR", "AUGBL", "AUGGJ"], additional_query_part=additional_query_part)
    bsak = bsak.to_dict("r")
    clearances = {x["BELNR"]+x["BUZEI"]+x["GJAHR"]: x["AUGBL"]+x["AUGGJ"] for x in bsak}
    return clearances
