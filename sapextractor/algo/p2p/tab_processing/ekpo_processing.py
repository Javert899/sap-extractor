def eban_ekko_connection(con, mandt="800", bukrs="1000", extra_els_query=None):
    additional_query_part = " WHERE MANDT ='"+mandt+"' AND BUKRS = '"+bukrs+"'"
    if "EKPO" in extra_els_query:
        additional_query_part += " " + extra_els_query["EKPO"]
    ekpo = con.prepare_and_execute_query("EKPO", ["EBELN", "BANFN"], additional_query_part=additional_query_part)
    ekpo = ekpo.dropna(subset=["EBELN", "BANFN"], how="any")
    ekpo = ekpo.to_dict('records')
    ekpo = [("EBAN_"+x["BANFN"], "EKKO_"+x["EBELN"]) for x in ekpo]
    return ekpo
