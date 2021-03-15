def eban_ekko_connection(con, mandt="800", bukrs="1000"):
    ekpo = con.prepare_and_execute_query("EKPO", ["EBELN", "BANFN"], additional_query_part=" WHERE MANDT ='"+mandt+"'")
    ekpo = ekpo.dropna(subset=["EBELN", "BANFN"], how="any")
    ekpo = ekpo.to_dict('records')
    ekpo = [("EBAN_"+x["BANFN"], "EKKO_"+x["EBELN"]) for x in ekpo]
    return ekpo
