def eban_ekko_connection(con):
    ekpo = con.prepare_and_execute_query("EKPO", ["EBELN", "BANFN"])
    ekpo = ekpo.dropna(subset=["EBELN", "BANFN"], how="any")
    ekpo = ekpo.to_dict("r")
    ekpo = [(x[0], x[1]) for x in ekpo]
    return ekpo

