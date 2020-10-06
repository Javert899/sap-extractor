def ekko_mkpf_connection(con):
    mseg = con.prepare_and_execute_query("MSEG", ["MBLNR", "EBELN"])
    mseg = mseg.dropna(subset=["MBLNR", "EBELN"], how="any")
    mseg = mseg.to_dict('records')
    mseg = [("EKKO_"+x["EBELN"], "MKPF_"+x["MBLNR"]) for x in mseg]
    return mseg
