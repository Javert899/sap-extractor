def ekko_mkpf_connection(con):
    mseg = con.prepare_and_execute_query("MSEG", ["MLBNR", "EBELN"])
    mseg = mseg.dropna(subset=["MBLNR", "EBELN"], how="any")
    mseg = mseg.to_dict('records')
    mseg = [(x["EBELN"], x["MBLNR"]) for x in mseg]
    return mseg
