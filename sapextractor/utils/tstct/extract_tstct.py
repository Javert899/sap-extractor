def read_tstct(con, target_language="E"):
    df = con.execute_sql("SELECT SPRSL, TCODE, TTEXT FROM TSTCT")
    df = df[df["SPRSL"] == target_language]
    stream = df.to_dict('r')
    dictio = {}
    for el in stream:
        dictio[el["TCODE"]] = el["TTEXT"]
    return dictio
