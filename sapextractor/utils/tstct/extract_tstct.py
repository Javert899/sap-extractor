import pandas as pd


def read_tstct(con, target_language="E"):
    df = pd.read_sql("SELECT SPRSL, TCODE, TTEXT FROM TSTCT", con)
    df.columns = [x.upper() for x in df.columns]
    df = df[df["SPRSL"] == target_language]
    stream = df.to_dict('r')
    dictio = {}
    for el in stream:
        dictio[el["TCODE"]] = el["TTEXT"]
    return dictio
