class Shared:
    transactions_dictio = {}


def apply(con, target_language="E"):
    df = con.execute_sql("SELECT SPRSL, TCODE, TTEXT FROM TSTCT")
    df = df[df["SPRSL"] == target_language]
    stream = df.to_dict('r')
    dictio = {}
    for el in stream:
        dictio[el["TCODE"]] = el["TTEXT"]
    return dictio


def apply_static(con):
    if not Shared.transactions_dictio:
        Shared.transactions_dictio = apply(con)
    return Shared.transactions_dictio
