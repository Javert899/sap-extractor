from copy import copy


class Shared:
    transactions_dictio = {}


def apply(con, target_language="E"):
    df = con.execute_read_sql("SELECT SPRSL, TCODE, TTEXT FROM TSTCT")
    df = df[df["SPRSL"] == target_language]
    stream = df.to_dict('r')
    dictio = {}
    for el in stream:
        dictio[el["TCODE"]] = el["TTEXT"]
    return dictio


def apply_static(con, transactions=None):
    if not Shared.transactions_dictio:
        Shared.transactions_dictio = apply(con)
    ret = copy(Shared.transactions_dictio)
    if transactions is not None:
        transactions = set(transactions).difference(set(ret.keys()))
        for t in transactions:
            ret[t] = str(t)
    return ret
