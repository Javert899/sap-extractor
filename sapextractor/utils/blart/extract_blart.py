from copy import copy


class Shared:
    blart_dictio = {}


def apply(con, target_language="E"):
    df = con.prepare_and_execute_query("T003T", ["SPRAS", "BLART", "LTEXT"])
    df = df[df["SPRAS"] == target_language]
    stream = df.to_dict('records')
    dictio = {}
    for el in stream:
        dictio[el["BLART"]] = el["LTEXT"]
    return dictio


def apply_static(con, doc_types=None):
    if not Shared.blart_dictio:
        Shared.blart_dictio = apply(con)
    ret = copy(Shared.blart_dictio)
    if doc_types is not None:
        doc_types = set(doc_types).difference(set(Shared.blart_dictio.keys()))
        for d in doc_types:
            ret[d] = str(d)
    return ret
