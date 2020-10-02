from copy import copy


class Shared:
    vbtyp_dictio = {}


def apply(con, target_language="E"):
    df = con.prepare_and_execute_query("DD07T", ["DOMNAME", "DDLANGUAGE", "DDTEXT", "DOMVALUE_L"])
    df = df[df["DOMNAME"] == "VBTYP"]
    df = df[df["DDLANGUAGE"] == target_language]
    stream = df.to_dict('records')
    dictio = {}
    for el in stream:
        dictio[el["DOMVALUE_L"]] = el["DDTEXT"]
    return dictio


def apply_static(con, doc_types=None):
    if not Shared.vbtyp_dictio:
        Shared.vbtyp_dictio = apply(con)
    ret = copy(Shared.vbtyp_dictio)
    if doc_types is not None:
        diff = set(doc_types).difference(set(Shared.vbtyp_dictio.keys()))
        for d in diff:
            ret[d] = str(d)
    return ret
