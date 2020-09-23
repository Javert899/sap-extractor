from copy import copy


class Shared:
    vbtyp_dictio = {}


def apply(con, target_language="E"):
    df = con.execute_sql("SELECT DOMNAME, DDLANGUAGE, DDTEXT, DOMVALUE_L FROM DD07T")
    df = df[df["DOMNAME"] == "VBTYP"]
    df = df[df["DDLANGUAGE"] == target_language]
    stream = df.to_dict('r')
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
