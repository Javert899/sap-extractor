from copy import copy


class Shared:
    usr02_dictio_ustyp = {}
    usr02_dictio_class = {}


def apply(con):
    stream = con.execute_read_sql("SELECT BNAME, USTYP, CLASS FROM SAPSR3.USR02", ["BNAME", "USTYP", "CLASS"]).to_dict("records")
    dictio_ustyp = {}
    dictio_class = {}
    for el in stream:
        dictio_ustyp[el["BNAME"]] = el["USTYP"]
        dictio_class[el["BNAME"]] = el["CLASS"]
    return dictio_ustyp, dictio_class


def apply_static(con):
    if not Shared.usr02_dictio_ustyp:
        Shared.usr02_dictio_ustyp, Shared.usr02_dictio_class = apply(con)
    return copy([Shared.usr02_dictio_ustyp, Shared.usr02_dictio_class])
