from copy import copy


class Shared:
    domnames_dictio = {}


def apply(con):
    df = con.execute_read_sql("SELECT DOMNAME, Count(*) FROM SAPSR3.DD03VV GROUP BY DOMNAME ORDER BY Count(*) DESC", ["DOMNAME", "COUNT"])
    return df


def apply_static(con):
    if not Shared.domnames_dictio:
        Shared.domnames_dictio = apply(con)
    return Shared.domnames_dictio
