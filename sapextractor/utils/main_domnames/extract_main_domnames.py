from copy import copy
import pandas as pd

class Shared:
    domnames_dictio = {}


def apply(con, min_count=1000):
    df = con.execute_read_sql("SELECT DOMNAME, Count(*) FROM "+con.table_prefix+"DD03VV where tabname IN(Select Tabname from SAPSR3.DD02V A join SAPSR3.DBSTATTORA B on A.tabname = B.tname where tabclass='TRANSP' and ddlanguage = 'E' and contflag = 'A' and NROWS > 0) GROUP BY DOMNAME ORDER BY Count(*) DESC", ["DOMNAME", "COUNT"])
    df = df[df["DOMNAME"] != " "]
    return set(df["DOMNAME"].unique())



def apply_static(con, min_count=1000):
    if not Shared.domnames_dictio:
        Shared.domnames_dictio = apply(con, min_count=min_count)
    return copy(Shared.domnames_dictio)
