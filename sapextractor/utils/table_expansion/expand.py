def expand(con, tab):
    df = con.execute_read_sql("SELECT CHECKTABLE FROM "+con.table_prefix+"DD03VV WHERE TABNAME = '"+tab+"' AND TABCLASS = 'TRANSP'", ["CHECKTABLE"])
    df = df[df["CHECKTABLE"] != " "]
    return set(df["CHECKTABLE"].unique())


def expand_set(con, tab_set):
    new_set = set(tab_set)
    for tab in tab_set:
        new_set = new_set.union(expand(con, tab))
    return new_set
