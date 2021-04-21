def expand(con, tab):
    df = con.execute_read_sql("SELECT CHECKTABLE FROM "+con.table_prefix+"DD03VV WHERE TABNAME = '"+tab+"' AND TABCLASS = 'TRANSP'", ["CHECKTABLE"])
    df = df[df["CHECKTABLE"] != " "]
    set1 = set(df["CHECKTABLE"].unique())
    df = con.execute_read_sql("SELECT TABNAME FROM "+con.table_prefix+"DD03VV WHERE CHECKTABLE = '"+tab+"' AND TABCLASS = 'TRANSP'", ["TABNAME"])
    set2 = set(df["TABNAME"].unique())
    set3 = set1.union(set2)
    return set3

def expand_set(con, tab_set):
    new_set = set(tab_set)
    for tab in tab_set:
        new_set = new_set.union(expand(con, tab))
    return new_set
