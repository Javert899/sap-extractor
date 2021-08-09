from sapextractor.utils.dbstattora import extract_count


def expand(con, tab, min_occ=100):
    df = con.execute_read_sql("SELECT CHECKTABLE FROM "+con.table_prefix+"DD03VV WHERE TABNAME = '"+tab+"' AND TABCLASS = 'TRANSP'", ["CHECKTABLE"])
    df = df[df["CHECKTABLE"] != " "]
    set1 = set(df["CHECKTABLE"].unique())
    df = con.execute_read_sql("SELECT TABNAME FROM "+con.table_prefix+"DD03VV WHERE CHECKTABLE = '"+tab+"' AND TABCLASS = 'TRANSP'", ["TABNAME"])
    set2 = set(df["TABNAME"].unique())
    set3 = set1.union(set2)
    counts = extract_count.apply_static(con)
    set3 = {x for x in set3 if x in counts and counts[x] > min_occ}
    return set3


def expand_set(con, tab_set):
    new_set = set(tab_set)
    for tab in tab_set:
        new_set = new_set.union(expand(con, tab))
    return new_set


def extract_expansion_graph(con, tab_set):
    tab_set = list(tab_set)
    query = ["SELECT CHECKTABLE, TABNAME FROM "+con.table_prefix+"DD03VV WHERE ("]
    i = 0
    while i < len(tab_set):
        if i > 0:
            query.append(" OR ")
        query.append("TABNAME = '"+tab_set[i]+"'")
        i = i + 1
    query.append(") AND (")
    i = 0
    while i < len(tab_set):
        if i > 0:
            query.append(" OR ")
        query.append("CHECKTABLE = '"+tab_set[i]+"'")
        i = i + 1
    query.append(")")
    query = "".join(query)
    df = con.execute_read_sql(query, ["TABNAME", "CHECKTABLE"])
    stream = df.to_dict("r")
    edges = list([x["TABNAME"], x["CHECKTABLE"]] for x in stream)
    return edges
