def apply(con, tabnames, fname):
    tabnames_where = ["TABNAME = '"+t+"'" for t in tabnames]
    tabnames_where = "WHERE (" + "OR ".join(tabnames_where) + ")"
    tabnames_where += " AND FIELDNAME = '"+fname+"' "
    query = "SELECT TABNAME FROM "+con.table_prefix+"DD03VV "+tabnames_where
    df = con.execute_read_sql(query, ["TABNAME"])
    stream = df.to_dict("r")
    stream = set(x["TABNAME"] for x in stream)
    values = set()
    for table in stream:
        df = con.execute_read_sql("SELECT "+fname+" FROM "+con.table_prefix+table+" GROUP BY "+fname, [fname])
        stream2 = df.to_dict("r")
        stream2 = set(x[fname] for x in stream2)
        values = values.union(stream2)
    values = sorted(list(values))
    return values
