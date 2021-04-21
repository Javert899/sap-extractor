def apply(con, tabnames):
    tabnames_where = ["TABNAME = '"+t+"'" for t in tabnames]
    tabnames_where = "WHERE (" + "OR ".join(tabnames_where) + ")"
    tabnames_where += " AND KEYFLAG = 'X'"
    query = "SELECT FIELDNAME FROM "+con.table_prefix+"DD03VV "+tabnames_where
    df = con.execute_read_sql(query, ["FIELDNAME"])
    stream = df.to_dict("r")
    stream = sorted(list(set(x["FIELDNAME"] for x in stream)))
    return stream

