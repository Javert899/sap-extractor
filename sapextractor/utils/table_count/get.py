def apply(con, table):
    df = con.execute_read_sql("SELECT Count(*) FROM "+con.table_prefix+table, ["COUNT"])
    return int(df.to_dict("r")[0]["COUNT"])
