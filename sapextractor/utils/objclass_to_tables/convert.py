def apply(con, obj_class, mandt="800"):
    df = con.execute_read_sql("SELECT TABNAME FROM "+con.table_prefix+"CDPOS WHERE MANDANT = '"+mandt+"' AND OBJECTCLAS = '"+obj_class+"'", ["TABNAME"])
    return set(df["TABNAME"].unique())
