def apply(con, mandt="800"):
    df = con.execute_read_sql("SELECT OBJECTCLAS, Count(*) FROM "+con.table_prefix+"CDHDR WHERE MANDANT = '"+mandt+"' GROUP BY OBJECTCLAS ORDER BY Count(*) DESC", ["OBJECTCLAS", "COUNT"])
    df = df.to_dict("r")
    df = {str(x["OBJECTCLAS"]): int(x["COUNT"]) for x in df}
    return df
