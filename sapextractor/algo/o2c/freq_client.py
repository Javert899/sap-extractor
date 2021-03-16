def apply(con):
    df = con.execute_read_sql("SELECT MANDT, Count(*) FROM "+con.table_prefix+"VBFA GROUP BY MANDT ORDER BY Count(*) DESC", ["CLIENT", "COUNT"])
    df = df.head(100)
    return df.to_html(index=False)
