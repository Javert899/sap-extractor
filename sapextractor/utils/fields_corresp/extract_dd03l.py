def apply(con, target_table, target_language="E"):
    df = con.prepare_and_execute_query("DDL03T", ["SPRAS", "BLART", "LTEXT"], " WHERE SPRAS = '"+target_language+"'")
