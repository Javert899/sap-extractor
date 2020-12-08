def apply(con, target_language="E"):
    dict_field_desc = {}
    try:
        df = con.prepare_and_execute_query("DD03T", ["DDLANGUAGE", "FIELDNAME", "DDTEXT"], " WHERE DDLANGUAGE = '"+target_language+"'")
        stream = df.to_dict("r")
        for el in stream:
            dict_field_desc[el["FIELDNAME"]] = el["DDTEXT"]
    except:
        pass
    return dict_field_desc
