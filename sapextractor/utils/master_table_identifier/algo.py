from sapextractor.utils.table_fields import extract_fields
import pandas as pd
from sapextractor.utils.dbstattora import extract_count

def apply(con, tab_name):
    fields = extract_fields.apply_static(con, tab_name).to_dict("r")
    prim_key_fields = set(x["FIELDNAME"]+"@@@"+x["ROLLNAME"] for x in fields if (x["KEYFLAG"] == "X" and x["FIELDNAME"] != "event_MANDT"))
    coll_df = []
    for k in prim_key_fields:
        query = "SELECT TABNAME, FIELDNAME, ROLLNAME FROM "+con.table_prefix+"DD03VV WHERE TABCLASS = 'TRANSP' AND KEYFLAG = 'X' AND FIELDNAME = '"+k.split("event_")[1].split("@@@")[0]+"'"
        df = con.execute_read_sql(query, ["TABNAME", "FIELDNAME", "ROLLNAME"])
        coll_df.append(df)
    coll_df = pd.concat(coll_df)
    all_sizes = coll_df["TABNAME"].value_counts().to_dict()
    coll_df["NFIELDS"] = coll_df["TABNAME"].map(all_sizes)
    coll_df = coll_df[coll_df["NFIELDS"] == len(prim_key_fields)-1]
    tabcount = extract_count.apply(con)
    coll_df["TABCOUNT"] = coll_df["TABNAME"].map(tabcount)
    coll_df = coll_df.dropna(subset=["TABCOUNT"])
    coll_df = coll_df.sort_values("TABNAME")
    return list(coll_df["TABNAME"].unique())
    #all_sizes = coll_df["TABNAME"].value_counts().to_dict()
    #all_sizes = {x: y for x, y in all_sizes.items() if y == len(prim_key_fields)-1}

    #print(all_sizes)
