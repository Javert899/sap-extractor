from sapextractor.utils.table_meaningful_fields import meaningful_fields
import uuid
import pandas as pd
from pm4pymdl.algo.mvp.utils import exploded_mdl_to_succint_mdl


def basic_extraction(con, tab_name, mandt="800", key_spec=None, min_unq_values=10):
    if key_spec is None:
        key_spec = {}
    primary_keys, foreign_keys, timestamp_resource, fields_with_type, fname_checktable = meaningful_fields.apply_static(con, tab_name)
    print(fname_checktable)
    fields = [x for x in fields_with_type if x != "event_CUSTOMOBJECTID"]
    fields = [x for x in fields if x in primary_keys or x in foreign_keys or x in timestamp_resource.values()]
    fields = [x.split("event_")[1] for x in fields]
    query = "SELECT "+",".join(fields)+" FROM "+con.table_prefix+tab_name+" WHERE MANDT = '"+mandt+"'"
    for key in primary_keys:
        if key in key_spec:
            query += " AND "+key+"='"+key_spec[key]+"'"
    df = con.execute_read_sql(query, fields)
    df.columns = ["event_" + x for x in df.columns]
    df["event_id"] = df.apply(lambda _: str(uuid.uuid4()), axis=1)
    df["event_activity"] = "dummy"
    df["event_timestamp"] = df[timestamp_resource["DATUM"]]
    allowed_cols = []
    for c in df.columns:
        unq_values = df[c].nunique()
        if unq_values >= min_unq_values or c == "event_activity" or c == "event_timestamp" or c in primary_keys:
            allowed_cols.append(c)
    df = df[allowed_cols]
    df["event_CUSTOMOBJECTID"] = ""
    for idx, key in enumerate(primary_keys):
        if idx > 0:
            df["event_CUSTOMOBJECTID"] = "," + df[key]
        else:
            df["event_CUSTOMOBJECTID"] = df[key]
    return df


def apply(con, tab_name, mandt="800", key_spec=None, min_unq_values=10):
    primary_keys, foreign_keys, timestamp_resource, fields_with_type, fname_checktable = meaningful_fields.apply_static(con, tab_name)
    fields_with_type["event_CUSTOMOBJECTID"] = "AWKEY"

    inv_fields_with_type = {}
    for x, y in fields_with_type.items():
        if y not in inv_fields_with_type:
            inv_fields_with_type[y] = set()
        inv_fields_with_type[y].add(x)

    df = basic_extraction(con, tab_name, mandt=mandt, key_spec=key_spec, min_unq_values=min_unq_values)
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], format=con.DATE_FORMAT, errors="coerce")
    df = df.dropna(subset=["event_timestamp"])
    keys_to_consider = set(primary_keys)
    keys_to_consider = keys_to_consider.union(foreign_keys)
    keys_to_consider = keys_to_consider.union({"event_CUSTOMOBJECTID"})
    types_to_consider = set(fields_with_type[x] for x in keys_to_consider)
    list_dfs = []
    for t in types_to_consider:
        int_df = []
        for f in inv_fields_with_type[t]:
            if f in df.columns:
                df2 = df.dropna(subset=[f])
                df2[f] = df2[f].astype(str)
                df2 = df2[df2[f] != " "]
                df2[t] = df2[f]
                if len(df2) > 0:
                    int_df.append(df2)
        if int_df:
            df3 = pd.concat(int_df)
            df3 = df3.groupby(t).first().reset_index()
            list_dfs.append(df3)
    df = pd.concat(list_dfs)
    df.type = "exploded"
    df = exploded_mdl_to_succint_mdl.apply(df)
    return df
