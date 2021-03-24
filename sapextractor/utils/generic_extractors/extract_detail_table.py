from sapextractor.utils.table_meaningful_fields import meaningful_fields
import uuid
import pandas as pd
from pm4pymdl.algo.mvp.utils import exploded_mdl_to_succint_mdl


def extract_detail_table(cache, con, tab_name, mandt="800", key_spec=None, min_unq_values=100):
    if key_spec is None:
        key_spec = {}
    primary_keys, foreign_keys, timestamp_resource, fields_with_type, fname_checktable = meaningful_fields.apply_static(con, tab_name)
    fields = [x for x in fields_with_type if x != "event_CUSTOMOBJECTID"]
    fields = [x for x in fields if x in primary_keys or x in foreign_keys]
    fields = [x.split("event_")[1] for x in fields]
    if not fields:
        return pd.DataFrame()
    query = "SELECT "+",".join(fields)+" FROM "+con.table_prefix+tab_name+" WHERE MANDT = '"+mandt+"'"
    for key in key_spec:
        query += " AND " + key + "='" + key_spec[key] + "'"
    try:
        df = con.execute_read_sql(query, fields)
    except:
        return pd.DataFrame()
    allowed_cols = []
    for c in df.columns:
        unq_values = df[c].nunique()
        if unq_values >= min_unq_values:
            allowed_cols.append(c)
    df = df[allowed_cols]
    df.columns = ["event_"+x for x in df.columns]
    prim_keys_in_df = frozenset(x for x in df.columns if x in primary_keys)
    not_prim_keys_in_df = frozenset(x for x in df.columns if x not in prim_keys_in_df)
    list_dfs = []
    for k in not_prim_keys_in_df:
        df2 = df[list(prim_keys_in_df)+[k]].dropna(subset=[k])
        df2 = df2[df2[k] != " "]
        if len(df2) > 0:
            list_dfs.append(df2)
    if list_dfs:
        df = pd.concat(list_dfs)
        if prim_keys_in_df not in cache:
            cache[prim_keys_in_df] = {}
        cache[prim_keys_in_df][tab_name] = (not_prim_keys_in_df, fields_with_type, df)

