import uuid

import pandas as pd
from pm4pymdl.algo.mvp.utils import exploded_mdl_to_succint_mdl

from sapextractor.utils.table_meaningful_fields import meaningful_fields


def basic_extraction(con, tab_name, mandt="800", key_spec=None, min_unq_values=10):
    if key_spec is None:
        key_spec = {}
    primary_keys, foreign_keys, timestamp_resource, fields_with_type, fname_checktable = meaningful_fields.apply_static(
        con, tab_name)
    fields = [x for x in fields_with_type if x != "event_CUSTOMOBJECTID"]
    fields = [x for x in fields if x in primary_keys or x in foreign_keys or x in timestamp_resource.values()]
    fields = [x.split("event_")[1] for x in fields]
    if not fields:
        return pd.DataFrame()
    query = "SELECT " + ",".join(fields) + " FROM " + con.table_prefix + tab_name + " WHERE MANDT = '" + mandt + "'"
    for key in key_spec:
        query += " AND " + key + "='" + key_spec[key] + "'"
    try:
        df = con.execute_read_sql(query, fields)
    except:
        return pd.DataFrame()
    df.columns = ["event_" + x for x in df.columns]
    df["event_id"] = df.apply(lambda _: str(uuid.uuid4()), axis=1)
    df["event_activity"] = "create document (" + tab_name + ")"
    df["event_timestamp"] = df[timestamp_resource["DATUM"]]
    allowed_cols = []
    for c in df.columns:
        unq_values = df[c].nunique()
        if unq_values >= min_unq_values or c == "event_activity" or c == "event_timestamp" or c in primary_keys or c == "event_TCODE" or c == "event_VBTYP_N" or c == "event_VBTYP_V" or c == "event_VBELN" or c == "event_VBELV" or c == "event_OBJECTID" or c == "event_AWKEY":
            allowed_cols.append(c)
    df = df[allowed_cols]
    df["event_CUSTOMOBJECTID"] = ""
    for idx, key in enumerate(primary_keys):
        if idx > 0:
            df["event_CUSTOMOBJECTID"] = "," + df[key]
        else:
            df["event_CUSTOMOBJECTID"] = df[key]
    return df


def apply(cache, con, tab_name, mandt="800", key_spec=None, min_unq_values=100):
    primary_keys, foreign_keys, timestamp_resource, fields_with_type, fname_checktable = meaningful_fields.apply_static(
        con, tab_name)
    fields_with_type["event_CUSTOMOBJECTID"] = "AWKEY"

    inv_fields_with_type = {}
    for x, y in fields_with_type.items():
        if y not in inv_fields_with_type:
            inv_fields_with_type[y] = set()
        inv_fields_with_type[y].add(x)

    df = basic_extraction(con, tab_name, mandt=mandt, key_spec=key_spec, min_unq_values=min_unq_values)
    if len(df) > 0:
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], format=con.DATE_FORMAT, errors="coerce")
        df = df.dropna(subset=["event_timestamp"])
        keys_to_consider = set(primary_keys)
        keys_to_consider = keys_to_consider.union(foreign_keys)
        keys_to_consider = keys_to_consider.union({"event_CUSTOMOBJECTID"})
        types_to_consider = set(fields_with_type[x] for x in keys_to_consider)
        intermediate_df = {}
        primary_keys = set(primary_keys)
        for el in cache:
            idx_keys = frozenset(primary_keys.intersection(el))
            if len(idx_keys) > 0:
                dff = df.set_index(list(idx_keys))
                for tab_name_2 in cache[el]:
                    not_prim_keys_in_df, join_fields_with_type, df2 = cache[el][tab_name_2]
                    for kk in idx_keys:
                        if join_fields_with_type[kk] != fields_with_type[kk]:
                            break
                    print("linking ", tab_name, " with ", tab_name_2, " on", idx_keys)
                    df2 = df2.set_index(list(idx_keys))
                    df2 = dff.join(df2, rsuffix="_2")
                    df2 = df2.reset_index(drop=True)
                    join_types_to_consider = {}
                    for x in not_prim_keys_in_df:
                        this_type = join_fields_with_type[x]
                        if this_type not in join_types_to_consider:
                            join_types_to_consider[this_type] = set()
                        join_types_to_consider[this_type].add(x)
                    for t in join_types_to_consider:
                        for f in join_types_to_consider[t]:
                            if f in df2.columns:
                                df3 = df2.dropna(subset=[f])
                                df3[f] = df3[f].astype(str)
                                df3 = df3[df3[f] != " "]
                                df3[t] = df3[f]
                                if len(df3) > 0:
                                    if t not in intermediate_df:
                                        intermediate_df[t] = []
                                    intermediate_df[t].append(df3)
        for t in types_to_consider:
            for f in inv_fields_with_type[t]:
                if f in df.columns:
                    df2 = df.dropna(subset=[f])
                    df2[f] = df2[f].astype(str)
                    df2 = df2[df2[f] != " "]
                    df2[t] = df2[f]
                    if len(df2) > 0:
                        if t not in intermediate_df:
                            intermediate_df[t] = []
                        intermediate_df[t].append(df2)
        list_dfs = []
        for t in intermediate_df:
            df5 = pd.concat(intermediate_df[t])
            df5 = df5.groupby(t).first().reset_index()
            list_dfs.append(df5)
        if len(list_dfs) > 0:
            df6 = pd.concat(list_dfs)
            if len(df6) > 0:
                df6.type = "exploded"
                df6 = exploded_mdl_to_succint_mdl.apply(df6)
                return df6
