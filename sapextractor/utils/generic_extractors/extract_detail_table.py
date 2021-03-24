from sapextractor.utils.table_meaningful_fields import meaningful_fields
import uuid
import pandas as pd
from pm4pymdl.algo.mvp.utils import exploded_mdl_to_succint_mdl


def extract_detail_table(con, tab_name, mandt="800", key_spec=None, min_unq_values=10):
    if key_spec is None:
        key_spec = {}
    primary_keys, foreign_keys, timestamp_resource, fields_with_type, fname_checktable = meaningful_fields.apply_static(con, tab_name)
    fields = [x for x in fields_with_type if x != "event_CUSTOMOBJECTID"]
    fields = [x for x in fields if (x in primary_keys and x in foreign_keys and not x in key_spec) or (x in foreign_keys and not x in primary_keys)]
    fields = [x.split("event_")[1] for x in fields]
    query = "SELECT "+",".join(fields)+" FROM "+con.table_prefix+tab_name+" WHERE MANDT = '"+mandt+"'"
    for key in primary_keys:
        if key in key_spec:
            query += " AND "+key+"='"+key_spec[key]+"'"
    df = con.execute_read_sql(query, fields)
    allowed_cols = []
    for c in df.columns:
        unq_values = df[c].nunique()
        if unq_values >= min_unq_values:
            allowed_cols.append(c)
    df = df[allowed_cols]
    return df
