from sapextractor.utils.table_meaningful_fields import meaningful_fields
import uuid


def basic_extraction(con, tab_name, mandt="800", key_spec=None):
    if key_spec is None:
        key_spec = {}
    primary_keys, foreign_keys, timestamp_resource, fields_with_type = meaningful_fields.apply_static(con, tab_name)
    fields = [x.split("event_")[1] for x in fields_with_type]
    query = "SELECT "+",".join(fields)+" FROM "+con.table_prefix+tab_name+" WHERE MANDT = '"+mandt+"'"
    for key in primary_keys:
        if key in key_spec:
            query += " AND "+key+"='"+key_spec[key]+"'"
    df = con.execute_read_sql(query, fields)
    df.columns = ["event_" + x for x in df.columns]
    df["event_id"] = df.apply(lambda _: str(uuid.uuid4()), axis=1)
    df["event_CUSTOMOBJECTID"] = ""
    for idx, key in enumerate(primary_keys):
        if idx > 0:
            df["event_CUSTOMOBJECTID"] = "," + df[key]
        else:
            df["event_CUSTOMOBJECTID"] = df[key]
    fields_with_type["event_CUSTOMOBJECTID"] = "AWKEY"
    return df
