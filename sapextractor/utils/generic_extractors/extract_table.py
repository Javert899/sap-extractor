from sapextractor.utils.table_meaningful_fields import meaningful_fields
from sapextractor.utils.generic_extractors import extract_event_table, extract_detail_table
import pandas as pd


def check_is_event(con, tab):
    primary_keys, foreign_keys, timestamp_resource, fields_with_type, fname_checktable = meaningful_fields.apply_static(
        con, tab)
    if len(set(foreign_keys).intersection(set(primary_keys))) > 0:
        return False
    elif "DATUM" not in timestamp_resource or ("USNAM" not in timestamp_resource and "USNAM" not in fields_with_type):
        return False
    return True


def apply(cache, con, tab_name, mandt="800", key_spec=None, min_unq_values=100):
    primary_keys, foreign_keys, timestamp_resource, fields_with_type, fname_checktable = meaningful_fields.apply_static(
        con, tab_name)
    if not check_is_event(con, tab_name):
        # detail table
        extract_detail_table.extract_detail_table(cache, con, tab_name, mandt=mandt, key_spec=key_spec, min_unq_values=min_unq_values)
    else:
        # event table
        df = extract_event_table.apply(cache, con, tab_name, mandt=mandt, key_spec=key_spec, min_unq_values=min_unq_values)
        return df


def apply_set_tables(con, tables, mandt="800", key_spec=None, min_unq_values=100):
    cache = {}
    detail_tables = []
    for tab in tables:
        if not check_is_event(con, tab):
            # detail table
            detail_tables.append(tab)
            apply(cache, con, tab, mandt=mandt, key_spec=key_spec, min_unq_values=min_unq_values)
    # explore other tables
    all_tables = []
    for tab in tables:
        if tab not in detail_tables:
            df = apply(cache, con, tab, mandt=mandt, key_spec=key_spec, min_unq_values=min_unq_values)
            if df is not None and len(df) > 0:
                all_tables.append(df)
    if len(all_tables) > 0:
        df = pd.concat(all_tables)
        df = df.sort_values("event_timestamp")
    else:
        df = pd.DataFrame()
    df.type = "succint"
    return df
