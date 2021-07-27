from sapextractor.utils.table_meaningful_fields import meaningful_fields
from sapextractor.utils.generic_extractors import extract_event_table, extract_detail_table
import pandas as pd
from copy import copy


def check_is_event(con, tab, all_primary_keys, all_foreign_keys, all_fields_with_type):
    if tab == "RSEG":
        return False
    this_pk = set(all_primary_keys[tab])
    this_type = all_fields_with_type[tab]
    for tab2 in all_primary_keys:
        if tab2 != tab:
            other_pk = set(all_primary_keys[tab2])
            other_type = all_fields_with_type[tab2]
            if not this_pk.issubset(other_pk) and other_pk.issubset(this_pk):
                is_break = False
                for t in other_pk:
                    if this_type[t] != other_type[t]:
                        is_break = True
                        break
                if not is_break:
                    return False
    return True


def apply(cache, con, tab_name, all_primary_keys, all_foreign_keys, all_fields_with_type, mandt="800", key_spec=None, min_unq_values=100):
    primary_keys, foreign_keys, timestamp_resource, fields_with_type, fname_checktable, fname_rollname = meaningful_fields.apply_static(
        con, tab_name)
    if not check_is_event(con, tab_name, all_primary_keys, all_foreign_keys, all_fields_with_type):
        # detail table
        #print(tab_name+" is detail")
        extract_detail_table.extract_detail_table(cache, con, tab_name, mandt=mandt, key_spec=key_spec, min_unq_values=min_unq_values)
    else:
        # event table
        print(tab_name+" is event")
        df = extract_event_table.apply(cache, con, tab_name, mandt=mandt, key_spec=key_spec, min_unq_values=min_unq_values)
        return df


def apply_set_tables(con, tables, mandt="800", key_spec=None, min_unq_values=100):
    all_primary_keys = {}
    all_foreign_keys = {}
    all_fields_with_type = {}
    global_rollname = {}
    for tab in tables:
        primary_keys, foreign_keys, timestamp_resource, fields_with_type, fname_checktable, fname_rollname = meaningful_fields.apply_static(
            con, tab)
        all_primary_keys[tab] = primary_keys
        all_foreign_keys[tab] = foreign_keys
        all_fields_with_type[tab] = fname_rollname
        for k in fname_rollname:
            global_rollname[k] = fname_rollname[k]
    cache = {}
    detail_tables = []
    for tab in tables:
        if not check_is_event(con, tab, all_primary_keys, all_foreign_keys, all_fields_with_type):
            # detail table
            detail_tables.append(tab)
            apply(cache, con, tab, all_primary_keys, all_foreign_keys, all_fields_with_type, mandt=mandt, key_spec=key_spec, min_unq_values=min_unq_values)
    # explore other tables
    all_tables = []
    for tab in tables:
        if tab not in detail_tables:
            df = apply(cache, con, tab, all_primary_keys, all_foreign_keys, all_fields_with_type, mandt=mandt, key_spec=key_spec, min_unq_values=min_unq_values)
            primary_keys, foreign_keys, timestamp_resource, fields_with_type, fname_checktable, fname_rollname = meaningful_fields.apply_static(
                con, tab)
            fname_rollname = copy(fname_rollname)
            for k in global_rollname:
                if k not in fname_rollname:
                    fname_rollname[k] = global_rollname[k]
            aaa = {x.split("event_")[-1]: x.split("event_")[-1]+"-"+y for x, y in fname_rollname.items()}
            df = df.rename(columns=aaa)
            if df is not None and len(df) > 0:
                all_tables.append(df)
    if len(all_tables) > 0:
        df = pd.concat(all_tables)
        df = df.sort_values("event_timestamp")
    else:
        df = pd.DataFrame()
    df.type = "succint"
    return df
