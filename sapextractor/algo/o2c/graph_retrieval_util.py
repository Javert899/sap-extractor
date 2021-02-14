from sapextractor.utils.vbtyp import extract_vbtyp


def extract_dfg(con):
    vbfa = con.execute_read_sql("SELECT VBTYP_V, VBTYP_N, Count(*) FROM "+con.table_prefix+"VBFA GROUP BY VBTYP_V, VBTYP_N", ["VBTYP_V", "VBTYP_N", "COUNT"])
    doc_types = set(vbfa["VBTYP_N"].unique()).union(set(vbfa["VBTYP_V"].unique()))
    vbtyp = extract_vbtyp.apply_static(con, doc_types=doc_types)
    vbfa["VBTYP_V"] = vbfa["VBTYP_V"].map(vbtyp)
    vbfa["VBTYP_N"] = vbfa["VBTYP_N"].map(vbtyp)
    vbfa = vbfa.dropna(subset=["VBTYP_V", "VBTYP_N"], how="any")
    vbfa = vbfa.to_dict("r")
    dfg = {(x["VBTYP_V"], x["VBTYP_N"]): int(x["COUNT"]) for x in vbfa}
    start_activities = con.execute_read_sql("SELECT VBTYP_V, Count(*) FROM (SELECT VBELV, VBTYP_V FROM "+con.table_prefix+"VBFA WHERE VBELV NOT IN (SELECT VBELN FROM "+con.table_prefix+"VBFA)) GROUP BY VBTYP_V", ["ACT", "COUNT"])
    end_activities = con.execute_read_sql("SELECT VBTYP_N, Count(*) FROM (SELECT VBELN, VBTYP_N FROM "+con.table_prefix+"VBFA WHERE VBELN NOT IN (SELECT VBELV FROM "+con.table_prefix+"VBFA)) GROUP BY VBTYP_N", ["ACT", "COUNT"])
    start_activities["ACT"] = start_activities["ACT"].map(vbtyp)
    end_activities["ACT"] = end_activities["ACT"].map(vbtyp)
    start_activities = start_activities.to_dict("r")
    end_activities = end_activities.to_dict("r")
    start_activities = {x["ACT"]: x["COUNT"] for x in start_activities}
    end_activities = {x["ACT"]: x["COUNT"] for x in end_activities}
    act_count_exit = {}
    for el in dfg:
        if not el[0] in act_count_exit:
            act_count_exit[el[0]] = 0
        act_count_exit[el[0]] += int(dfg[el])
    act_count_entry = {}
    for el in dfg:
        if not el[1] in act_count_entry:
            act_count_entry[el[1]] = 0
        act_count_entry[el[1]] += int(dfg[el])
    act_count = {}
    activities = set(act_count_entry).union(set(act_count_exit))
    for act in activities:
        if act in act_count_entry and act in act_count_exit:
            act_count[act] = max(act_count_entry[act], act_count_exit[act])
        elif act in act_count_entry:
            act_count[act] = act_count_entry[act]
        elif act in act_count_exit:
            act_count[act] = act_count_exit[act]
    return dfg, act_count, start_activities, end_activities