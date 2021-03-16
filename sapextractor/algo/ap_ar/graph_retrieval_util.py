from sapextractor.utils.blart import extract_blart


def extract_dfg_apar(con, mandt="800", bukrs="1000"):
    blart = con.execute_read_sql("SELECT a.BLART, b.BLART, Count(*) FROM (SELECT BELNR, BLART, AUGBL FROM "+con.table_prefix+"BSAK) a JOIN (SELECT BELNR, BLART FROM "+con.table_prefix+"BSAK) b ON a.AUGBL = b.BELNR GROUP BY a.BLART, b.BLART ORDER BY Count(*) DESC", ["BLART", "BLARTC", "COUNT"])
    end_activities = con.execute_read_sql("SELECT BLART, Count(*) FROM "+con.table_prefix+"BKPF WHERE BELNR NOT IN (SELECT BELNR FROM "+con.table_prefix+"BSAK) GROUP BY BLART ORDER BY Count(*) DESC", ["BLART", "COUNT"])
    doc_types = set(blart["BLART"].unique()).union(set(blart["BLARTC"].unique())).union(set(end_activities["BLART"].unique()))
    vbtyp = extract_blart.apply_static(con, doc_types=doc_types)
    blart["BLART"] = blart["BLART"].map(vbtyp)
    blart["BLARTC"] = blart["BLARTC"].map(vbtyp)
    blart = blart.dropna(subset=["BLART", "BLARTC"], how="any")
    blart = blart.to_dict("r")
    dfg = {(x["BLART"], x["BLARTC"]): int(x["COUNT"]) for x in blart}
    start_activities = con.execute_read_sql("SELECT BLART, Count(*) FROM (SELECT BLART FROM "+con.table_prefix+"BSAK WHERE BLART NOT IN (SELECT AUGBL FROM "+con.table_prefix+"BSAK)) GROUP BY BLART ORDER BY Count(*) DESC", ["BLART", "COUNT"])
    start_activities["BLART"] = start_activities["BLART"].map(vbtyp)
    end_activities["BLART"] = end_activities["BLART"].map(vbtyp)
    start_activities = start_activities.to_dict("r")
    end_activities = end_activities.to_dict("r")
    start_activities = {x["BLART"]: x["COUNT"] for x in start_activities}
    end_activities = {x["BLART"]: x["COUNT"] for x in end_activities}
    dfg_act = set(x[0] for x in dfg).union(set(x[1] for x in dfg))
    start_activities = {x: y for x, y in start_activities.items() if x in dfg_act}
    end_activities = {x: y for x, y in end_activities.items() if x in dfg_act}
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
