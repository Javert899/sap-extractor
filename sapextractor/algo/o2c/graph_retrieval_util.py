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
    act_count = {}
    for el in dfg:
        if not el[0] in act_count:
            act_count[el[0]] = 0
        act_count[el[0]] += int(dfg[el])
    return dfg, act_count
