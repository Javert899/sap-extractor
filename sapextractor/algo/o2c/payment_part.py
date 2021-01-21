import pandas as pd
from sapextractor.utils.blart import extract_blart


def apply(con, gjahr="2020"):
    bkpf = con.prepare_and_execute_query("BKPF", ["BELNR", "GJAHR", "AWKEY", "BLART"], additional_query_part=" WHERE GJAHR = '"+gjahr+"'")
    blart_vals = set(bkpf["BLART"].unique())
    blart_vals = {x: x for x in blart_vals}
    blart_vals = extract_blart.apply_static(con, doc_types=blart_vals)
    bkpf = bkpf.to_dict("records")
    try:
        bseg = con.prepare_and_execute_query("BSAK", ["BELNR", "BUZEI", "AUGDT", "AUGBL"])
    except:
        bseg = con.prepare_and_execute_query("BSEG", ["BELNR", "BUZEI", "AUGDT", "AUGBL"])
    bseg = bseg.dropna(subset=["AUGBL"])
    try:
        bseg["AUGDT"] = pd.to_datetime(bseg["AUGDT"], format="%d.%m.%Y")
    except:
        bseg["AUGDT"] = pd.to_datetime(bseg["AUGDT"])
    bseg = bseg.to_dict("records")
    dict_awkey = {}
    clearance_docs_dates = {}
    blart_dict = {}
    for el in bkpf:
        awkey = el["AWKEY"]
        belnr = el["BELNR"]
        blart = el["BLART"]
        if awkey is not None:
            if len(awkey) == 18:
                awkey = awkey[4:-4]
                if awkey not in dict_awkey:
                    dict_awkey[awkey] = []
                dict_awkey[awkey].append(belnr)
        if not belnr in blart_dict:
            blart_dict[belnr] = blart
    for el in bseg:
        belnr = el["BELNR"]
        augbl = el["AUGBL"]
        augdt = el["AUGDT"]
        if augbl in blart_dict:
            blart = blart_dict[augbl]
            if not belnr in clearance_docs_dates:
                clearance_docs_dates[belnr] = set()
            clearance_docs_dates[belnr].add((augbl, augdt, blart))

    return dict_awkey, clearance_docs_dates, blart_vals
