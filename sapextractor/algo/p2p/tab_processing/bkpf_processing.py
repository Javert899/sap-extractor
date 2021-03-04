import pandas as pd


def apply(con, gjahr="2009"):
    additional_query_part = ""
    if gjahr is not None:
        additional_query_part = " WHERE GJAHR = '"+gjahr+"'"
    bkpf = con.prepare_and_execute_query("BKPF", ["BELNR", "BUZEI", "GJAHR", "BLART", "BLDAT", "AWKEY"], additional_query_part=additional_query_part)
    bkpf["BLDAT"] = pd.to_datetime(bkpf["BLDAT"], errors="coerce")
    bkpf = bkpf.dropna(subset=["BLDAT"])
    bkpf = bkpf.to_dict("r")
    awkey_docs = {}
    doc_dates = {}
    doc_types = {}
    for el in bkpf:
        doc_dates[el["BELNR"] + el["GJAHR"]] = el["BLDAT"]
        doc_types[el["BELNR"] + el["GJAHR"]] = el["BLART"]
        try:
            year = el["AWKEY"][-4:]
            docnum = el["AWKEY"][:10]
            key = docnum+year
            if key not in awkey_docs:
                awkey_docs[key] = set()
            awkey_docs[key].add(el["BELNR"]+el["BUZEI"]+el["GJAHR"])
        except:
            pass
    return awkey_docs, doc_dates, doc_types
