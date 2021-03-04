import pandas as pd
from frozendict import frozendict
from sapextractor.algo.p2p.tab_processing import bsak_processing


def apply(con, gjahr=None):
    awkey_docs, doc_dates, doc_types = extract_docs_from_bkpf(con, gjahr=gjahr)
    clearances = bsak_processing.apply(con, gjahr=gjahr)
    events, doc_types, connections = mix_bkpf_bseg(awkey_docs, doc_dates, doc_types, clearances)
    return events, doc_types, connections


def extract_docs_from_bkpf(con, gjahr=None):
    additional_query_part = ""
    if gjahr is not None:
        additional_query_part = " WHERE GJAHR = '"+gjahr+"'"
    bkpf = con.prepare_and_execute_query("BKPF", ["BELNR", "GJAHR", "BLART", "BLDAT", "AWKEY"], additional_query_part=additional_query_part)
    bkpf["BLDAT"] = pd.to_datetime(bkpf["BLDAT"], errors="coerce") + pd.Timedelta("6 seconds")
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
                awkey_docs[key] = list()
            awkey_docs[key].append(el["BELNR"]+el["GJAHR"])
        except:
            pass
    return awkey_docs, doc_dates, doc_types


def mix_bkpf_bseg(awkey_docs, doc_dates, doc_types, clearances):
    events = set()
    nodes_types = {}
    connections = set()

    for inv in awkey_docs:
        for docitem in awkey_docs[inv]:
            if docitem in clearances:
                for clear in clearances[docitem]:
                    if clear in doc_types and clear in doc_dates:
                        doc_type = doc_types[clear]
                        doc_date = doc_dates[clear]
                        events.add(frozendict({"event_node": "BKPF_"+clear, "event_timestamp": doc_date, "event_activity": "Clearance ("+doc_type+")", "event_FROMTABLE": "BKPF"}))
                        nodes_types["BKPF_"+clear] = "BKPF"
                        connections.add(("RBKP_"+inv, "BKPF_"+clear))
    events = list(events)
    for i in range(len(events)):
        events[i] = dict(events[i])
    events = pd.DataFrame(events)

    return events, nodes_types, connections
