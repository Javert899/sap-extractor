import pandas as pd


def apply(con, gjahr=None, mandt="800", bukrs="1000"):
    additional_query_part = " WHERE MANDT = '"+mandt+"' AND BUKRS = '"+bukrs+"'"
    if gjahr is not None:
        additional_query_part += " AND GJAHR = '"+gjahr+"'"
    rbkp, rbkp_query = con.prepare_and_execute_query("RBKP", ["BELNR", "GJAHR", "BLDAT", "BUDAT", "USNAM", "TCODE", "LIFNR"], additional_query_part=additional_query_part, return_query=True)
    rbkp["OBJECTID"] = rbkp["BELNR"] + rbkp["GJAHR"]
    rbkp.columns = ["event_"+x for x in rbkp.columns]
    rbkp["event_FROMTABLE"] = "RBKP"
    rbkp["event_node"] = "RBKP_"+rbkp["event_BELNR"]+rbkp["event_GJAHR"]
    rbkp1 = rbkp.copy()
    rbkp1["event_timestamp"] = pd.to_datetime(rbkp1["event_BLDAT"], errors="coerce", format=con.DATE_FORMAT)
    rbkp1["event_activity"] = "Invoice Emitted"
    rbkp1 = rbkp1.dropna(subset=["event_timestamp"])
    rbkp2 = rbkp.copy()
    rbkp2["event_timestamp"] = pd.to_datetime(rbkp2["event_BUDAT"], errors="coerce", format=con.DATE_FORMAT)
    rbkp2["event_activity"] = "Invoice Posted"
    rbkp2 = rbkp2.dropna(subset=["event_timestamp"])
    rbkp2["event_timestamp"] = rbkp2["event_timestamp"] + pd.Timedelta("3 seconds")
    rbkp = pd.concat([rbkp1, rbkp2])
    rbkp_nodes_types = {x: "RBKP" for x in rbkp["event_node"].unique()}
    return rbkp, rbkp_nodes_types, rbkp_query
