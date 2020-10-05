import pandas as pd


def apply(con):
    ekko = con.prepare_and_execute_query("EKKO", ["EBELN", "ERNAM", "AEDAT", "LIFNR", "ZTERM"])
    ekko.columns = ["event_"+x for x in ekko.columns]
    ekko = ekko.rename(columns={"event_ERNAM": "event_USERNAME", "event_AEDAT": "event_timestamp"})
    ekko["event_timestamp"] = pd.to_datetime(ekko["event_timestamp"])
    ekko["event_activity"] = "Create Purchase Order"
    ekko["event_FROMTABLE"] = "EKKO"
    return ekko
