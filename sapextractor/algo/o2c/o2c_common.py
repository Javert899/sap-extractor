import pandas as pd
from datetime import datetime


def apply(con):
    vbfa = con.execute_sql("SELECT ERDAT, ERZET, VBELN, VBELV, VBTYP_N, VBTYP_V FROM VBFA")
    vbfa["ERDAT"] = pd.to_datetime(vbfa["ERDAT"]).apply(lambda x: x.timestamp())
    vbfa["ERZET"] = pd.to_datetime(vbfa["ERZET"]).apply(lambda x: x.timestamp())
    vbfa["time:timestamp"] = vbfa["ERDAT"] + vbfa["ERZET"]
    vbfa["time:timestamp"] = vbfa["time:timestamp"].apply(lambda x: datetime.fromtimestamp(x))
    print(vbfa)
