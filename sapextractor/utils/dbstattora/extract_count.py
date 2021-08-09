class Shared:
    counts = None


def apply(con, min_occ=1000):
    df = con.execute_read_sql("SELECT TNAME, NROWS FROM SAPSR3.DBSTATTORA WHERE NROWS > "+str(min_occ), ["TNAME", "NROWS"])
    stream = df.to_dict("r")
    return {x["TNAME"]: x["NROWS"] for x in stream}


def apply_static(con, min_occ=1000):
    if Shared.counts is None:
        Shared.counts = apply(con, min_occ=min_occ)
    return Shared.counts
