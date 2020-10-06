from sapextractor.utils.tstct import extract_tstct
from sapextractor.utils.dates import timestamp_column_from_dt_tm


def apply(con):
    mkpf = con.prepare_and_execute_query("MKPF", ["MBLNR", "BLART", "CPUDT", "CPUTM", "USNAM", "TCODE"])
    mkpf.columns = ["event_"+x for x in mkpf.columns]
    mkpf = mkpf.rename(columns={"event_USNAM": "event_USERNAME"})
    mkpf = mkpf.dropna(subset=["event_MBLNR", "event_TCODE"], how="any")
    transactions = set(mkpf["event_TCODE"].unique())
    tcodes = extract_tstct.apply_static(con, transactions=transactions)
    mkpf["event_activity"] = mkpf["event_TCODE"].map(tcodes)
    mkpf = timestamp_column_from_dt_tm.apply(mkpf, "event_CPUDT", "event_CPUTM", "event_timestamp")
    mkpf = mkpf.dropna(subset=["event_timestamp"], how="any")
    mkpf["event_FROMTABLE"] = "MKPF"
    mkpf["event_node"] = "MKPF_" + mkpf["event_MBLNR"]
    mkpf_node_types = {x: "MKPF" for x in mkpf["event_node"].unique()}
    return mkpf, mkpf_node_types
