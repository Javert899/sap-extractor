from sapextractor.algo.p2p import p2p_1d_dataframe
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.util import sorting
from pm4py.objects.log.exporter.xes import exporter as xes_exporter


def apply(con, ref_type="EKKO", gjahr="2014", min_extr_date="2014-01-01 00:00:00", mandt="800", bukrs="1000"):
    dataframe = p2p_1d_dataframe.apply(con, gjahr=gjahr, ref_type=ref_type, min_extr_date=min_extr_date, mandt=mandt, bukrs=bukrs)
    log = log_converter.apply(dataframe, parameters={"stream_postprocessing": True})
    log = sorting.sort_timestamp(log, "time:timestamp")
    return log


def cli(con):
    print("\n\nP2P - XES log\n")
    ref_type = input("Provide the central table for the extraction (default: EKKO):")
    if not ref_type:
        ref_type = "EKKO"
    log = apply(con, ref_type=ref_type)
    path = input("Insert the path where the log should be saved (default: p2p.xes): ")
    if not path:
        path = "p2p.xes"
    xes_exporter.apply(log, path)
