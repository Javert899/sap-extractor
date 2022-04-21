from sapextractor import algo, database_connection, utils, main, diagrams

__version__ = '0.0.22'
__doc__ = "SAP Extractor"
__author__ = 'PADS'
__author_email__ = 'alessandro.berti89@gmail.com'
__maintainer__ = 'PADS'
__maintainer_email__ = 'alessandro.berti89@gmail.com'


def connect_sqlite(path='./sap.sqlite'):
    return database_connection.sqlite.apply(path=path)


def connect_oracle(hostname="127.0.0.1", port="1521", sid="xe", username="system", password="oracle"):
    return database_connection.oracle.apply(hostname=hostname, port=port, sid=sid, username=username, password=password)


def connect_mssql(hostname="127.0.0.1", username="sa", password="", database="prova", table_prefix=""):
    import database_connection.mic_sql
    return database_connection.mic_sql.apply(hostname=hostname, username=username, password=password, database=database, table_prefix=table_prefix)


def get_o2c_classic_event_log(con, ref_type="Invoice", keep_first=True, min_extr_date="2020-01-01 00:00:00", gjahr="2020", enable_changes=True, enable_payments=True, allowed_activities=None, mandt="800"):
    return algo.o2c.o2c_1d_log_extractor.apply(con, ref_type=ref_type, keep_first=keep_first,
                                               min_extr_date=min_extr_date, gjahr=gjahr, enable_changes=enable_changes, enable_payments=enable_payments, allowed_act_doc_types=allowed_activities, mandt=mandt)


def get_o2c_classic_dataframe(con, ref_type="Invoice", keep_first=True, min_extr_date="2020-01-01 00:00:00", gjahr="2020", enable_changes=True, enable_payments=True, allowed_activities=None, mandt="800"):
    return algo.o2c.o2c_1d_dataframe_extractor.apply(con, ref_type=ref_type, keep_first=keep_first,
                                                     min_extr_date=min_extr_date, gjahr=gjahr, enable_changes=enable_changes, enable_payments=enable_payments, allowed_act_doc_types=allowed_activities, mandt=mandt)


def get_o2c_obj_centr_log(con, keep_first=True, min_extr_date="2020-01-01 00:00:00", gjahr="2020", enable_changes=True, enable_payments=True, allowed_activities=None, mandt="800"):
    return algo.o2c.obj_centr_log.apply(con, keep_first=keep_first, min_extr_date=min_extr_date, gjahr=gjahr, enable_changes=enable_changes, enable_payments=enable_payments, allowed_act_doc_types=allowed_activities, mandt=mandt)


def get_p2p_classic_event_log(con, ref_type="EKKO", gjahr="2014", min_extr_date="2014-01-01 00:00:00", mandt="800", bukrs="1000", extra_els_query=None):
    return algo.p2p.p2p_1d_log.apply(con, ref_type=ref_type, gjahr=gjahr, min_extr_date=min_extr_date, mandt=mandt, bukrs=bukrs, extra_els_query=extra_els_query)


def get_p2p_classic_dataframe(con, ref_type="EKKO", gjahr="2014", min_extr_date="2014-01-01 00:00:00", mandt="800", bukrs="1000", extract_changes=True, extra_els_query=None):
    return algo.p2p.p2p_1d_dataframe.apply(con, ref_type=ref_type, gjahr=gjahr, min_extr_date=min_extr_date, mandt=mandt, bukrs=bukrs, extract_changes=extract_changes, extra_els_query=extra_els_query)


def get_p2p_obj_centr_log(con, gjahr="2014", min_extr_date="2014-01-01 00:00:00", mandt="800", bukrs="1000", extra_els_query=None):
    return algo.p2p.obj_centr_log.apply(con, gjahr=gjahr, min_extr_date=min_extr_date, mandt=mandt, bukrs=bukrs, extra_els_query=extra_els_query)


def get_ap_ar_single_doc_transactions_dataframe(con, gjahr="1997", mandt="800", bukrs="1000"):
    return algo.ap_ar.single_doc_transactions_dataframe.apply(con, gjahr=gjahr, bukrs=bukrs, mandt=mandt)


def get_ap_ar_single_doc_transactions_log(con, gjahr="1997", mandt="800", bukrs="1000"):
    return algo.ap_ar.single_doc_transactions_log.apply(con, gjahr=gjahr, bukrs=bukrs, mandt=mandt)


def get_ap_ar_document_flow_dataframe(con, ref_type="Goods receipt", gjahr="1997", mandt="800", bukrs="1000"):
    return algo.ap_ar.document_flow_dataframe.apply(con, ref_type=ref_type, gjahr=gjahr, bukrs=bukrs, mandt=mandt)


def get_ap_ar_document_flow_log(con, ref_type="Goods receipt", gjahr="1997", mandt="800", bukrs="1000"):
    return algo.ap_ar.document_flow_log.apply(con, ref_type=ref_type, gjahr=gjahr, bukrs=bukrs, mandt=mandt)


def get_ap_ar_doc_flow_transactions_dataframe(con, ref_type="Goods receipt", gjahr="1997", mandt="800", bukrs="1000"):
    return algo.ap_ar.doc_flow_transactions_dataframe.apply(con, ref_type=ref_type, gjahr=gjahr, bukrs=bukrs, mandt=mandt)


def get_ap_ar_doc_flow_transactions_log(con, ref_type="Goods receipt", gjahr="1997", mandt="800", bukrs="1000"):
    return algo.ap_ar.doc_flow_transactions_log.apply(con, ref_type=ref_type, gjahr=gjahr, bukrs=bukrs, mandt=mandt)


def get_ap_ar_obj_centr_log(con, gjahr="1997", bukrs=None):
    return algo.ap_ar.obj_centr_log.apply(con, gjahr=gjahr, bukrs=bukrs)


def cli():
    return main.main()
