from sapextractor import algo, database_connection, utils, main


__version__ = '0.0.7'
__doc__ = "SAP Extractor"
__author__ = 'PADS'
__author_email__ = 'alessandro.berti89@gmail.com'
__maintainer__ = 'PADS'
__maintainer_email__ = 'alessandro.berti89@gmail.com'


def connect_sqlite(path='./sap.sqlite'):
    return database_connection.sqlite.apply(path=path)


def connect_oracle(hostname="127.0.0.1", port="1521", sid="xe", username="system", password="oracle"):
    return database_connection.oracle.apply(hostname=hostname, port=port, sid=sid, username=username, password=password)


def get_o2c_classic_event_log(con, ref_type="Invoice", keep_first=True):
    return algo.o2c.o2c_1d_log_extractor.apply(con, ref_type=ref_type, keep_first=keep_first)


def get_o2c_classic_dataframe(con, ref_type="Invoice", keep_first=True):
    return algo.o2c.o2c_1d_dataframe_extractor.apply(con, ref_type=ref_type, keep_first=keep_first)


def get_o2c_obj_centr_log(con, keep_first=True):
    return algo.o2c.obj_centr_log.apply(con, keep_first=keep_first)


def get_p2p_classic_event_log(con, ref_type="EKKO"):
    return algo.p2p.p2p_1d_log.apply(con, ref_type=ref_type)


def get_p2p_classic_dataframe(con, ref_type="EKKO"):
    return algo.p2p.p2p_1d_dataframe.apply(con, ref_type=ref_type)


def get_p2p_obj_centr_log(con):
    return algo.p2p.obj_centr_log.apply(con)


def get_ap_ar_single_doc_transactions_dataframe(con):
    return algo.ap_ar.single_doc_transactions_dataframe.apply(con)


def get_ap_ar_single_doc_transactions_log(con):
    return algo.ap_ar.single_doc_transactions_log.apply(con)


def get_ap_ar_document_flow_dataframe(con, ref_type="Goods receipt"):
    return algo.ap_ar.document_flow_dataframe.apply(con, ref_type=ref_type)


def get_ap_ar_document_flow_log(con, ref_type="Goods receipt"):
    return algo.ap_ar.document_flow_log.apply(con, ref_type=ref_type)


def get_ap_ar_doc_flow_transactions_dataframe(con, ref_type="Goods receipt"):
    return algo.ap_ar.doc_flow_transactions_dataframe.apply(con, ref_type=ref_type)


def get_ap_ar_doc_flow_transactions_log(con, ref_type="Goods receipt"):
    return algo.ap_ar.doc_flow_transactions_log.apply(con, ref_type=ref_type)


def get_ap_ar_obj_centr_log(con):
    return algo.ap_ar.obj_centr_log.apply(con)


def cli():
    return main.main()
