from sapextractor import algo, database_connection, utils, main


__version__ = '0.0.2'
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


def get_o2c_mdl_dataframe(con):
    pass


def get_ap_ar_single_doc_transactions_dataframe(con):
    return algo.ap_ar.single_doc_transactions_dataframe.apply(con)


def get_ap_ar_single_doc_transactions_log(con):
    return algo.ap_ar.single_doc_transactions_log.apply(con)


def cli():
    return main.main()


def extraction_with_arguments(db_con_arg, ext_type, ext_arg):
    pass
