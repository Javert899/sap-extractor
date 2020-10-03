from sapextractor.database_connection import factory as conn_factory
from sapextractor.algo import factory as algo_factory


def apply(db_type, db_con_arg, process, ext_type, ext_arg):
    con = conn_factory.apply(db_type, db_con_arg)
    return algo_factory.apply(con, process, ext_type, ext_arg)
