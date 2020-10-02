from sapextractor import algo, database_connection, utils


def connect_sqlite(path):
    return database_connection.sqlite.apply(path)


def connect_oracle(hostname="127.0.0.1", port="1521", sid="xe", username="system", password="oracle"):
    return database_connection.sqlite.apply(hostname=hostname, port=port, sid=sid, username=username, password=password)


def get_o2c_classic_event_log(con):
    return algo.o2c.o2c_1d_log_extractor.apply(con)


def get_o2c_classic_dataframe(con):
    return algo.o2c.o2c_1d_dataframe_extractor.apply(con)


def get_o2c_mdl_dataframe(con):
    pass
