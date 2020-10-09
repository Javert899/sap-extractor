import sapextractor


def get_con():
    return get_sqlite_con()


def get_sqlite_con():
    con = sapextractor.connect_sqlite('../sap.sqlite')
    return con


def get_oracle_con():
    con = sapextractor.connect_oracle(hostname="127.0.0.1", port="1521", sid="xe", username="system", password="oracle")
    return con
