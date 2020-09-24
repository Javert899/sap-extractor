from sapextractor.database_connection.interface import DatabaseConnection
import pandas as pd


class OracleConnection(DatabaseConnection):
    def __init__(self, hostname="127.0.0.1", port="1521", sid="xe", username="system", password="oracle"):
        import cx_Oracle
        self.con = cx_Oracle.connect(username, password, hostname+":"+str(port)+"/"+str(sid), encoding="UTF-8", events=True)
        DatabaseConnection.__init__(self)

    def execute_read_sql(self, sql):
        df = pd.read_sql(sql, self.con)
        df.columns = [x.upper() for x in df.columns]
        return df


def apply(hostname="127.0.0.1", port="1521", sid="xe", username="system", password="oracle"):
    return OracleConnection(hostname=hostname, port=port, sid=sid, username=username, password=password)
