from sapextractor.database_connection.interface import DatabaseConnection
import pandas as pd


class OracleConnection(DatabaseConnection):
    def __init__(self, hostname="127.0.0.1", port="1521", sid="xe", username="system", password="oracle"):
        import cx_Oracle
        self.con = cx_Oracle.connect(username, password, hostname + ":" + str(port) + "/" + str(sid), encoding="UTF-8",
                                     events=True)
        DatabaseConnection.__init__(self)

    def execute_read_sql(self, sql):
        df = pd.read_sql(sql, self.con)
        df.columns = [x.upper() for x in df.columns]
        return df

    def get_list_tables(self):
        cursor = self.con.cursor()
        cursor.execute("SELECT table_name FROM dba_tables")
        tables = cursor.fetchall()
        tables = [x[0] for x in tables]
        return tables

    def write_dataframe(self, dataframe, table_name):
        dataframe.to_sql(table_name, con=self.con)

    def get_columns(self, table_name):
        cursor = self.con.cursor()
        cursor.execute("select col.column_name from sys.dba_tab_columns col where col.table_name = '%s'" % table_name)
        columns = cursor.fetchall()
        columns = [x[0] for x in columns]
        return columns


def apply(hostname="127.0.0.1", port="1521", sid="xe", username="system", password="oracle"):
    return OracleConnection(hostname=hostname, port=port, sid=sid, username=username, password=password)
