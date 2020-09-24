from sapextractor.database_connection.interface import DatabaseConnection
import sqlite3
import pandas as pd


class SqliteConnection(DatabaseConnection):
    def __init__(self, path):
        self.path = path
        self.con = sqlite3.connect(self.path)
        DatabaseConnection.__init__(self)

    def execute_read_sql(self, sql):
        df = pd.read_sql(sql, self.con)
        df.columns = [x.upper() for x in df.columns]
        return df

    def get_list_tables(self):
        cursor = self.con.cursor()
        cursor.execute('SELECT name from sqlite_master where type= "table"')
        tables = cursor.fetchall()
        tables = [x[0] for x in tables]
        return tables

    def write_dataframe(self, dataframe, table_name):
        dataframe.to_sql(table_name, con=self.con)

    def get_columns(self, table_name):
        cursor = self.con.cursor()
        cursor.execute("SELECT name FROM PRAGMA_TABLE_INFO('%s')" % table_name)
        columns = cursor.fetchall()
        columns = [x[0] for x in columns]
        return columns


def apply(path):
    return SqliteConnection(path)
