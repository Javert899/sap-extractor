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


def apply(path):
    return SqliteConnection(path)
