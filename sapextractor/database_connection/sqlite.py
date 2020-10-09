from sapextractor.database_connection.interface import DatabaseConnection
from sapextractor.utils.string_matching import find_corr
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

    def format_table_name(self, table_name):
        return table_name

    def prepare_query(self, table_name, columns):
        table_name = self.format_table_name(table_name)
        table_columns = self.get_columns(table_name)
        columns = find_corr.apply(columns, table_columns)
        return "SELECT "+",".join(columns)+" FROM "+table_name

    def prepare_and_execute_query(self, table_name, columns):
        query = self.prepare_query(table_name, columns)
        dataframe = self.execute_read_sql(query)
        dataframe.columns = columns
        return dataframe


def apply(path='./sap.sqlite'):
    return SqliteConnection(path)


def cli():
    print("\n\n")
    print("== Connection to a SQLite database == \n\n")
    path = input("Insert the path to the SQLite database (default: ./sap.sqlite):")
    if not path:
        path = "./sap.sqlite"
    con = apply(path)
    return con
