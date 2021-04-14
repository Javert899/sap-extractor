from sapextractor.database_connection.interface import DatabaseConnection
from sapextractor.utils.string_matching import find_corr
import pandas as pd
from getpass import getpass
from sapextractor.utils import constants
import time


class MicSqlDatabaseConnection(DatabaseConnection):
    def __init__(self, hostname="127.0.0.1", username="sa", password="", database="prova", table_prefix=""):
        import pymssql
        self.TIMESTAMP_FORMAT = "%Y%m%d %H%M%S"
        self.DATE_FORMAT_INTERNAL = "%Y%m%d"
        self.HOUR_FORMAT_INTERNAL = "%H%M%S"
        self.DATE_FORMAT = "%Y-%m-%d"
        constants.TIMESTAMP_FORMAT = self.TIMESTAMP_FORMAT
        constants.DATE_FORMAT_INTERNAL = self.DATE_FORMAT_INTERNAL
        constants.HOUR_FORMAT_INTERNAL = self.HOUR_FORMAT_INTERNAL
        constants.DATE_FORMAT = self.DATE_FORMAT
        self.table_prefix = table_prefix
        self.con = pymssql.connect(hostname, username, password, database)
        DatabaseConnection.__init__(self)

    def execute_read_sql(self, sql, columns):
        cursor = self.con.cursor()
        print(time.time(), "executing: "+sql)
        cursor.execute(sql)
        stream = []
        df = []
        while True:
            res = cursor.fetchmany(10000)
            if len(res) == 0:
                break
            for row in res:
                el = {}
                for idx, col in enumerate(columns):
                    el[col] = row[idx]
                stream.append(el)
            this_dataframe = pd.DataFrame(stream)
            df.append(this_dataframe)
            stream = None
            stream = []
        if df:
            df = pd.concat(df)
        else:
            df = pd.DataFrame({x: [] for x in columns})
        df.columns = [x.upper() for x in df.columns]
        print(time.time(), "executed: "+sql)
        return df

    def get_list_tables(self):
        raise Exception("not implemented")

    def write_dataframe(self, dataframe, table_name):
        raise Exception("not implemented")

    def get_columns(self, table_name):
        raise Exception("not implemented")

    def format_table_name(self, table_name):
        return table_name

    def prepare_query(self, table_name, columns):
        table_name = self.table_prefix + table_name
        return "SELECT " + ",".join(columns) + " FROM " + table_name

    def prepare_and_execute_query(self, table_name, columns, additional_query_part=""):
        query = self.prepare_query(table_name, columns) + additional_query_part
        dataframe = self.execute_read_sql(query, columns)
        dataframe.columns = columns
        return dataframe


def apply(hostname="127.0.0.1", username="sa", password="", database="prova", table_prefix=""):
    return MicSqlDatabaseConnection(hostname=hostname, username=username, password=password, database=database, table_prefix=table_prefix)

