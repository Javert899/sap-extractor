from sapextractor.database_connection.interface import DatabaseConnection
from sapextractor.utils.string_matching import find_corr
import pandas as pd
from getpass import getpass


class OracleConnection(DatabaseConnection):
    def __init__(self, hostname="127.0.0.1", port="1521", sid="xe", username="system", password="oracle"):
        import cx_Oracle
        self.con = cx_Oracle.connect(username, password, hostname + ":" + str(port) + "/" + str(sid), encoding="UTF-8",
                                     events=True)
        DatabaseConnection.__init__(self)

    def execute_read_sql(self, sql):
        cursor = self.con.cursor()
        cursor.execute(sql)
        stream = cursor.fetchall()
        df = pd.DataFrame(stream)
        return df

    def get_list_tables(self):
        cursor = self.con.cursor()
        cursor.execute("SELECT table_name FROM dba_tables")
        tables = cursor.fetchall()
        tables = [x[0] for x in tables]
        return tables

    def write_dataframe(self, dataframe, table_name):
        columns = list(dataframe.columns)
        stream = dataframe.to_dict("r")
        stream2 = []
        while stream:
            event = stream.pop(0)
            new_event = []
            for index, c in enumerate(columns):
                val = event[c]
                new_event.append(val)
            stream2.append(tuple(new_event))
        cursor = self.con.cursor()
        try:
            cursor.execute("DROP TABLE "+table_name)
        except:
            pass
        creation_instructions = ["CREATE TABLE "+table_name+" ("]
        for index, c in enumerate(columns):
            creation_instructions.append(c)
            dtype = str(dataframe[c].dtype)
            if dtype == "object":
                creation_instructions.append(" VARCHAR2(3000)")
            elif dtype == "int64":
                creation_instructions.append(" INTEGER")
            elif dtype == "float64":
                creation_instructions.append(" FLOAT")
            if index < len(columns)-1:
                creation_instructions.append(", ")
        creation_instructions.append(")")
        creation_instructions = "".join(creation_instructions)
        cursor.execute(creation_instructions)
        insertmany_instruction = ["INSERT INTO "+table_name+" VALUES ("]
        for index in range(len(columns)):
            insertmany_instruction.append(":%d" % (index+1))
            if index < len(columns)-1:
                insertmany_instruction.append(", ")
        insertmany_instruction.append(")")
        insertmany_instruction = "".join(insertmany_instruction)
        cursor.executemany(insertmany_instruction, stream2)
        self.con.commit()

    def get_columns(self, table_name):
        cursor = self.con.cursor()
        cursor.execute("select col.column_name from sys.dba_tab_columns col where col.table_name = '%s'" % table_name)
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


def apply(hostname="127.0.0.1", port="1521", sid="xe", username="system", password="oracle"):
    return OracleConnection(hostname=hostname, port=port, sid=sid, username=username, password=password)


def cli():
    print("\n\n")
    print("== Connection to an Oracle database == \n\n")
    hostname = str(input("Insert the hostname (default: 127.0.0.1):"))
    if not hostname:
        hostname = "127.0.0.1"
    port = str(input("Insert the port (default: 1521):"))
    if not port:
        port = "1521"
    sid = str(input("Insert the SID (default: xe):"))
    if not sid:
        sid = "xe"
    username = str(input("Insert the username of a DB user (default: system):"))
    if not username:
        username = "system"
    password = str(getpass("Insert the password of the DB user (default: oracle):"))
    if not password:
        password = "oracle"
    return apply(hostname=hostname, port=port, sid=sid, username=username, password=password)
