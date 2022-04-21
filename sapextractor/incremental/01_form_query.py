import os.path
import sys

sys.path.insert(0, "../..")

import json

parameters = json.load(open("parameters.json", "r"))
connection_parameters = json.load(open("connection_parameters.json", "r"))
tables_count = json.load(open("tables_count.json", "r")) if os.path.exists("tables_count.json") else {}


def get_table_count(table):
    if table in tables_count:
        return str(tables_count[table])
    return "0"


def row_number():
    if connection_parameters["connection"] == "oracle":
        return "ROWNUM"
    else:
        return "ROW_NUMBER()"


def form_ekpo_query(ekko_name="ekko", ekpo_name="ekpo"):
    ret = ["SELECT "+ekpo_name+".EKPO_ROW_NUMBER, "+ekpo_name+".MANDT, "+ekpo_name+".EBELN, "+ekpo_name+".EBELP, "+ekpo_name+".EBELNEBELP FROM"]
    ret.append("(SELECT "+row_number()+" AS EKPO_ROW_NUMBER, MANDT, EBELN, EBELP, CONCAT(EBELN, EBELP) AS EBELNEBELP FROM")
    ret.append(parameters["prefix"]+"EKPO")
    ret.append("WHERE "+row_number()+" >= "+get_table_count("EKPO")+") "+ekpo_name+" JOIN (")
    ret.append("SELECT MANDT, EBELN FROM")
    ret.append(parameters["prefix"]+"EKKO")
    ret.append("WHERE "+row_number()+" >= "+get_table_count("EKKO")+") "+ekko_name)
    ret.append("ON "+ekpo_name+".EBELN = "+ekko_name+".EBELN AND "+ekpo_name+".MANDT = "+ekko_name+".MANDT")
    columns = ["EKPO_ROW_NUMBER, MANDT, EBELN, EBELP, EBELNEBELP"]
    return " ".join(ret), columns


def form_eban_query(eban_name="eban"):
    ret = ["SELECT "+row_number()+" AS EBAN_ROW_NUMBER, MANDT, BANFN FROM"]
    ret.append(parameters["prefix"]+"EBAN")
    ret.append("WHERE "+row_number()+" >= "+get_table_count("EBAN"))
    columns = ["EBAN_ROW_NUMBER, MANDT, BANFN"]
    return " ".join(ret), columns


if __name__ == "__main__":
    ekpo_query, ekpo_columns = form_ekpo_query()
    eban_query, eban_columns = form_eban_query()

    from sapextractor.incremental.DB_CONNECTION import get_connection
    c = get_connection()

    c.execute_read_sql(eban_query, eban_columns)
