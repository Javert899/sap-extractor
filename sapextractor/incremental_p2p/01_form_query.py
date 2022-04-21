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
    ret = ["SELECT "+ekpo_name+".EKPO_ROW_NUMBER AS EKPO_ROW_NUMBER, "+ekpo_name+".MANDT AS MANDT, "+ekpo_name+".EBELN AS EBELN, "+ekpo_name+".EBELP AS EBELP, "+ekpo_name+".EBELNEBELP AS EBELNEBELP, "+ekpo_name+".BANFN, "+ekpo_name+".BNFPO, "+ekko_name+".ERNAM AS ERNAM, "+ekko_name+".AEDAT AS AEDAT, "+ekko_name+".LIFNR AS LIFNR, "+ekko_name+".ZTERM AS ZTERM FROM"]
    ret.append("(SELECT "+row_number()+" AS EKPO_ROW_NUMBER, MANDT, EBELN, EBELP, CONCAT(EBELN, EBELP) AS EBELNEBELP, BANFN, BNFPO FROM")
    ret.append(parameters["prefix"]+"EKPO")
    ret.append("WHERE "+row_number()+" >= "+get_table_count("EKPO")+") "+ekpo_name+" JOIN (")
    ret.append("SELECT MANDT, EBELN, ERNAM, AEDAT, LIFNR, ZTERM FROM")
    ret.append(parameters["prefix"]+"EKKO")
    #ret.append("WHERE "+row_number()+" >= "+get_table_count("EKKO"))
    ret.append(") "+ekko_name)
    ret.append("ON "+ekpo_name+".MANDT = "+ekko_name+".MANDT AND "+ekpo_name+".EBELN = "+ekko_name+".EBELN")
    columns = ["EKPO_ROW_NUMBER", "MANDT", "EBELN", "EBELP", "EBELNEBELP", "BANFN", "BNFPO", "ERNAM", "AEDAT", "LIFNR", "ZTERM"]
    return " ".join(ret), columns


def form_eban_query(eban_name="eban"):
    ret = ["SELECT "+row_number()+" AS EBAN_ROW_NUMBER, MANDT, BANFN, BNFPO, CONCAT(BANFN, BNFPO) AS BANFNBNFPO, ERNAM, BADAT FROM"]
    ret.append(parameters["prefix"]+"EBAN")
    ret.append("WHERE "+row_number()+" >= "+get_table_count("EBAN"))
    columns = ["EBAN_ROW_NUMBER", "MANDT", "BANFN", "BNFPO", "BANFNBNFPO", "ERNAM", "BADAT"]
    return " ".join(ret), columns


def form_total_query(ekpo_name="a", eban_name="b"):
    fields = []

    ekpo_query, ekpo_columns = form_ekpo_query()
    eban_query, eban_columns = form_eban_query()

    fields.append(ekpo_name+".EKPO_ROW_NUMBER")
    fields.append(ekpo_name+".MANDT")
    fields.append(ekpo_name+".EBELN")
    fields.append(ekpo_name+".EBELNEBELP")
    fields.append(ekpo_name+".ERNAM")
    fields.append(ekpo_name+".AEDAT")
    fields.append(ekpo_name+".LIFNR")
    fields.append(ekpo_name+".ZTERM")
    fields.append(eban_name+".EBAN_ROW_NUMBER")
    fields.append(eban_name+".BANFN")
    fields.append(eban_name+".BNFPO")
    fields.append(eban_name+".BANFNBNFPO")
    fields.append(eban_name+".ERNAM")
    fields.append(eban_name+".BADAT")

    ret = ["SELECT "]
    ret.append(", ".join(fields))
    ret.append(" FROM (")
    ret.append(ekpo_query)
    ret.append(") "+ekpo_name)
    ret.append("FULL OUTER JOIN (")
    ret.append(eban_query)
    ret.append(") "+eban_name)
    ret.append("ON "+ekpo_name+".MANDT = "+eban_name+".MANDT")
    ret.append("AND "+ekpo_name+".BANFN = "+eban_name+".BANFN")
    ret.append("AND "+ekpo_name+".BNFPO = "+eban_name+".BNFPO")

    return " ".join(ret), fields


if __name__ == "__main__":
    ekpo_query, ekpo_columns = form_ekpo_query()
    eban_query, eban_columns = form_eban_query()

    total_query, total_columns = form_total_query()

    from sapextractor.incremental_p2p.DB_CONNECTION import get_connection
    c = get_connection()

    dataframe = c.execute_read_sql(total_query, total_columns)
    print(dataframe)
    print(dataframe.columns)
