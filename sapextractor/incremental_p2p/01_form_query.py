import os.path
import sys

sys.path.insert(0, "../..")

import json

parameters = json.load(open("parameters.json", "r"))
connection_parameters = json.load(open("connection_parameters.json", "r"))
tables_count = json.load(open("tables_count.json", "r")) if os.path.exists("tables_count.json") else {}

import sqlparse


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
    return sqlparse.format(" ".join(ret), reindent=True), columns


def form_eban_query(eban_name="eban"):
    ret = ["SELECT "+row_number()+" AS EBAN_ROW_NUMBER, MANDT, BANFN, BNFPO, CONCAT(BANFN, BNFPO) AS BANFNBNFPO, ERNAM, BADAT FROM"]
    ret.append(parameters["prefix"]+"EBAN")
    ret.append("WHERE "+row_number()+" >= "+get_table_count("EBAN"))
    columns = ["EBAN_ROW_NUMBER", "MANDT", "BANFN", "BNFPO", "BANFNBNFPO", "ERNAM", "BADAT"]
    return sqlparse.format(" ".join(ret), reindent=True), columns


def form_rseg_query(rseg_name="rseg", rbkp_name="rbkp"):
    ret = ["SELECT "+rseg_name+".RSEG_ROW_NUMBER AS RSEG_ROW_NUMBER, "+rseg_name+".MANDT AS MANDT, "+rseg_name+".BUKRS AS BUKRS, "+rseg_name+".GJAHR AS GJAHR, "+rseg_name+".BELNR AS BELNR, "+rseg_name+".BUZEI AS BUZEI, "+rseg_name+".EBELN AS EBELN, "+rseg_name+".EBELP AS EBELP, "+rbkp_name+".BLDAT AS BLDAT, "+rbkp_name+".BUDAT AS BUDAT, "+rbkp_name+".USNAM AS USNAM, "+rbkp_name+".TCODE AS TCODE, "+rbkp_name+".LIFNR AS LIFNR"]
    ret.append("FROM (")
    ret.append("SELECT "+row_number()+" AS RSEG_ROW_NUMBER, MANDT, BUKRS, GJAHR, BELNR, BUZEI, EBELN, EBELP FROM")
    ret.append(parameters["prefix"]+"RSEG")
    ret.append("WHERE "+row_number()+" >= "+get_table_count("RSEG"))
    ret.append(") "+rseg_name+" JOIN (")
    ret.append("SELECT MANDT, BUKRS, GJAHR, BELNR, BLDAT, BUDAT, USNAM, TCODE, LIFNR FROM")
    ret.append(parameters["prefix"]+"RBKP")
    ret.append(") "+rbkp_name+" ON")
    ret.append(rseg_name+".MANDT = "+rbkp_name+".MANDT AND")
    ret.append(rseg_name+".BUKRS = "+rbkp_name+".BUKRS AND")
    ret.append(rseg_name+".GJAHR = "+rbkp_name+".GJAHR AND")
    ret.append(rseg_name+".BELNR = "+rbkp_name+".BELNR")
    columns = ["RSEG_ROW_NUMBER", "MANDT", "BUKRS", "GJAHR", "BELNR", "BUZEI", "EBELN", "EBELP", "BLDAT", "BUDAT", "USNAM", "TCODE", "LIFNR"]
    return sqlparse.format(" ".join(ret), reindent=True), columns


def form_total_query(ekpo_name="a", eban_name="b", rseg_name="c"):
    fields = []

    ekpo_query, ekpo_columns = form_ekpo_query()
    eban_query, eban_columns = form_eban_query()
    rseg_query, rseg_columns = form_rseg_query()

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
    fields.append(rseg_name+".RSEG_ROW_NUMBER")
    fields.append(rseg_name+".BUKRS")
    fields.append(rseg_name+".GJAHR")
    fields.append(rseg_name+".BELNR")
    fields.append(rseg_name+".BUZEI")
    fields.append(rseg_name+".BLDAT")
    fields.append(rseg_name+".BUDAT")
    fields.append(rseg_name+".USNAM")
    fields.append(rseg_name+".TCODE")
    fields.append(rseg_name+".LIFNR")

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
    ret.append("FULL OUTER JOIN (")
    ret.append(rseg_query)
    ret.append(") "+rseg_name)
    ret.append("ON "+ekpo_name+".MANDT = "+rseg_name+".MANDT")
    ret.append("AND "+ekpo_name+".EBELN = "+rseg_name+".EBELN")
    ret.append("AND "+ekpo_name+".EBELP = "+rseg_name+".EBELP")

    return sqlparse.format(" ".join(ret), reindent=True), fields


if __name__ == "__main__":
    ekpo_query, ekpo_columns = form_ekpo_query()
    eban_query, eban_columns = form_eban_query()
    rseg_query, rseg_columns = form_rseg_query()

    total_query, total_columns = form_total_query()

    from sapextractor.incremental_p2p.DB_CONNECTION import get_connection
    c = get_connection()

    dataframe = c.execute_read_sql(total_query, total_columns)
    print(dataframe)
    print(dataframe.columns)
    dataframe.to_csv("prova.csv", index=False)
