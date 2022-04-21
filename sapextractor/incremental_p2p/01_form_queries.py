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


def form_ekpo_query(ekko_name="ekko", ekpo_name="ekpo", apply_rownum=True):
    ret = ["SELECT "+ekpo_name+".EKPO_ROW_NUMBER AS EKPO_ROW_NUMBER, "+ekpo_name+".MANDT AS MANDT, "+ekpo_name+".EBELN AS EBELN, "+ekpo_name+".EBELP AS EBELP, "+ekpo_name+".EBELNEBELP AS EBELNEBELP, "+ekpo_name+".BANFN, "+ekpo_name+".BNFPO, "+ekko_name+".ERNAM AS ERNAM, "+ekko_name+".AEDAT AS AEDAT, "+ekko_name+".LIFNR AS LIFNR, "+ekko_name+".ZTERM AS ZTERM FROM"]
    ret.append("(SELECT "+row_number()+" AS EKPO_ROW_NUMBER, MANDT, EBELN, EBELP, CONCAT(EBELN, EBELP) AS EBELNEBELP, BANFN, BNFPO FROM")
    ret.append(parameters["prefix"]+"EKPO")
    if apply_rownum:
        ret.append("WHERE "+row_number()+" >= "+get_table_count("EKPO")+")")
    ret.append(ekpo_name+" JOIN (")
    ret.append("SELECT MANDT, EBELN, ERNAM, AEDAT, LIFNR, ZTERM FROM")
    ret.append(parameters["prefix"]+"EKKO")
    #ret.append("WHERE "+row_number()+" >= "+get_table_count("EKKO"))
    ret.append(") "+ekko_name)
    ret.append("ON "+ekpo_name+".MANDT = "+ekko_name+".MANDT AND "+ekpo_name+".EBELN = "+ekko_name+".EBELN")
    columns = ["EKPO_ROW_NUMBER", "MANDT", "EBELN", "EBELP", "EBELNEBELP", "BANFN", "BNFPO", "ERNAM", "AEDAT", "LIFNR", "ZTERM"]
    return sqlparse.format(" ".join(ret), reindent=True), columns


def form_eban_query(eban_name="eban", apply_rownum=True):
    ret = ["SELECT "+row_number()+" AS EBAN_ROW_NUMBER, MANDT, BANFN, BNFPO, CONCAT(BANFN, BNFPO) AS BANFNBNFPO, ERNAM, BADAT FROM"]
    if apply_rownum:
        ret.append(parameters["prefix"]+"EBAN")
    ret.append("WHERE "+row_number()+" >= "+get_table_count("EBAN"))
    columns = ["EBAN_ROW_NUMBER", "MANDT", "BANFN", "BNFPO", "BANFNBNFPO", "ERNAM", "BADAT"]
    return sqlparse.format(" ".join(ret), reindent=True), columns


def form_rseg_query(rseg_name="rseg", rbkp_name="rbkp", apply_rownum=True):
    ret = ["SELECT "+rseg_name+".RSEG_ROW_NUMBER AS RSEG_ROW_NUMBER, "+rseg_name+".MANDT AS MANDT, "+rseg_name+".BUKRS AS BUKRS, "+rseg_name+".GJAHR AS GJAHR, "+rseg_name+".BELNR AS BELNR, "+rseg_name+".BUZEI AS BUZEI, "+rseg_name+".EBELN AS EBELN, "+rseg_name+".EBELNEBELP AS EBELNEBELP, "+rbkp_name+".BLDAT AS BLDAT, "+rbkp_name+".BUDAT AS BUDAT, "+rbkp_name+".USNAM AS USNAM, "+rbkp_name+".TCODE AS TCODE, "+rbkp_name+".LIFNR AS LIFNR"]
    ret.append("FROM (")
    ret.append("SELECT "+row_number()+" AS RSEG_ROW_NUMBER, MANDT, BUKRS, GJAHR, BELNR, BUZEI, EBELN, CONCAT(EBELN, EBELP) AS EBELNEBELP FROM")
    ret.append(parameters["prefix"]+"RSEG")
    if apply_rownum:
        ret.append("WHERE "+row_number()+" >= "+get_table_count("RSEG"))
    ret.append(") "+rseg_name+" JOIN (")
    ret.append("SELECT MANDT, BUKRS, GJAHR, BELNR, BLDAT, BUDAT, USNAM, TCODE, LIFNR FROM")
    ret.append(parameters["prefix"]+"RBKP")
    ret.append(") "+rbkp_name+" ON")
    ret.append(rseg_name+".MANDT = "+rbkp_name+".MANDT AND")
    ret.append(rseg_name+".BUKRS = "+rbkp_name+".BUKRS AND")
    ret.append(rseg_name+".GJAHR = "+rbkp_name+".GJAHR AND")
    ret.append(rseg_name+".BELNR = "+rbkp_name+".BELNR")
    columns = ["RSEG_ROW_NUMBER", "MANDT", "BUKRS", "GJAHR", "BELNR", "BUZEI", "EBELN", "EBELNEBELP", "BLDAT", "BUDAT", "USNAM", "TCODE", "LIFNR"]
    return sqlparse.format(" ".join(ret), reindent=True), columns


def final_query_purchase_requisitions(eban_name="a", ekpo_name="b"):
    fields = []

    eban_query, eban_columns = form_eban_query()

    fields.append(eban_name+".EBAN_ROW_NUMBER")
    fields.append(eban_name+".MANDT")
    fields.append(eban_name+".BANFN")
    fields.append(eban_name+".BNFPO")
    fields.append(eban_name+".BANFNBNFPO")
    fields.append(eban_name+".ERNAM")
    fields.append(eban_name+".BADAT")
    fields.append(ekpo_name+".EBELN")
    fields.append(ekpo_name+".EBELNEBELP")

    ret = ["SELECT "]
    ret.append(", ".join(fields))
    ret.append(" FROM (")
    ret.append(eban_query)
    ret.append(") "+eban_name+" LEFT JOIN (")
    ret.append("SELECT MANDT, EBELN, CONCAT(EBELN, EBELP) AS EBELNEBELP, BANFN, BNFPO FROM "+parameters["prefix"]+"EKPO")
    ret.append(") "+ekpo_name)
    ret.append("ON "+eban_name+".MANDT = "+ekpo_name+".MANDT")
    ret.append("AND "+eban_name+".BANFN = "+ekpo_name+".BANFN")
    ret.append("AND "+eban_name+".BNFPO = "+ekpo_name+".BNFPO")

    return sqlparse.format(" ".join(ret), reindent=True), fields


def final_query_invoice_processing(rseg_name="a", ekpo_name="b"):
    fields = []

    rseg_query, rseg_columns = form_rseg_query()

    fields.append(rseg_name+".RSEG_ROW_NUMBER")
    fields.append(rseg_name+".MANDT")
    fields.append(rseg_name+".BUKRS")
    fields.append(rseg_name+".GJAHR")
    fields.append(rseg_name+".BELNR")
    fields.append(rseg_name+".BUZEI")
    fields.append(rseg_name+".BLDAT")
    fields.append(rseg_name+".BUDAT")
    fields.append(rseg_name+".USNAM")
    fields.append(rseg_name+".TCODE")
    fields.append(rseg_name+".LIFNR")
    fields.append(ekpo_name+".EBELN")
    fields.append(ekpo_name+".EBELNEBELP")

    ret = ["SELECT "]
    ret.append(", ".join(fields))
    ret.append(" FROM (")
    ret.append(rseg_query)
    ret.append(") "+rseg_name+" LEFT JOIN (")
    ret.append("SELECT MANDT, EBELN, CONCAT(EBELN, EBELP) AS EBELNEBELP FROM "+parameters["prefix"]+"EKPO")
    ret.append(") "+ekpo_name)
    ret.append("ON "+rseg_name+".MANDT = "+ekpo_name+".MANDT")
    ret.append("AND "+rseg_name+".EBELN = "+ekpo_name+".EBELN")
    ret.append("AND "+rseg_name+".EBELNEBELP = "+ekpo_name+".EBELNEBELP")

    return sqlparse.format(" ".join(ret), reindent=True), fields


def write_result(name, query, columns):
    F = open("query_content_" + name + ".txt", "w")
    F.write(query)
    F.close()

    F = open("query_columns_" + name + ".txt", "w")
    F.write(" ".join(columns))
    F.close()


if __name__ == "__main__":
    ekpo_query, ekpo_columns = form_ekpo_query()
    eban_query, eban_columns = form_eban_query()
    rseg_query, rseg_columns = form_rseg_query()

    pr_query, pr_columns = final_query_purchase_requisitions()
    write_result("pr", pr_query, pr_columns)

    inv_query, inv_columns = final_query_invoice_processing()
    write_result("invp", inv_query, inv_columns)

    """from sapextractor.incremental_p2p.DB_CONNECTION import get_connection
    c = get_connection()

    dataframe = c.execute_read_sql(pr_query, pr_columns)
    print(dataframe)
    print(dataframe.columns)
    dataframe.to_csv("prova.csv", index=False)"""

    """total_query, total_columns = form_total_general_query()

    F = open("query_content_general.txt", "w")
    F.write(total_query)
    F.close()
    F = open("query_columns_general.txt", "w")
    F.write(" ".join(total_columns))
    F.close()

    from sapextractor.incremental_p2p.DB_CONNECTION import get_connection
    c = get_connection()

    dataframe = c.execute_read_sql(total_query, total_columns)
    print(dataframe)
    print(dataframe.columns)
    dataframe.to_csv("prova.csv", index=False)"""
