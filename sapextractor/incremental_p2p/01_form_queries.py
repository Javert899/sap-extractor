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
        return str(tables_count[table] + 1)
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
    ret.append(parameters["prefix"] + "EBAN")
    if apply_rownum:
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


def form_bkpf_query(rseg_name="rseg", invoice_bkpf_name="invoicebkpf", invoice_bseg_name="invoicebseg", payment_bkpf_name="paymentbkpf", payment_bseg_name="paymentbseg", apply_rownum=True):
    ret = []
    #ret.append("SELECT Count(*) FROM (")
    ret.append("SELECT PAYMENTBSEGROWNUMBER, " + payment_bseg_name + ".MANDT AS MANDT, " + payment_bseg_name + ".BUKRS AS BUKRS, " + payment_bseg_name + ".GJAHR AS GJAHR, ACCDOCBELNRGJAHR, ACCDOCBELNRBUZEIGJAHR, INVOICEBELNRGJAHR, INVOICEBELNRBUZEIGJAHR, PAYMENTBSEGBSCHL, PAYMENTBLDAT, PAYMENTBUDAT, PAYMENTBLART, PAYMENTUSNAM")
    ret.append("FROM (")
    ret.append("SELECT "+row_number()+" AS PAYMENTBSEGROWNUMBER, MANDT, BUKRS, GJAHR, BELNR, BUZEI AS PAYMENTBSEGBUZEI, CONCAT(BELNR, GJAHR) AS ACCDOCBELNRGJAHR, CONCAT(CONCAT(BELNR, BUZEI), GJAHR) AS ACCDOCBELNRBUZEIGJAHR, BSCHL AS PAYMENTBSEGBSCHL FROM")
    ret.append(parameters["prefix"]+"BSEG")
    if apply_rownum:
        ret.append("WHERE "+row_number()+" >= "+get_table_count("BSEG"))
    ret.append(") " + payment_bseg_name)
    ret.append("JOIN (")
    ret.append("SELECT "+row_number()+" AS INVOICEBSEGROWNUMBER, MANDT, BUKRS, GJAHR, BELNR, BUZEI AS INVOICEBSEGBUZEI, AUGBL, AUGGJ, AUGDT FROM")
    ret.append(parameters["prefix"]+"BSAK")
    ret.append(") " + invoice_bseg_name + " ON " + payment_bseg_name + ".MANDT = " + invoice_bseg_name + ".MANDT")
    ret.append("AND " + payment_bseg_name + ".BUKRS = " + invoice_bseg_name + ".BUKRS")
    ret.append("AND " + payment_bseg_name + ".GJAHR = " + invoice_bseg_name + ".AUGGJ")
    ret.append("AND " + payment_bseg_name + ".BELNR = " + invoice_bseg_name + ".AUGBL")
    ret.append("JOIN (")
    ret.append("SELECT MANDT, BUKRS, GJAHR, BELNR, BLDAT AS PAYMENTBLDAT, BUDAT AS PAYMENTBUDAT, BLART AS PAYMENTBLART, USNAM AS PAYMENTUSNAM FROM")
    ret.append(parameters["prefix"]+"BKPF")
    ret.append(") " + payment_bkpf_name + " ON " + payment_bseg_name + ".MANDT = " + payment_bkpf_name + ".MANDT")
    ret.append("AND " + payment_bseg_name + ".BUKRS = " + payment_bkpf_name + ".BUKRS")
    ret.append("AND " + payment_bseg_name + ".GJAHR = " + payment_bkpf_name + ".GJAHR")
    ret.append("AND " + payment_bseg_name + ".BELNR = " + payment_bkpf_name + ".BELNR")
    ret.append("JOIN (")
    ret.append("SELECT MANDT, BUKRS, GJAHR, BELNR, AWKEY FROM")
    ret.append(parameters["prefix"]+"BKPF")
    ret.append(") " + invoice_bkpf_name + " ON " + invoice_bseg_name + ".MANDT = " + invoice_bkpf_name + ".MANDT")
    ret.append("AND " + invoice_bseg_name + ".BUKRS = " + invoice_bkpf_name + ".BUKRS")
    ret.append("AND " + invoice_bseg_name + ".GJAHR = " + invoice_bkpf_name + ".GJAHR")
    ret.append("AND " + invoice_bseg_name + ".BELNR = " + invoice_bkpf_name + ".BELNR")
    ret.append("JOIN (")
    ret.append("SELECT MANDT, BUKRS, CONCAT(BELNR, GJAHR) AS INVOICEBELNRGJAHR, CONCAT(CONCAT(BELNR, BUZEI), GJAHR) AS INVOICEBELNRBUZEIGJAHR, BUZEI AS RSEGBUZEI FROM")
    ret.append(parameters["prefix"]+"RSEG")
    ret.append(") " + rseg_name +" ON " + invoice_bkpf_name + ".MANDT = " + rseg_name + ".MANDT")
    ret.append("AND " + rseg_name +".BUKRS = " + invoice_bkpf_name + ".BUKRS")
    ret.append("AND " + rseg_name +".INVOICEBELNRGJAHR = " + invoice_bkpf_name + ".AWKEY")
    ret.append("AND LTRIM(" + rseg_name +".RSEGBUZEI, '0') = LTRIM(" + invoice_bseg_name + ".INVOICEBSEGBUZEI, '0')")

    columns = ["PAYMENTBSEGROWNUMBER", "MANDT", "BUKRS", "GJAHR", "ACCDOCBELNRGJAHR", "ACCDOCBELNRBUZEIGJAHR", "INVOICEBELNRGJAHR", "INVOICEBELNRBUZEIGJAHR", "PAYMENTBSEGBSCHL", "PAYMENTBLDAT", "PAYMENTBUDAT", "PAYMENTBLART", "PAYMENTUSNAM"]

    #ret.append(")")
    #columns = ["COUNT"]

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


def changes_ekko(apply_rownum=True):
    columns = ["MANDT", "EBELN", "CHANGENR", "TABNAME", "FNAME", "CHNGIND", "VALUE_NEW", "VALUE_OLD"]

    ret = ["SELECT a.MANDANT AS MANDT, a.OBJECTID AS EBELN, a.CHANGENR AS CHANGENR, TABNAME, FNAME, CHNGIND, VALUE_NEW, VALUE_OLD FROM"]
    ret.append("(SELECT MANDANT, OBJECTID, CHANGENR, TABNAME, FNAME, CHNGIND, VALUE_NEW, VALUE_OLD FROM "+parameters["prefix"]+"CDPOS")
    if apply_rownum:
        ret.append("WHERE "+row_number()+" >= "+get_table_count("CDPOS"))
    ret.append(") a")
    ret.append("JOIN")
    ret.append("(SELECT MANDT, EBELN FROM "+parameters["prefix"]+"EKKO) b")
    ret.append("ON a.MANDANT = b.MANDT AND a.OBJECTID = b.EBELN JOIN")
    ret.append("(SELECT MANDANT, CHANGENR, USERNAME, UDATE, UTIME, TCODE FROM "+parameters["prefix"]+"CDHDR) c")
    ret.append("ON a.MANDANT = c.MANDANT AND a.CHANGENR = c.CHANGENR")

    return sqlparse.format(" ".join(ret), reindent=True), columns


def changes_rbkp(apply_rownum=True):
    columns = ["MANDT", "BELNRGJAHR", "CHANGENR", "TABNAME", "FNAME", "CHNGIND", "VALUE_NEW", "VALUE_OLD"]

    ret = ["SELECT a.MANDANT AS MANDT, a.OBJECTID AS BELNRGJAHR, a.CHANGENR AS CHANGENR, TABNAME, FNAME, CHNGIND, VALUE_NEW, VALUE_OLD FROM"]
    ret.append("(SELECT MANDANT, OBJECTID, CHANGENR, TABNAME, FNAME, CHNGIND, VALUE_NEW, VALUE_OLD FROM "+parameters["prefix"]+"CDPOS")
    if apply_rownum:
        ret.append("WHERE "+row_number()+" >= "+get_table_count("CDPOS"))
    ret.append(") a")
    ret.append("JOIN")
    ret.append("(SELECT MANDT, CONCAT(BELNR, GJAHR) AS BELNRGJAHR FROM "+parameters["prefix"]+"RBKP) b")
    ret.append("ON a.MANDANT = b.MANDT AND a.OBJECTID = b.BELNRGJAHR JOIN")
    ret.append("(SELECT MANDANT, CHANGENR, USERNAME, UDATE, UTIME, TCODE FROM "+parameters["prefix"]+"CDHDR) c")
    ret.append("ON a.MANDANT = c.MANDANT AND a.CHANGENR = c.CHANGENR")

    return sqlparse.format(" ".join(ret), reindent=True), columns


def changes_bkpf(apply_rownum=True):
    columns = ["MANDT", "BELNRGJAHR", "CHANGENR", "TABNAME", "FNAME", "CHNGIND", "VALUE_NEW", "VALUE_OLD"]

    ret = ["SELECT a.MANDANT AS MANDT, a.OBJECTID AS BELNRGJAHR, a.CHANGENR AS CHANGENR, TABNAME, FNAME, CHNGIND, VALUE_NEW, VALUE_OLD FROM"]
    ret.append("(SELECT MANDANT, OBJECTID, CHANGENR, TABNAME, FNAME, CHNGIND, VALUE_NEW, VALUE_OLD FROM "+parameters["prefix"]+"CDPOS")
    if apply_rownum:
        ret.append("WHERE "+row_number()+" >= "+get_table_count("CDPOS"))
    ret.append(") a")
    ret.append("JOIN")
    ret.append("(SELECT MANDT, CONCAT(BELNR, GJAHR) AS BELNRGJAHR FROM "+parameters["prefix"]+"BKPF) b")
    ret.append("ON a.MANDANT = b.MANDT AND a.OBJECTID = b.BELNRGJAHR JOIN")
    ret.append("(SELECT MANDANT, CHANGENR, USERNAME, UDATE, UTIME, TCODE FROM "+parameters["prefix"]+"CDHDR) c")
    ret.append("ON a.MANDANT = c.MANDANT AND a.CHANGENR = c.CHANGENR")

    return sqlparse.format(" ".join(ret), reindent=True), columns


def write_result(name, query, columns):
    F = open("query_content_" + name + ".txt", "w")
    F.write(query)
    F.close()

    F = open("query_columns_" + name + ".txt", "w")
    F.write(" ".join(columns))
    F.close()


if __name__ == "__main__":
    ekpo_query, ekpo_columns = form_ekpo_query()
    write_result("po", ekpo_query, ekpo_columns)

    pr_query, pr_columns = final_query_purchase_requisitions()
    write_result("pr", pr_query, pr_columns)

    inv_query, inv_columns = final_query_invoice_processing()
    write_result("invp", inv_query, inv_columns)

    bkpf_query, bkpf_columns = form_bkpf_query()
    write_result("bkpf", bkpf_query, bkpf_columns)

    ekko_chng_query, ekko_chng_columns = changes_ekko()
    write_result("chngsekko", ekko_chng_query, ekko_chng_columns)

    rbkp_chng_query, rbkp_chng_columns = changes_rbkp()
    write_result("chngsrbkp", rbkp_chng_query, rbkp_chng_columns)

    bkpf_chng_query, bkpf_chng_columns = changes_bkpf()
    write_result("chngsrbkp", bkpf_chng_query, bkpf_chng_columns)

    from sapextractor.incremental_p2p.DB_CONNECTION import get_connection

    c = get_connection()

    dataframe = c.execute_read_sql(bkpf_query, bkpf_columns)
    print(dataframe)
    print(dataframe.columns)
    dataframe.to_csv("prova.csv", index=False)

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
