from sapextractor.utils.blart import extract_blart


def apply(con):
    df = con.execute_read_sql("SELECT MANDT, BUKRS, GJAHR, BLART, Count(*) FROM "+con.table_prefix+"RBKP GROUP BY MANDT, BUKRS, GJAHR, BLART ORDER BY Count(*) DESC", ["Client", "Company Code", "Fiscal Year", "Document Type", "Num. Elements"])
    doc_types = set(df["DOCUMENT TYPE"].unique())
    vbtyp = extract_blart.apply_static(con, doc_types=doc_types)
    df = df.head(100)
    df["DOCUMENT TYPE"] = df["DOCUMENT TYPE"].map(vbtyp)
    return df.to_html(index=False)
