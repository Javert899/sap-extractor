from sapextractor.utils.change_tables import mapping


def extract(con, mandt="800"):
    df = con.execute_read_sql(
                              "SELECT OBJECTCLAS, TABNAME, FNAME, VALUE_OLD, VALUE_NEW, CHNGIND, Count(*) AS COUNT FROM (SELECT OBJECTCLAS, TABNAME, FNAME, VALUE_OLD, VALUE_NEW, CHNGIND FROM "+con.table_prefix+"CDPOS WHERE (OBJECTCLAS = 'VERKBELEG' OR OBJECTCLAS = 'LIEFERUNG') AND MANDANT = '"+mandt+"') GROUP BY OBJECTCLAS, TABNAME, FNAME, VALUE_OLD, VALUE_NEW, CHNGIND", ["OBJECTCLAS", "TABNAME", "FNAME", "VALUE_OLD", "VALUE_NEW", "CHNGIND", "COUNT"])
    stream = df.to_dict("r")
    fnames = {}
    counter = {}
    i = 0
    while i < len(stream):
        el = stream[i]

        change, typ = mapping.perform_mapping(el["TABNAME"], el["FNAME"], el["VALUE_OLD"], el["VALUE_NEW"], el["CHNGIND"], fnames, return_however=False)
        el["CHANGEDESC"] = (change, typ, el["OBJECTCLAS"])
        if el["CHANGEDESC"][0] is None:
            del stream[i]
            continue
        if not el["CHANGEDESC"] in counter:
            counter[el["CHANGEDESC"]] = 0

        counter[el["CHANGEDESC"]] += int(el["COUNT"])
        i = i + 1

    counter = [[x, y] for x, y in counter.items()]
    counter = sorted(counter, key=lambda x: x[1], reverse=True)

    return counter
