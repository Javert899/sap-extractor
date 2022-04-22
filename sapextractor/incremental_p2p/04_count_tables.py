import sys

sys.path.insert(0, "../..")

import json
from sapextractor.incremental_p2p.DB_CONNECTION import get_connection

parameters = json.load(open("parameters.json", "r"))
tables = ["EKKO", "EKPO", "RBKP", "RSEG", "EKBE", "BKPF", "BSEG", "CDHDR", "CDPOS"]

c = get_connection()


def count_table(table):
    df = c.execute_read_sql("SELECT Count(*) FROM "+parameters["prefix"]+table, ["COUNT"])
    return list(df["COUNT"])[0]


def count_tables():
    dictio = {}
    for table in tables:
        dictio[table] = count_table(table)
    json.dump(dictio, open("tables_count.json", "w"), indent=2)


if __name__ == "__main__":
    count_tables()
