import os
import sys
import time

sys.path.insert(0, "../..")

import json

ex_time = {}
num_rows = {}

from sapextractor.incremental_p2p.DB_CONNECTION import get_connection

c = get_connection()

t0 = time.time()

for file in os.listdir("."):
    if file.startswith("query_content_"):
        name = file.split(".txt")[0].split("query_content_")[1]

        query = open(file, "r").read()
        columns = open("query_columns_"+name+".txt", "r").read().split()

        print(query)
        print(columns)

        aa = time.time()
        dataframe = c.execute_read_sql(query, columns)
        bb = time.time()
        ex_time[name] = bb - aa
        num_rows[name] = len(dataframe)

        print(dataframe)
        print({"ex_time": ex_time, "num_rows": num_rows, "total_time": bb - t0})

        json.dump({"ex_time": ex_time, "num_rows": num_rows, "total_time": bb - t0}, open("output_measurement.json", "w"), indent=2)

        dataframe.to_csv("dataframe_"+name+".csv", index=False)
