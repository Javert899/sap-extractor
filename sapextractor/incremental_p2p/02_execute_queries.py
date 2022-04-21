import os
import sys

sys.path.insert(0, "../..")

import json

from sapextractor.incremental_p2p.DB_CONNECTION import get_connection

c = get_connection()

for file in os.listdir("."):
    if file.startswith("query_content_"):
        name = file.split(".txt")[0].split("query_content_")[1]

        query = open(file, "r").read()
        columns = open("query_columns_"+name+".txt", "r").read().split()

        print(query)
        print(columns)

        dataframe = c.execute_read_sql(query, columns)

        print(dataframe)

        dataframe.to_csv("dataframe_"+name+".csv", index=False)
