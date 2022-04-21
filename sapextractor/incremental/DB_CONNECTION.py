import sys

sys.path.insert(0, "../..")

import sapextractor
import json

connection_parameters = json.load(open("connection_parameters.json", "r"))


def get_connection():
    if connection_parameters["connection"] == "oracle":
        return sapextractor.connect_oracle(**connection_parameters["parameters"])
    elif connection_parameters["connection"] == "mic_sql":
        return sapextractor.connect_mssql(**connection_parameters["parameters"])
