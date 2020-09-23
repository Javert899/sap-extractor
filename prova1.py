from sapextractor.database_connection import sqlite
from sapextractor.change_tables import extract_change

con = sqlite.apply("sap.sqlite")
extract_change.apply(con)
