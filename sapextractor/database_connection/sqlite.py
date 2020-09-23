import sqlite3


def apply(sqlite_path):
    con = sqlite3.connect(sqlite_path)
    return con
