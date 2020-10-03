from sapextractor.database_connection import oracle, sqlite
from sapextractor.algo.o2c import cli as o2c_cli
from sapextractor.algo.ap_ar import cli as ap_ar_cli


def main():
    print("== Connection to the database ==")
    print("Please pick one of the available connectors:")
    print("1) SQLite")
    print("2) Oracle")
    print()
    connector = input("Please insert your choice (default: 1) -> ")
    if not connector:
        connector = "1"
    con = None
    if connector == "1":
        con = sqlite.cli()
    elif connector == "2":
        con = oracle.cli()
    print()
    print("Please pick one of the supported processes:")
    print("a) O2C")
    print("b) Accounting (AP/AR)")
    print()
    process = input("Please insert your choice (default: a) -> ")
    if not process:
        process = "a"
    if process == "a":
        return o2c_cli.cli(con)
    elif process == "b":
        return ap_ar_cli.cli(con)
