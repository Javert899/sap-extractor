from sapextractor.database_connection import oracle, sqlite, factory as conn_factory
from sapextractor.algo import factory as algo_factory
from sapextractor.algo.o2c import cli as o2c_cli
from sapextractor.algo.ap_ar import cli as ap_ar_cli


def main():
    print("== Connection to the database ==")
    print("Please pick one of the available connectors:")
    print("1) SQLite")
    print("2) Oracle")
    print()
    connector = input("Please insert your choice -> ")
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
    process = input("Please insert your choice -> ")
    if process == "a":
        return o2c_cli.cli(con)
    elif process == "b":
        return ap_ar_cli.cli(con)


def extraction_with_arguments(db_type, db_con_arg, process, ext_type, ext_arg):
    con = conn_factory.apply(db_type, db_con_arg)
    return algo_factory.apply(con, process, ext_type, ext_arg)

