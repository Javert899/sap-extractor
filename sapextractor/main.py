from sapextractor.database_connection import oracle, sqlite


def main():
    print("== Connection to the database ==")
    print("Please pick one of the available connectors:")
    print("1) SQLite")
    print("2) Oracle")
    print()
    connector = input("Please insert your choice -> ")
    if connector == "1":
        con = sqlite.cli()
    else:
        con = oracle.cli()
    return con
