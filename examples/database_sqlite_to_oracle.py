import sapextractor


def execute_script():
    c1 = sapextractor.connect_sqlite("../sap.sqlite")
    c2 = sapextractor.connect_oracle(hostname="127.0.0.1", port="1521", sid="xe", username="system", password="oracle")
    list_tables = c1.get_list_tables()
    for table in list_tables:
        print()
        print(table)
        df = c1.execute_read_sql("SELECT * FROM "+table)
        c2.write_dataframe(df, table)


if __name__ == "__main__":
    execute_script()
