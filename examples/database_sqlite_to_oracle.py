import sapextractor
import traceback
from sapextractor.utils import constants
from examples import example_connection


def execute_script():
    c1 = example_connection.get_sqlite_con()
    c2 = example_connection.get_oracle_con()
    #list_tables = c1.get_list_tables()
    for table in constants.INVOLVED_TABLES:
        try:
            print()
            print(table)
            df = c1.execute_read_sql("SELECT * FROM "+table)
            c2.write_dataframe(df, table)
        except:
            traceback.print_exc()


if __name__ == "__main__":
    execute_script()
