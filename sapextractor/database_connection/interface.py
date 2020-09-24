class DatabaseConnection(object):
    def __init__(self):
        pass

    def execute_read_sql(self, sql):
        raise Exception("not implemented")

    def get_list_tables(self):
        raise Exception("not implemented")

    def write_dataframe(self, dataframe, table_name):
        raise Exception("not implemented")
