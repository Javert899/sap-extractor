class DatabaseConnection(object):
    def __init__(self):
        pass

    def execute_read_sql(self, sql):
        raise Exception("not implemented")

    def get_list_tables(self):
        raise Exception("not implemented")

    def write_dataframe(self, dataframe, table_name):
        raise Exception("not implemented")

    def get_columns(self, table_name):
        raise Exception("not implemented")

    def format_table_name(self, table_name):
        raise Exception("not implemented")

    def prepare_query(self, table_name, columns):
        raise Exception("not implemented")

    def prepare_and_execute_query(self, table_name, columns):
        raise Exception("not implemented")
