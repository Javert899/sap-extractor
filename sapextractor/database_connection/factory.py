from sapextractor.database_connection import oracle, sqlite


def apply(db_type, db_con_args):
    if db_type == "sqlite":
        return sqlite.apply(**db_con_args)
    elif db_type == "oracle":
        return oracle.apply(**db_con_args)
