from sapextractor.algo.p2p.tab_processing import eban_processing, ekko_processing, ekpo_processing


def extract_tables(con):
    eban, eban_nodes_types = eban_processing.apply(con)
    ekko, ekko_nodes_types = ekko_processing.apply(con)
    eban_ekpo_connection = ekpo_processing.eban_ekko_connection(con)
    print(eban)
    print(ekko)
