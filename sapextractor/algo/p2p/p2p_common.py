from sapextractor.algo.p2p.tab_processing import eban_processing, ekko_processing


def extract_tables(con):
    eban = eban_processing.apply(con)
    ekko = ekko_processing.apply(con)
    print(eban)
    print(ekko)
