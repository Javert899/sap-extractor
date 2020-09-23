from sapextractor.utils.change_tables import extract_change
from sapextractor.utils.tstct import extract_tstct


def apply(con):
    tstct = extract_tstct.apply_static(con)
    change = extract_change.apply_static(con)
