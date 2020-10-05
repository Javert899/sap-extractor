def filter_on_case_size(df, case_id_glue="case:concept:name", min_case_size=2, max_case_size=None):
    """
    Filter a dataframe keeping only traces with at least the specified number of events

    Parameters
    -----------
    df
        Dataframe
    case_id_glue
        Case ID column in the CSV
    min_case_size
        Minimum size of a case
    max_case_size
        Maximum case size

    Returns
    -----------
    df
        Filtered dataframe
    """
    element_group_size = df[case_id_glue].groupby(df[case_id_glue]).transform('size')
    df = df[element_group_size >= min_case_size]
    if max_case_size:
        element_group_size = df[case_id_glue].groupby(df[case_id_glue]).transform('size')
        df = df[element_group_size <= max_case_size]
    return df
