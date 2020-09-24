def apply(df, prev, curr):
    df = df.dropna(subset=[prev, curr], how="any")
    stream = df.to_dict('r')
    for el in stream:
        pass
