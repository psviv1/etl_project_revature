#clean dataframe by removing duplicate
import pandas as pd
def clean_dataframe(df):
    # Remove duplicate rows
    df = df.dropna(subset=["id"])
    df = df.drop_duplicates(subset=["id"])
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce").dt.date
    df["runtime"] = pd.to_numeric(df["runtime"], errors="coerce")
    df["popularity"] = pd.to_numeric(df["popularity"], errors="coerce")
    df["vote_count"] = pd.to_numeric(df["vote_count"], errors="coerce")
    df["vote_average"] = pd.to_numeric(df["vote_average"], errors="coerce")
    df["budget"] = pd.to_numeric(df["budget"], errors="coerce")
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")

    df = df[(df["imdb_id"].notna()) & (df["adult"] == False)]

    return df
