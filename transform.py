import pandas as pd
def popular(df, top_n=10):
    return df.nlargest(top_n, 'popularity')[['id', 'title', 'release_date', 'popularity']]

def highest_revenue(df, top_n=10):
    return df.nlargest(top_n, 'revenue')[['id', 'title', 'release_date', 'revenue']]

def convert_to_datetime(df, column_name):
    df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
    return df

def find_top_movies_in_date_range(df, start_date, end_date, top_n=10):
    mask = (df['release_date'] >= start_date) & (df['release_date'] <= end_date)
    filtered_df = df.loc[mask]
    return filtered_df.nlargest(top_n, 'popularity')[['id', 'title', 'release_date', 'popularity']]

def find_most_popular_genres(df, top_n=10):
    genre_counts = df['genres'].str.split(',').explode().value_counts()
    return genre_counts.head(top_n)


def find_top_movies_in_date_range_language(df, start_date, end_date, language = 'en', top_n=10):
    mask = (df['release_date'] >= start_date) & (df['release_date'] <= end_date) & (df['original_language'] == language)
    filtered_df = df.loc[mask]
    return filtered_df.nlargest(top_n, 'popularity')[['title', 'release_date', 'popularity']]

def search_movies_by_title(df, title):
    return df[df['title'] == title][['title', 'release_date', 'popularity']]

def get_genres(df):
    genres_df = (
        df['genres']
        .dropna()
        .str.split(',')
        .explode()
        .str.strip()
        .drop_duplicates()
        .reset_index(drop=True)
        .to_frame(name='genre')
    )

    # create IDs
    genres_df['genre_id'] = genres_df.index + 1

    # reorder columns to match SQL
    genres_df = genres_df[['genre_id', 'genre']]

    return genres_df

# generate movie_id and genre pairs for movie_genres table
def get_genre_movie(df):
    genre_movie_df = df[['id', 'genres']].dropna()
    genre_movie_df['genres'] = genre_movie_df['genres'].str.split(',')
    genre_movie_df = genre_movie_df.explode('genres')
    genre_movie_df['genres'] = genre_movie_df['genres'].str.strip() 
    return genre_movie_df


def get_languages(df):
    languages_df = df[['original_language']].dropna().drop_duplicates().reset_index(drop=True)
    return languages_df
