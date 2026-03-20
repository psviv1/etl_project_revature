import io
import pandas as pd
import psycopg2
from cleaner import clean_dataframe
import matplotlib.pyplot as plt
from transform import (
    convert_to_datetime,
    get_genre_movie,
    get_genres,
    popular,
    highest_revenue,
    find_top_movies_in_date_range,
    find_most_popular_genres,
    find_top_movies_in_date_range_language,
    search_movies_by_title,
    get_languages
)
import logging

logging.basicConfig(filename="etl.log",level=logging.INFO, filemode="w", format="%(name)s - %(levelname)s - %(message)s")

class Loader:
    def __init__(self, conn, cur):
        self.conn = conn
        self.cur = cur

    def copy_df_to_table(self, df, table_name, columns):
        buffer = io.StringIO()

        df_to_load = df[columns].copy()
        df_to_load.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        self.cur.copy_expert(
            f"""
            COPY {table_name} ({", ".join(columns)})
            FROM STDIN WITH CSV
            """,
            buffer
        )
        self.conn.commit()
        print(f"Loaded {table_name}")
        logging.info(f"Loaded {table_name} with {len(df)} records")

def plot_top_movies(df, x_col, y_col="title", title="Chart", xlabel="", output_file=None):
    if df.empty:
        print(f"No data to plot for: {title}")
        return

    plot_df = df.copy().sort_values(by=x_col, ascending=True)

    plt.figure(figsize=(12, 7))
    plt.barh(plot_df[y_col], plot_df[x_col])
    plt.xlabel(xlabel if xlabel else x_col)
    plt.ylabel("Movie Title")
    plt.title(title)
    plt.tight_layout()

    if output_file:
        plt.savefig(output_file, bbox_inches="tight")
        print(f"Saved chart to {output_file}")

    plt.close()

if __name__ == "__main__":
    conn = psycopg2.connect(
        host="localhost",
        database="movies_db",
        user="postgres",
        password="mysecretpassword",
        port=5432
    )
    cur = conn.cursor()

    with open("movies.sql", "r") as f:
        sql_script = f.read()

    cur.execute(sql_script)
    conn.commit()

    logging.info("Tables created successfully")

    # Step 1: Chunk the CSV file
    chunk_size = 50000
    file_path = "datasets/TMDB_movie_dataset_v11.csv"

    dfs = []

    for chunk_df in pd.read_csv(file_path, chunksize=50000):
        dfs.append(chunk_df)

    df = pd.concat(dfs, ignore_index=True)
    logging.info(f"Data loaded from CSV. Shape: {df.shape}")
    # Step 3: Clean and transform
    df = clean_dataframe(df)
    df = convert_to_datetime(df, "release_date")
    logging.info(f"Data cleaned and transformed. Shape: {df.shape}")

    print(df.head())
    print(df.info())

    
    print("All genres:")
    genres_df = get_genres(df)
    print(genres_df)
    
    
   
    

    print("Top 10 popular movies:")
    popular_movies = popular(df, top_n=10)
    plot_top_movies(popular_movies, x_col="popularity", y_col="title", title="Top 10 Popular Movies", xlabel="Popularity", output_file="top_popular_movies.png")
    print(popular_movies)
    print("Top 10 highest revenue movies:")
    highest_revenue_movies = highest_revenue(df, top_n=10)
    plot_top_movies(highest_revenue_movies, x_col="revenue", y_col="title", title="Top 10 Highest Revenue Movies", xlabel="Revenue", output_file="top_highest_revenue_movies.png")
    print(highest_revenue_movies)
    print("Top 10 movies in a specific date range:")
    print(find_top_movies_in_date_range(df, "2025-09-01", "2026-03-26", top_n=10))
    print("Top 10 most popular genres:")
    print(find_most_popular_genres(df, top_n=10))
    print("Top 10 movies in a specific date range and language:")
    print(find_top_movies_in_date_range_language(df, "2025-09-01", "2026-03-26", language="en", top_n=10))
    print("Search movie:")
    print(search_movies_by_title(df, "Jurassic Park"))

    genre_movie_df = get_genre_movie(df)
    
    languages_df = get_languages(df).copy()

    movies_df = df[
        [
            "id",
            "title",
            "release_date",
            "runtime",
            "popularity",
            "vote_count",
            "vote_average",
            "budget",
            "revenue",
            "original_language"
        ]
    ].copy()
    languages_df['language_id'] = languages_df.index + 1
    languages_df.rename(columns={"original_language": "language"}, inplace=True)

    movies_df = movies_df.merge(languages_df, left_on="original_language", right_on="language", how="left")
    movies_df = movies_df.drop(columns=["original_language"])

    genres_df = genres_df.rename(columns={"genre": "genre_name"})

    genre_movie_df = genre_movie_df.merge(genres_df, left_on="genres", right_on="genre_name", how="left")
    genre_movie_df.drop(columns=[ "genre_name", "genres"], inplace=True)

    # LOAD
    loader = Loader(conn, cur)
    loader.copy_df_to_table(languages_df, "languages", languages_df.columns.tolist())
    
    #load movie facts
    loader.copy_df_to_table(movies_df, "movies_facts", movies_df.columns.tolist())

    #load genres
    print(type(genres_df))
    
    loader.copy_df_to_table(genres_df, "genres", ["genre_id", "genre_name"])

    
    loader.copy_df_to_table(genre_movie_df, "movie_genres", ["genre_id", "id"])

    loader.copy_df_to_table(popular_movies, "top_movies_popular", popular_movies.columns.tolist())


    loader.copy_df_to_table(highest_revenue_movies, "top_movies_revenue", highest_revenue_movies.columns.tolist())

    #Transformations on loaded tables
    #get top 10 highest revenue movies for a particular genre
    genre = input("Enter genre to find top 10 revenue movies: ")
    genre = genre.strip()
    cur.execute("""
    SELECT
        mf.id,
        mf.title,
        g.genre_name,
        mf.revenue
    FROM movies_facts mf
    JOIN movie_genres mg
        ON mf.id = mg.id
    JOIN genres g
        ON mg.genre_id = g.genre_id
    WHERE g.genre_name = %s
    ORDER BY mf.revenue DESC
    LIMIT 10
    """, (genre,))
    rows = cur.fetchall()

    print(f"\nTop 10 revenue movies for genre = {genre}:")
    for row in rows:
        print(row)
    genre_revenue_df = pd.DataFrame(rows,columns=["id", "title", "genre_name", "revenue"])
    plot_top_movies(genre_revenue_df, x_col="revenue", y_col="title", title=f"Top 10 Revenue Movies for Genre: {genre}", xlabel="Revenue", output_file=f"top_revenue_movies_{genre}.png")

    #get top 10 most popular movies for a particular genre in a particular date range and language
    genre = input("Enter genre to find top 10 popular movies: ").strip()
    start_date = input("Enter start date (YYYY-MM-DD): ").strip()
    end_date = input("Enter end date (YYYY-MM-DD): ").strip()
    language = input("Enter language code (e.g., 'en' for English): ").strip()

    cur.execute("""
        SELECT
            mf.id,
            mf.title,
            g.genre_name,
            mf.release_date,
            mf.language,
            mf.popularity,
            mf.vote_count,
            mf.vote_average
        FROM movies_facts mf
        JOIN movie_genres mg
            ON mf.id = mg.id
        JOIN genres g
            ON mg.genre_id = g.genre_id
        WHERE g.genre_name = %s
        AND mf.release_date BETWEEN %s AND %s
        AND mf.language = %s
        ORDER BY mf.popularity DESC
        LIMIT 10
    """, (genre, start_date, end_date, language))

    rows = cur.fetchall()

    print(f"\nTop 10 popular movies for genre = {genre}, date range = {start_date} to {end_date}, language = {language}:")
    for row in rows:
        print(row)

    popular_movies_df = pd.DataFrame(rows, columns=["id",
        "title",
        "genre_name",
        "release_date",
        "language",
        "popularity",
        "vote_count",
        "vote_average"])
    plot_top_movies(popular_movies_df, x_col="popularity", y_col="title", title=f"Top 10 Popular Movies for Genre: {genre} released from {start_date} to {end_date}", xlabel="Popularity", output_file=f"top_popular_movies_{genre}.png")


    #show most liked movies for each genre
    cur.execute("""
        INSERT INTO best_rated_movies_by_genre (
            genre_name,
            id,
            title,
            vote_average,
            vote_count,
            release_date
        )
        SELECT genre_name, id, title, vote_average, vote_count, release_date
        FROM (
            SELECT
                g.genre_name,
                mf.id,
                mf.title,
                mf.vote_average,
                mf.vote_count,
                mf.release_date,
                ROW_NUMBER() OVER (
                    PARTITION BY g.genre_name
                    ORDER BY mf.vote_average DESC, mf.vote_count DESC
                ) AS rank_num
            FROM movies_facts mf
            JOIN movie_genres mg
                ON mf.id = mg.id
            JOIN genres g
                ON mg.genre_id = g.genre_id
            WHERE mf.vote_count >= 100
        ) ranked
        WHERE rank_num <= 10
    """)
    print("Inserted best rated movies by genre")
    conn.commit()
    #get best rated movies for each genre
    cur.execute("""
        SELECT genre_name, title, vote_average, vote_count, release_date
        FROM best_rated_movies_by_genre
        ORDER BY genre_name, vote_average DESC
    """)
    rows = cur.fetchall()
    best_rated_movies_df = pd.DataFrame(rows, columns=["genre_name", "title", "vote_average", "vote_count", "release_date"])
    selected_genres = ["Action", "Drama", "Comedy"]
    filtered_df = best_rated_movies_df[best_rated_movies_df["genre_name"].isin(selected_genres)]
    for genre in selected_genres:
        genre_df = filtered_df[filtered_df["genre_name"] == genre]
        plot_top_movies(genre_df, x_col="vote_average", y_col="title", title=f"Top Rated Movies for Genre: {genre}", xlabel="Average Vote", output_file=f"top_rated_movies_{genre}.png")

    cur.close()
    conn.close()