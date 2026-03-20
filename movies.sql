DROP TABLE IF EXISTS languages CASCADE;
CREATE TABLE languages (
    language_id INT PRIMARY KEY,
    language TEXT
);

DROP TABLE IF EXISTS movies_facts CASCADE;

CREATE TABLE movies_facts (
    id BIGINT PRIMARY KEY,
    title TEXT,
    release_date DATE,
    runtime INT,
    popularity FLOAT,
    vote_count INT,
    vote_average FLOAT,
    budget BIGINT,
    revenue BIGINT,
    language TEXT,
    language_id INT,
    FOREIGN KEY (language_id) REFERENCES languages(language_id)
);

DROP TABLE IF EXISTS genres CASCADE;
CREATE TABLE genres (
    genre_id INT PRIMARY KEY,
    genre_name TEXT
);


DROP TABLE IF EXISTS movie_genres CASCADE;

CREATE TABLE movie_genres (
    genre_movie_id SERIAL PRIMARY KEY,
    genre TEXT,
    genre_id INT,
    id BIGINT,
    FOREIGN KEY (id) REFERENCES movies_facts(id),
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
);


DROP TABLE IF EXISTS top_movies_popular;

CREATE TABLE top_movies_popular (
    id BIGINT PRIMARY KEY,
    title TEXT,
    release_date DATE,
    popularity FLOAT
);

DROP TABLE IF EXISTS top_movies_revenue;
CREATE TABLE top_movies_revenue (
    id BIGINT PRIMARY KEY,
    title TEXT,
    release_date DATE,
    revenue BIGINT
);

DROP TABLE IF EXISTS best_rated_movies_by_genre;

CREATE TABLE best_rated_movies_by_genre (
    record_id SERIAL PRIMARY KEY,
    genre_name TEXT,
    id BIGINT,
    title TEXT,
    vote_average FLOAT,
    vote_count INT,
    release_date DATE
);