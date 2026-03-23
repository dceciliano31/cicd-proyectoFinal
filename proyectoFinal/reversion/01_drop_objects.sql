DROP VIEW IF EXISTS proyectoFinal.gold.vw_powerbi_movie_finance;
DROP VIEW IF EXISTS proyectoFinal.gold.vw_powerbi_movie_genre;

DROP TABLE IF EXISTS proyectoFinal.gold.fact_movie_metrics;
DROP TABLE IF EXISTS proyectoFinal.gold.bridge_movie_genre;
DROP TABLE IF EXISTS proyectoFinal.gold.dim_movie;
DROP TABLE IF EXISTS proyectoFinal.gold.dim_genre;
DROP TABLE IF EXISTS proyectoFinal.gold.dim_date;
DROP TABLE IF EXISTS proyectoFinal.gold.dim_language;
DROP TABLE IF EXISTS proyectoFinal.gold.dim_director;

DROP TABLE IF EXISTS proyectoFinal.silver.movie_genres_exploded;
DROP TABLE IF EXISTS proyectoFinal.silver.movie_conformed;
DROP TABLE IF EXISTS proyectoFinal.silver.poster_silver;
DROP TABLE IF EXISTS proyectoFinal.silver.more_info_silver;
DROP TABLE IF EXISTS proyectoFinal.silver.film_details_silver;
DROP TABLE IF EXISTS proyectoFinal.silver.movies_silver;

DROP TABLE IF EXISTS proyectoFinal.bronze.poster_path;
DROP TABLE IF EXISTS proyectoFinal.bronze.more_info;
DROP TABLE IF EXISTS proyectoFinal.bronze.film_details;
DROP TABLE IF EXISTS proyectoFinal.bronze.movies;
