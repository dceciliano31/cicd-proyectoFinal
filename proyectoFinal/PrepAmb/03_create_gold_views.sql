USE CATALOG proyectoFinal;
USE SCHEMA gold;

CREATE OR REPLACE VIEW proyectoFinal.gold.vw_powerbi_movie_finance AS
SELECT
    m.movie_id,
    m.title AS movie_title,
    d.director_name,
    l.language_code AS language_name,
    dt.full_date AS release_date,
    dt.year AS year_number,
    f.budget_usd,
    f.revenue_usd,
    f.profit_usd,
    f.roi_pct,
    f.margin_pct,
    f.runtime_total_min,
    f.user_score,
    f.vote_count
FROM proyectoFinal.gold.fact_movie_metrics f
JOIN proyectoFinal.gold.dim_movie m
    ON f.movie_key = m.movie_key
LEFT JOIN proyectoFinal.gold.dim_director d
    ON m.director_key = d.director_key
LEFT JOIN proyectoFinal.gold.dim_language l
    ON m.language_key = l.language_key
LEFT JOIN proyectoFinal.gold.dim_date dt
    ON f.release_date_key = dt.date_key;

CREATE OR REPLACE VIEW vw_powerbi_movie_genre AS
SELECT
    m.movie_key,
    m.movie_id,
    m.title,
    g.genre_name
FROM proyectoFinal.gold.dim_movie m
LEFT JOIN proyectoFinal.gold.bridge_movie_genre b
    ON m.movie_key = b.movie_key
LEFT JOIN proyectoFinal.gold.dim_genre g
    ON b.genre_key = g.genre_key;
