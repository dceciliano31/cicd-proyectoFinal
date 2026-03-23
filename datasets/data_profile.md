# Perfil de datos

## Movies.csv
- `id`: identificador de película
- `title`: nombre de la película
- `genres`: géneros separados por coma
- `language`: idioma original
- `user_score`: puntuación del usuario
- `runtime_hour`, `runtime_min`: duración estructurada
- `release_date`: fecha de estreno
- `vote_count`: cantidad de votos

## FilmDetails.csv
- `id`
- `director`
- `top_billed`
- `budget_usd`
- `revenue_usd`

## MoreInfo.csv
- `id`
- `runtime`: duración en texto
- `budget`: presupuesto en texto
- `revenue`: ingresos en texto
- `film_id`

## PosterPath.csv
- `id`
- `poster_path`
- `backdrop_path`

## Diseño del ETL
- `Movies.csv` funciona como tabla maestra
- `FilmDetails.csv` tiene prioridad para presupuesto e ingresos
- `MoreInfo.csv` se usa como respaldo y complemento
- `PosterPath.csv` enriquece la dimensión película
- `genres` se normaliza hacia dimensión + bridge
