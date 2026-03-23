# Databricks notebook source
# MAGIC %run "/Shared/proyectoFinal/proceso/00_config"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_catalog}")

movie_conformed = spark.table(silver_tables["movie_conformed"])
movie_genres = spark.table(silver_tables["movie_genres_exploded"])

UNKNOWN_DIRECTOR = "Unknown"
UNKNOWN_LANGUAGE = "unknown"

dim_director = (
    movie_conformed
    .select(F.coalesce(standardize_text(F.col("director_name")), F.lit(UNKNOWN_DIRECTOR)).alias("director_name"))
    .dropDuplicates()
    .withColumn("director_key", F.row_number().over(Window.orderBy("director_name")))
    .select("director_key", "director_name")
)

dim_language = (
    movie_conformed
    .select(F.coalesce(F.lower(standardize_text(F.col("language_code"))), F.lit(UNKNOWN_LANGUAGE)).alias("language_code"))
    .dropDuplicates()
    .withColumn(
        "language_group",
        F.when(F.col("language_code").isin("en", "es", "fr", "it", "de", "pt"), F.col("language_code")).otherwise(F.lit("other"))
    )
    .withColumn("language_key", F.row_number().over(Window.orderBy("language_code")))
    .select("language_key", "language_code", "language_group")
)

dim_date = (
    movie_conformed
    .select(F.col("release_date").alias("full_date"))
    .filter(F.col("full_date").isNotNull())
    .dropDuplicates()
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("day_of_month", F.dayofmonth("full_date"))
    .withColumn("day_of_week", F.date_format("full_date", "E"))
    .withColumn("week_of_year", F.weekofyear("full_date"))
    .withColumn("is_weekend", F.when(F.dayofweek("full_date").isin([1, 7]), F.lit(1)).otherwise(F.lit(0)))
    .withColumn("decade", (F.floor(F.year("full_date") / 10) * 10).cast("int"))
)

dim_genre = (
    movie_genres
    .select(standardize_text(F.col("genre_name")).alias("genre_name"))
    .dropDuplicates()
    .withColumn("genre_key", F.row_number().over(Window.orderBy("genre_name")))
    .select("genre_key", "genre_name")
)

dim_movie = (
    movie_conformed.alias("m")
    .join(
        dim_director.alias("d"),
        F.coalesce(standardize_text(F.col("m.director_name")), F.lit(UNKNOWN_DIRECTOR)) == F.col("d.director_name"),
        "left",
    )
    .join(
        dim_language.alias("l"),
        F.coalesce(F.lower(standardize_text(F.col("m.language_code"))), F.lit(UNKNOWN_LANGUAGE)) == F.col("l.language_code"),
        "left",
    )
    .withColumn("release_date_key", F.date_format("release_date", "yyyyMMdd").cast("int"))
    .withColumn("movie_key", F.row_number().over(Window.orderBy("movie_id")))
    .select(
        "movie_key",
        "movie_id",
        "title",
        F.col("runtime_total_min_final").alias("runtime_total_min"),
        "user_score",
        "vote_count",
        "poster_path",
        "backdrop_path",
        F.col("l.language_key").alias("language_key"),
        F.col("d.director_key").alias("director_key"),
        "release_date_key",
        "budget_source",
        "revenue_source",
        "runtime_source",
        "financial_completeness_flag",
        "runtime_validation_flag",
    )
    .dropDuplicates(["movie_id"])
)

fact_movie_metrics = (
    movie_conformed.alias("m")
    .join(dim_movie.alias("dm"), on="movie_id", how="inner")
    .withColumn("release_date_key", F.date_format("release_date", "yyyyMMdd").cast("int"))
    .withColumn("fact_id", F.row_number().over(Window.orderBy("movie_id")))
    .select(
        "fact_id",
        F.col("dm.movie_key").alias("movie_key"),
        F.col("dm.director_key").alias("director_key"),
        F.col("dm.language_key").alias("language_key"),
        "release_date_key",
        F.col("budget_usd_final").alias("budget_usd"),
        F.col("revenue_usd_final").alias("revenue_usd"),
        "profit_usd",
        "roi_pct",
        "margin_pct",
        F.col("runtime_total_min_final").alias("runtime_total_min"),
        F.col("m.user_score").alias("user_score"),
        F.col("m.vote_count").alias("vote_count"),
        "has_poster",
        "has_backdrop",
    )
)

bridge_movie_genre = (
    movie_genres.alias("mg")
    .join(dim_movie.alias("dm"), on="movie_id", how="inner")
    .join(dim_genre.alias("dg"), standardize_text(F.col("mg.genre_name")) == F.col("dg.genre_name"), how="inner")
    .select(F.col("dm.movie_key").alias("movie_key"), F.col("dg.genre_key").alias("genre_key"))
    .dropDuplicates()
)

for table_name, df in {
    "dim_director": dim_director,
    "dim_language": dim_language,
    "dim_date": dim_date,
    "dim_genre": dim_genre,
    "dim_movie": dim_movie,
    "bridge_movie_genre": bridge_movie_genre,
    "fact_movie_metrics": fact_movie_metrics,
}.items():
    save_delta_table(df, gold_tables[table_name])

print("Proceso silver -> gold completado correctamente.")
