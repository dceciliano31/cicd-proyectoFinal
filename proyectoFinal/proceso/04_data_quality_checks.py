# Databricks notebook source
# MAGIC %run "/Shared/proyectoFinal/proceso/00_config"

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql(f"USE CATALOG {catalog_name}")

tables_to_check = {
    "bronze.movies": bronze_tables["movies"],
    "bronze.film_details": bronze_tables["film_details"],
    "bronze.more_info": bronze_tables["more_info"],
    "bronze.poster_path": bronze_tables["poster_path"],
    "silver.movie_conformed": silver_tables["movie_conformed"],
    "gold.dim_movie": gold_tables["dim_movie"],
    "gold.fact_movie_metrics": gold_tables["fact_movie_metrics"],
}

print("Conteos por tabla:")
for label, full_name in tables_to_check.items():
    print(f"{label}: {spark.table(full_name).count()}")

movie_conformed = spark.table(silver_tables["movie_conformed"])
dim_movie = spark.table(gold_tables["dim_movie"])
fact_movie = spark.table(gold_tables["fact_movie_metrics"])
bridge_movie_genre = spark.table(gold_tables["bridge_movie_genre"])
dim_genre = spark.table(gold_tables["dim_genre"])

metrics = {
    "duplicate_movie_ids_in_silver": movie_conformed.groupBy("movie_id").count().filter("count > 1").count(),
    "null_movie_keys_in_dim_movie": dim_movie.filter(F.col("movie_key").isNull()).count(),
    "null_movie_keys_in_fact": fact_movie.filter(F.col("movie_key").isNull()).count(),
    "facts_without_financials": fact_movie.filter(F.col("budget_usd").isNull() | F.col("revenue_usd").isNull()).count(),
    "runtime_mismatch_count": movie_conformed.filter(F.col("runtime_validation_flag") == "MISMATCH").count(),
    "bridge_rows_without_genre": bridge_movie_genre.join(dim_genre, "genre_key", "left_anti").count(),
    "movies_without_title": dim_movie.filter(F.col("title").isNull() | (F.trim(F.col("title")) == "")).count(),
}

print("\nMétricas de calidad:")
for key, value in metrics.items():
    print(f"{key}: {value}")

assert metrics["duplicate_movie_ids_in_silver"] == 0, "Existen duplicados en movie_conformed."
assert metrics["null_movie_keys_in_dim_movie"] == 0, "Existen movie_key nulos en dim_movie."
assert metrics["null_movie_keys_in_fact"] == 0, "Existen movie_key nulos en fact_movie_metrics."
assert metrics["bridge_rows_without_genre"] == 0, "Existen géneros huérfanos en bridge_movie_genre."
assert metrics["movies_without_title"] == 0, "Existen películas sin título en dim_movie."

print("\nValidaciones completadas correctamente.")
