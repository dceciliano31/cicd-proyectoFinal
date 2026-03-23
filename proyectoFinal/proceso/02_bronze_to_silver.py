# Databricks notebook source
# MAGIC %run "/Shared/proyectoFinal/proceso/00_config"

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_catalog}")

movies = spark.table(bronze_tables["movies"]).alias("m")
film_details = spark.table(bronze_tables["film_details"]).alias("d")
more_info = spark.table(bronze_tables["more_info"]).alias("mi")
poster_path = spark.table(bronze_tables["poster_path"]).alias("p")

def clean_money(col_name):
    cleaned = F.regexp_replace(F.coalesce(F.col(col_name).cast("string"), F.lit("")), r"[^0-9\.-]", "")
    return F.when(cleaned == "", None).otherwise(cleaned.cast("double"))

def safe_to_date(col_name):
    return F.coalesce(
        F.to_date(col_name, "yyyy-MM-dd"),
        F.to_date(col_name, "dd/MM/yyyy"),
        F.to_date(col_name, "MM/dd/yyyy"),
        F.to_date(col_name, "dd-MM-yyyy"),
        F.to_date(col_name, "yyyyMMdd"),
    )

def parse_runtime_to_minutes(col_name):
    value = F.coalesce(F.col(col_name).cast("string"), F.lit(""))
    hours = F.when(F.regexp_extract(value, r"(\d+)\s*h", 1) == "", None).otherwise(F.regexp_extract(value, r"(\d+)\s*h", 1)).cast("int")
    minutes = F.when(F.regexp_extract(value, r"(\d+)\s*min", 1) == "", None).otherwise(F.regexp_extract(value, r"(\d+)\s*min", 1)).cast("int")
    only_minutes = F.when(F.regexp_extract(value, r"^(\d+)$", 1) == "", None).otherwise(F.regexp_extract(value, r"^(\d+)$", 1)).cast("int")
    total = F.coalesce(hours, F.lit(0)) * F.lit(60) + F.coalesce(minutes, F.lit(0))
    return F.when(only_minutes.isNotNull(), only_minutes).otherwise(F.when(total > 0, total))

audit_cols = ["_ingestion_ts", "_record_hash", "_pipeline_run_ts", "_source_file", "_load_date"]

movies_silver = (
    movies
    .select(
        F.col("id").cast("int").alias("movie_id"),
        standardize_text(F.col("title")).alias("title"),
        standardize_text(F.col("genres")).alias("genres"),
        F.lower(standardize_text(F.col("language"))).alias("language_code"),
        F.col("user_score").cast("double").alias("user_score"),
        F.col("runtime_hour").cast("int").alias("runtime_hour"),
        F.col("runtime_min").cast("int").alias("runtime_min"),
        safe_to_date("release_date").alias("release_date"),
        F.col("vote_count").cast("int").alias("vote_count"),
        *select_existing_audit_columns(movies),
    )
    .withColumn(
        "runtime_total_min",
        F.when(
            F.col("runtime_hour").isNotNull() | F.col("runtime_min").isNotNull(),
            (F.coalesce(F.col("runtime_hour"), F.lit(0)) * F.lit(60)) + F.coalesce(F.col("runtime_min"), F.lit(0)),
        ),
    )
    .withColumn("record_status", F.when(F.col("title").isNull() | (F.trim(F.col("title")) == ""), F.lit("INVALID")).otherwise(F.lit("VALID")))
    .dropDuplicates(["movie_id"])
)

film_details_silver = (
    film_details
    .select(
        F.col("id").cast("int").alias("movie_id"),
        standardize_text(F.col("director")).alias("director_name"),
        standardize_text(F.col("top_billed")).alias("top_billed"),
        F.col("budget_usd").cast("double").alias("budget_usd_details"),
        F.col("revenue_usd").cast("double").alias("revenue_usd_details"),
        *select_existing_audit_columns(film_details),
    )
    .dropDuplicates(["movie_id"])
)

more_info_silver = (
    more_info
    .select(
        F.col("id").cast("int").alias("more_info_id"),
        F.col("film_id").cast("int").alias("movie_id"),
        standardize_text(F.col("runtime")).alias("runtime_raw"),
        clean_money("budget").alias("budget_usd_moreinfo"),
        clean_money("revenue").alias("revenue_usd_moreinfo"),
        parse_runtime_to_minutes("runtime").alias("runtime_from_moreinfo_min"),
        *select_existing_audit_columns(more_info),
    )
    .dropDuplicates(["movie_id"])
)

poster_silver = (
    poster_path
    .select(
        F.col("id").cast("int").alias("movie_id"),
        standardize_text(F.col("poster_path")).alias("poster_path"),
        standardize_text(F.col("backdrop_path")).alias("backdrop_path"),
        *select_existing_audit_columns(poster_path),
    )
    .dropDuplicates(["movie_id"])
)

movie_conformed = (
    movies_silver.alias("m")
    .join(film_details_silver.drop(*audit_cols).alias("d"), on="movie_id", how="left")
    .join(more_info_silver.drop(*audit_cols).alias("mi"), on="movie_id", how="left")
    .join(poster_silver.drop(*audit_cols).alias("p"), on="movie_id", how="left")
    .withColumn("budget_usd_final", F.coalesce(F.col("budget_usd_details"), F.col("budget_usd_moreinfo")))
    .withColumn("revenue_usd_final", F.coalesce(F.col("revenue_usd_details"), F.col("revenue_usd_moreinfo")))
    .withColumn("runtime_total_min_final", F.coalesce(F.col("runtime_total_min"), F.col("runtime_from_moreinfo_min")))
    .withColumn(
        "budget_source",
        F.when(F.col("budget_usd_details").isNotNull(), F.lit("film_details"))
         .when(F.col("budget_usd_moreinfo").isNotNull(), F.lit("more_info"))
         .otherwise(F.lit("not_available"))
    )
    .withColumn(
        "revenue_source",
        F.when(F.col("revenue_usd_details").isNotNull(), F.lit("film_details"))
         .when(F.col("revenue_usd_moreinfo").isNotNull(), F.lit("more_info"))
         .otherwise(F.lit("not_available"))
    )
    .withColumn(
        "runtime_source",
        F.when(F.col("runtime_total_min").isNotNull(), F.lit("movies"))
         .when(F.col("runtime_from_moreinfo_min").isNotNull(), F.lit("more_info"))
         .otherwise(F.lit("not_available"))
    )
    .withColumn("profit_usd", F.when(F.col("budget_usd_final").isNotNull() & F.col("revenue_usd_final").isNotNull(), F.col("revenue_usd_final") - F.col("budget_usd_final")))
    .withColumn(
        "roi_pct",
        F.when(
            (F.col("budget_usd_final") > 0) & F.col("revenue_usd_final").isNotNull(),
            ((F.col("revenue_usd_final") - F.col("budget_usd_final")) / F.col("budget_usd_final")) * F.lit(100.0),
        ),
    )
    .withColumn(
        "margin_pct",
        F.when(
            (F.col("revenue_usd_final") > 0) & F.col("budget_usd_final").isNotNull(),
            ((F.col("revenue_usd_final") - F.col("budget_usd_final")) / F.col("revenue_usd_final")) * F.lit(100.0),
        ),
    )
    .withColumn("has_poster", F.when(F.col("poster_path").isNotNull() & (F.trim(F.col("poster_path")) != ""), F.lit(1)).otherwise(F.lit(0)))
    .withColumn("has_backdrop", F.when(F.col("backdrop_path").isNotNull() & (F.trim(F.col("backdrop_path")) != ""), F.lit(1)).otherwise(F.lit(0)))
    .withColumn(
        "runtime_validation_flag",
        F.when(
            F.col("runtime_total_min").isNotNull()
            & F.col("runtime_from_moreinfo_min").isNotNull()
            & (F.abs(F.col("runtime_total_min") - F.col("runtime_from_moreinfo_min")) > 5),
            F.lit("MISMATCH"),
        ).otherwise(F.lit("OK")),
    )
    .withColumn(
        "financial_completeness_flag",
        F.when(F.col("budget_usd_final").isNull() | F.col("revenue_usd_final").isNull(), F.lit("INCOMPLETE"))
         .otherwise(F.lit("COMPLETE"))
    )
)

movie_genres_exploded = (
    movies_silver
    .select("movie_id", F.explode_outer(F.split(F.coalesce(F.col("genres"), F.lit("")), r",\s*")).alias("genre_name"))
    .withColumn("genre_name", standardize_text(F.col("genre_name")))
    .filter(F.col("genre_name").isNotNull() & (F.col("genre_name") != ""))
    .dropDuplicates(["movie_id", "genre_name"])
)

for name, df in {
    "movies_silver": movies_silver,
    "film_details_silver": film_details_silver,
    "more_info_silver": more_info_silver,
    "poster_silver": poster_silver,
    "movie_conformed": movie_conformed,
    "movie_genres_exploded": movie_genres_exploded,
}.items():
    save_delta_table(df, silver_tables[name])

print("Proceso bronze -> silver completado correctamente.")
