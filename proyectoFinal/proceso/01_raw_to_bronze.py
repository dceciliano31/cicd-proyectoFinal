# Databricks notebook source
# MAGIC %run "/Shared/proyectoFinal/proceso/00_config"

# COMMAND ----------

from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_catalog}")

RAW_SCHEMAS = {
    "movies": T.StructType([
        T.StructField("id", T.IntegerType(), False),
        T.StructField("title", T.StringType(), True),
        T.StructField("genres", T.StringType(), True),
        T.StructField("language", T.StringType(), True),
        T.StructField("user_score", T.DoubleType(), True),
        T.StructField("runtime_hour", T.IntegerType(), True),
        T.StructField("runtime_min", T.IntegerType(), True),
        T.StructField("release_date", T.StringType(), True),
        T.StructField("vote_count", T.IntegerType(), True),
    ]),
    "film_details": T.StructType([
        T.StructField("id", T.IntegerType(), False),
        T.StructField("director", T.StringType(), True),
        T.StructField("top_billed", T.StringType(), True),
        T.StructField("budget_usd", T.DoubleType(), True),
        T.StructField("revenue_usd", T.DoubleType(), True),
    ]),
    "more_info": T.StructType([
        T.StructField("id", T.IntegerType(), False),
        T.StructField("runtime", T.StringType(), True),
        T.StructField("budget", T.StringType(), True),
        T.StructField("revenue", T.StringType(), True),
        T.StructField("film_id", T.IntegerType(), True),
    ]),
    "poster_path": T.StructType([
        T.StructField("id", T.IntegerType(), False),
        T.StructField("poster_path", T.StringType(), True),
        T.StructField("backdrop_path", T.StringType(), True),
    ]),
}

def read_raw_csv(table_name: str, file_path: str):
    schema = RAW_SCHEMAS[table_name]
    return (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(file_path)
        .toDF(*[to_snake_case(c) for c in schema.fieldNames()])
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_load_date", F.current_date())
        .withColumn("_pipeline_run_ts", F.current_timestamp())
    )

def add_record_hash(df):
    business_cols = [c for c in df.columns if not c.startswith("_")]
    return df.withColumn(
        "_record_hash",
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in business_cols]), 256),
    )

def enforce_basic_quality(df, table_name: str):
    null_id_count = df.filter(F.col("id").isNull()).count()
    if null_id_count > 0:
        raise ValueError(f"La tabla {table_name} contiene {null_id_count} registros con id nulo.")
    return df

def deduplicate_latest(df, business_key: str = "id"):
    w = Window.partitionBy(business_key).orderBy(F.col("_ingestion_ts").desc(), F.col("_record_hash").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

for table_name, file_path in source_files.items():
    print(f"[RAW -> BRONZE] Procesando {table_name} desde {file_path}")
    df = read_raw_csv(table_name, file_path)
    df = add_record_hash(df)
    df = enforce_basic_quality(df, table_name)
    df = deduplicate_latest(df, "id")
    save_delta_table(df, bronze_tables[table_name])

print("Proceso raw -> bronze completado correctamente.")
