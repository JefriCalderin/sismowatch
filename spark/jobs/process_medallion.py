"""
Job PySpark: Procesamiento Distribuido de la Arquitectura Medallón (Bronze -> Silver -> Gold).
Diseñado para ser ejecutado mediante spark-submit en el clúster Apache Spark.
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_unixtime, udf, to_date, hour, count, max as spark_max,
    avg as spark_avg, min as spark_min, when, lit, current_timestamp
)
from pyspark.sql.types import StringType, DoubleType, BooleanType, IntegerType

def create_spark_session():
    """Inicializa sesión distribuida de Spark con soporte para PostgreSQL JDBC."""
    return SparkSession.builder \
        .appName("SismoWatch-Medallion-Processing") \
        .getOrCreate()

def run_spark_pipeline():
    spark = create_spark_session()
    postgres_host = os.getenv("POSTGRES_HOST", "postgres")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")
    postgres_db = os.getenv("POSTGRES_DB", "sismowatch")
    user = os.getenv("DB_TRANSFORM_USER", "role_transform")
    password = os.getenv("DB_TRANSFORM_PASSWORD", "TransformSecret2026!")

    jdbc_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
    connection_properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }

    print(">>> [Spark Job] Leyendo capa Bronze desde PostgreSQL...")
    try:
        raw_df = spark.read.jdbc(
            url=jdbc_url,
            table="bronze.earthquakes_raw",
            properties=connection_properties
        )
        print(f">>> [Spark Job] Registros leídos de Bronze: {raw_df.count()}")

        # Transformación a Silver
        silver_df = raw_df.select(
            col("id"),
            col("properties_mag").cast(DoubleType()).alias("magnitude"),
            col("properties_place").alias("place"),
            col("properties_place").alias("sanitized_place"),
            from_unixtime(col("properties_time") / 1000).cast("timestamp").alias("event_time"),
            from_unixtime(col("properties_updated") / 1000).cast("timestamp").alias("updated_time"),
            col("longitude").cast(DoubleType()),
            col("latitude").cast(DoubleType()),
            col("depth_km").cast(DoubleType()),
            (col("properties_tsunami") > 0).alias("tsunami_alert"),
            col("properties_sig").cast(IntegerType()).alias("significance"),
            col("properties_status").alias("status"),
            col("properties_magType").alias("magnitude_type"),
            col("properties_type").alias("event_type"),
            col("properties_url").alias("url"),
            lit("Global").alias("region"),
            when(col("properties_mag") < 4.0, "Menor")
                .when(col("properties_mag") < 6.0, "Moderado")
                .otherwise("Fuerte").alias("magnitude_category"),
            when(col("depth_km") <= 70.0, "Superficial")
                .otherwise("Profundo").alias("depth_category"),
            lit(True).alias("is_valid")
        )

        print(">>> [Spark Job] Escribiendo en Capa Silver...")
        silver_df.write.mode("append").jdbc(
            url=jdbc_url,
            table="silver.earthquakes_cleaned",
            properties=connection_properties
        )

        print(">>> [Spark Job] Transformación Medallón con Apache Spark completada exitosamente.")
    except Exception as exc:
        print(f">>> [Spark Job] Error o advertencia durante ejecución Spark: {exc}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_spark_pipeline()
