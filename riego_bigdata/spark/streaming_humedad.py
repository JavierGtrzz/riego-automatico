"""PySpark Structured Streaming sobre las lecturas del sensor.

Lee data/raw/sensor/*.jsonl conforme van apareciendo, limpia, calcula
promedio/mínimo/máximo de humedad en ventanas de 5 min (cada 1 min) y marca
anomalías. Escribe Parquet en data/curated/humedad_ventanas.

Uso:
    python3 spark/streaming_humedad.py            # escribe Parquet
    python3 spark/streaming_humedad.py --consola  # solo imprime (depuración)
"""
import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402

SCHEMA_SENSOR = StructType([
    StructField("ts", StringType()),
    StructField("planta", StringType()),
    StructField("humedad", DoubleType()),
])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--consola", action="store_true", help="imprime en consola en vez de escribir Parquet")
    args = ap.parse_args()

    config.RAW_SENSOR.mkdir(parents=True, exist_ok=True)

    spark = config.sesion_spark("riego-streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1) Ingesta: cada archivo nuevo es un micro-lote
    crudo = (
        spark.readStream.schema(SCHEMA_SENSOR)
        .option("maxFilesPerTrigger", 20)
        .json(str(config.RAW_SENSOR))
    )

    # 2) Limpieza: tipos, nulos y rango válido
    lecturas = (
        crudo.withColumn("ts", F.col("ts").cast("timestamp"))
        .filter(F.col("ts").isNotNull() & F.col("humedad").isNotNull())
        .filter(F.col("humedad").between(0, 100))
    )

    # 3) Agregación por ventana. El watermark limita cuánto esperamos datos tardíos.
    ventanas = (
        lecturas.withWatermark("ts", "2 minutes")
        .groupBy(F.window("ts", "5 minutes", "1 minute"), "planta")
        .agg(
            F.avg("humedad").alias("humedad_prom"),
            F.min("humedad").alias("humedad_min"),
            F.max("humedad").alias("humedad_max"),
            F.count("*").alias("n_lecturas"),
        )
        .select(
            F.col("window.start").alias("ventana_inicio"),
            F.col("window.end").alias("ventana_fin"),
            "planta",
            F.round("humedad_prom", 2).alias("humedad_prom"),
            "humedad_min",
            "humedad_max",
            "n_lecturas",
        )
        .withColumn(
            "anomalia",
            F.when(F.col("humedad_prom") <= 1, "posible_sensor_desconectado")
            .when(F.col("humedad_max") - F.col("humedad_min") > 30, "cambio_brusco")
            .otherwise("ok"),
        )
    )

    if args.consola:
        # En 'update' se ve cada ventana apenas cambia, sin esperar al watermark
        query = (
            ventanas.writeStream.outputMode("update")
            .format("console")
            .option("truncate", False)
            .trigger(processingTime="10 seconds")
            .start()
        )
    else:
        # En 'append' cada ventana se escribe UNA vez, cuando el watermark la cierra
        query = (
            ventanas.writeStream.outputMode("append")
            .format("parquet")
            .option("path", str(config.CURATED_VENTANAS))
            .option("checkpointLocation", str(config.CHECKPOINTS / "humedad_ventanas"))
            .trigger(processingTime="30 seconds")
            .start()
        )

    print("Streaming activo. Ctrl+C para detener.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
