"""PySpark batch sobre todo el histórico de la Raw Zone.

Cruza humedad del suelo + eventos de riego + clima y produce:
  data/curated/resumen_horario  (Parquet, particionado por fecha)
  data/curated/resumen_diario   (Parquet)

Responde: ¿cuántos litros de agua se consumieron por cada grado de
temperatura ambiental en el periodo analizado?

Uso:
    python3 spark/batch_analisis.py
"""
import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402

SCHEMA_SENSOR = StructType([
    StructField("ts", StringType()),
    StructField("planta", StringType()),
    StructField("humedad", DoubleType()),
])
SCHEMA_RIEGO = StructType([
    StructField("ts", StringType()),
    StructField("planta", StringType()),
    StructField("evento", StringType()),
    StructField("duracion_s", IntegerType()),
    StructField("humedad_previa", DoubleType()),
])
SCHEMA_CLIMA = StructType([
    StructField("ts", StringType()),
    StructField("temperatura_c", DoubleType()),
    StructField("humedad_rel", DoubleType()),
    StructField("precipitacion_mm", DoubleType()),
    StructField("nubosidad_pct", DoubleType()),
    StructField("viento_kmh", DoubleType()),
    StructField("prob_lluvia_pct", DoubleType()),
    StructField("radiacion_wm2", DoubleType()),
])


def leer(spark, schema, ruta: Path):
    ruta.mkdir(parents=True, exist_ok=True)
    return (
        spark.read.schema(schema).json(str(ruta))
        .withColumn("ts", F.col("ts").cast("timestamp"))
        .filter(F.col("ts").isNotNull())
    )


def main() -> None:
    spark = (
        config.sesion_spark("riego-batch")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Limpieza de cada fuente ---
    sensor = (
        leer(spark, SCHEMA_SENSOR, config.RAW_SENSOR)
        .filter(F.col("humedad").isNotNull() & F.col("humedad").between(0, 100))
        .dropDuplicates(["ts", "planta"])
    )
    riego = leer(spark, SCHEMA_RIEGO, config.RAW_RIEGO).dropDuplicates(["ts", "planta", "evento"])
    clima = leer(spark, SCHEMA_CLIMA, config.RAW_CLIMA)

    if sensor.rdd.isEmpty():
        print("No hay lecturas del sensor en data/raw/sensor. Corre la ingesta o tools/generar_datos_demo.py")
        return

    hora = F.date_trunc("hour", F.col("ts")).alias("hora")

    # --- Agregaciones por hora ---
    sensor_h = sensor.groupBy(hora, "planta").agg(
        F.round(F.avg("humedad"), 2).alias("humedad_prom"),
        F.min("humedad").alias("humedad_min"),
        F.max("humedad").alias("humedad_max"),
        F.count("*").alias("n_lecturas"),
    )

    riego_h = (
        riego.filter(F.col("evento") == "RIEGO_CMD")
        .groupBy(hora, "planta")
        .agg(
            F.count("*").alias("n_riegos"),
            F.sum("duracion_s").alias("segundos_riego"),
        )
        .withColumn("litros", F.round(F.col("segundos_riego") / 60.0 * config.CAUDAL_L_MIN, 3))
    )

    clima_h = clima.groupBy(hora).agg(
        F.round(F.avg("temperatura_c"), 2).alias("temp_prom"),
        F.round(F.avg("humedad_rel"), 2).alias("humedad_rel_prom"),
        F.max("precipitacion_mm").alias("precipitacion_mm"),
        F.round(F.avg("prob_lluvia_pct"), 1).alias("prob_lluvia_prom"),
        F.round(F.avg("radiacion_wm2"), 1).alias("radiacion_prom"),
    )

    # --- Integración de las 3 fuentes ---
    horario = (
        sensor_h.join(riego_h, ["hora", "planta"], "left")
        .join(clima_h, ["hora"], "left")
        .fillna({"n_riegos": 0, "segundos_riego": 0, "litros": 0.0})
        .withColumn("fecha", F.to_date("hora"))
        .orderBy("hora")
    )
    horario.cache()

    (horario.write.mode("overwrite").partitionBy("fecha").parquet(str(config.CURATED_HORARIO)))

    # --- Resumen diario ---
    diario = (
        horario.groupBy("fecha", "planta")
        .agg(
            F.round(F.avg("humedad_prom"), 2).alias("humedad_prom"),
            F.sum("n_riegos").alias("n_riegos"),
            F.round(F.sum("litros"), 3).alias("litros"),
            F.round(F.avg("temp_prom"), 2).alias("temp_prom"),
            F.round(F.sum("precipitacion_mm"), 2).alias("lluvia_mm"),
            F.count("*").alias("horas_con_datos"),
        )
        .withColumn(
            "litros_por_grado",
            F.when(F.col("temp_prom") > 0, F.round(F.col("litros") / F.col("temp_prom"), 4)),
        )
        .orderBy("fecha")
    )
    diario.write.mode("overwrite").parquet(str(config.CURATED_DIARIO))

    # --- Respuestas analíticas en consola ---
    print("\n=== Resumen diario (últimos 10 días) ===")
    diario.orderBy(F.col("fecha").desc()).limit(10).orderBy("fecha").show(truncate=False)

    tot = diario.agg(
        F.sum("litros").alias("litros"),
        F.avg("temp_prom").alias("temp"),
        F.countDistinct("fecha").alias("dias"),
    ).first()
    if tot["litros"] is not None and tot["temp"]:
        print(f"Periodo: {tot['dias']} días | Agua total estimada: {tot['litros']:.2f} L | "
              f"Temperatura media: {tot['temp']:.1f} °C")
        print(f"-> Consumo por grado de temperatura: {tot['litros'] / tot['temp']:.3f} L/°C")

    pares = horario.filter(F.col("temp_prom").isNotNull()).count()
    if pares >= 10:
        corr = horario.stat.corr("temp_prom", "humedad_prom")
        print(f"Correlación temperatura vs humedad del suelo: {corr:.2f} "
              "(negativa = más calor, suelo más seco)")
        completos = diario.filter(F.col("temp_prom").isNotNull() & (F.col("horas_con_datos") >= 20))
        corr_l = completos.stat.corr("temp_prom", "litros")
        print(f"Correlación temperatura vs litros por día (solo días completos): {corr_l:.2f} "
              "(positiva = más calor, más agua). Como el sistema mantiene la humedad "
              "cerca del umbral, esta métrica suele ser más reveladora que la anterior.")
    else:
        print("Aún hay pocas horas con clima para calcular correlación (se necesitan >= 10).")

    spark.stop()


if __name__ == "__main__":
    main()
