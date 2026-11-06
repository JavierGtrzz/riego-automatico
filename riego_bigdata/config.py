"""Configuración compartida por ingesta, Spark y dashboard."""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

# Raw zone: JSONL tal como llega de cada fuente
RAW_SENSOR = DATA_DIR / "raw" / "sensor"
RAW_RIEGO = DATA_DIR / "raw" / "riego"
RAW_CLIMA = DATA_DIR / "raw" / "clima"

# Curated zone: Parquet limpio producido por Spark
CURATED = DATA_DIR / "curated"
CURATED_VENTANAS = CURATED / "humedad_ventanas"
CURATED_HORARIO = CURATED / "resumen_horario"
CURATED_DIARIO = CURATED / "resumen_diario"

CHECKPOINTS = DATA_DIR / "checkpoints"

# Arduino (el sketch se carga a mano; este proceso solo abre el puerto)
# En Docker el dispositivo del host se monta en /dev/ttyACM0.
PUERTO_SERIAL = os.environ.get("PUERTO_SERIAL", "/dev/ttyACM0")
BAUDIOS = int(os.environ.get("BAUDIOS", "9600"))

# Reglas de riego (las mismas del script anterior)
PLANTA = "lechuga"
UMBRAL_HUMEDAD = 60.0       # % mínimo; por debajo se riega
SEGUNDOS_RIEGO = 3
ESPERA_ENTRE_RIEGOS = 120   # s

# Consumo de agua: MIDE el tuyo (ver README, sección de calibración)
CAUDAL_L_MIN = 1.0          # litros por minuto con la bomba encendida

# Escritura de archivos JSONL
FLUSH_SENSOR_S = 30         # cada cuántos segundos se cierra un archivo de sensor
FLUSH_RIEGO_S = 5

# Clima (Open-Meteo, sin API key). Coordenadas de Querétaro.
LATITUD = 20.5888
LONGITUD = -100.3899
INTERVALO_CLIMA_S = 600     # 10 min

# Alertas del dashboard
SENSOR_OFFLINE_S = 120      # sin lecturas en 2 min => alerta
SEQUIA_HORAS = 6            # humedad bajo umbral durante estas horas => alerta
ZONA_HORARIA = "America/Mexico_City"


def sesion_spark(nombre: str):
    """Spark local. En un contenedor el driver tiene que atarse a localhost."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(nombre)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "768m"))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )
