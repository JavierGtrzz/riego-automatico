"""Fuente 2 (API externa): clima de Querétaro con Open-Meteo (sin API key).

Guarda un registro cada INTERVALO_CLIMA_S en data/raw/clima/*.jsonl
"""
import logging
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402
from ingesta.jsonl_writer import JsonlBatchWriter, ahora_iso  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("clima")

URL = "https://api.open-meteo.com/v1/forecast"
PARAMS = {
    "latitude": config.LATITUD,
    "longitude": config.LONGITUD,
    # "current" no admite radiación: se toma de la serie horaria
    "current": "temperature_2m,relative_humidity_2m,precipitation,cloud_cover,wind_speed_10m",
    "hourly": "precipitation_probability,shortwave_radiation",
    "forecast_days": 1,
    "timezone": "UTC",
}


def valor_horario(data: dict, variable: str, hora_actual: str):
    """Valor de la serie horaria para la hora de 'current.time' (ej. 2026-11-05T18)."""
    try:
        horas = data["hourly"]["time"]
        idx = next(i for i, t in enumerate(horas) if t[:13] == hora_actual[:13])
        return data["hourly"][variable][idx]
    except (KeyError, StopIteration, IndexError):
        return None


def obtener_clima() -> dict:
    r = requests.get(URL, params=PARAMS, timeout=15)
    r.raise_for_status()
    data = r.json()
    cur = data.get("current", {})
    hora = cur.get("time", "")
    return {
        "ts": ahora_iso(),
        "temperatura_c": cur.get("temperature_2m"),
        "humedad_rel": cur.get("relative_humidity_2m"),
        "precipitacion_mm": cur.get("precipitation"),
        "nubosidad_pct": cur.get("cloud_cover"),
        "viento_kmh": cur.get("wind_speed_10m"),
        "prob_lluvia_pct": valor_horario(data, "precipitation_probability", hora),
        "radiacion_wm2": valor_horario(data, "shortwave_radiation", hora),
    }


def main() -> None:
    writer = JsonlBatchWriter(config.RAW_CLIMA, "clima", flush_cada_s=0)
    log.info("Ingesta de clima iniciada (cada %d s). Ctrl+C para salir.", config.INTERVALO_CLIMA_S)
    try:
        while True:
            try:
                registro = obtener_clima()
                writer.add(registro)
                writer.flush()
                log.info("Clima: %s", registro)
            except requests.RequestException as e:
                log.warning("No se pudo obtener el clima: %s", e)
            time.sleep(config.INTERVALO_CLIMA_S)
    except KeyboardInterrupt:
        log.info("Saliendo...")


if __name__ == "__main__":
    main()
