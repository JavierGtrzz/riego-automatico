"""Genera un histórico SIMULADO para probar Spark y el dashboard sin esperar días.

Simula: temperatura con ciclo diario, suelo que se seca más rápido con calor,
y riego automático con las mismas reglas que riego_ingesta.py.

Uso:
    python3 tools/generar_datos_demo.py --dias 14
    python3 tools/generar_datos_demo.py --dias 14 --limpiar   # borra data/raw antes
"""
import argparse
import json
import math
import random
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402

PASO_S = 120   # una lectura cada 2 min


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def escribir(directorio: Path, prefijo: str, dia: str, registros: list) -> None:
    directorio.mkdir(parents=True, exist_ok=True)
    with open(directorio / f"{prefijo}_{dia}_demo.jsonl", "w", encoding="utf-8") as f:
        for r in registros:
            f.write(json.dumps(r) + "\n")


OFFSETS: dict = {}   # variación de temperatura de un día a otro


def temperatura(dt_utc: datetime) -> float:
    """Ciclo diario (mín. de madrugada, máx. por la tarde, hora local) + variación por día."""
    hora_local = (dt_utc.hour - 6 + dt_utc.minute / 60) % 24
    offset = OFFSETS.setdefault(dt_utc.date(), random.uniform(-4, 5))
    return 20 + offset + 9 * math.sin((hora_local - 9) / 24 * 2 * math.pi) + random.gauss(0, 0.4)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dias", type=int, default=14)
    ap.add_argument("--limpiar", action="store_true")
    args = ap.parse_args()
    random.seed(42)

    if args.limpiar:
        shutil.rmtree(config.DATA_DIR / "raw", ignore_errors=True)

    fin = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    inicio = fin - timedelta(days=args.dias)

    humedad = 70.0
    ultimo_riego = inicio - timedelta(hours=1)
    sensor_dia, riego_dia, clima_dia = [], [], []
    dia_actual = inicio.strftime("%Y%m%d")
    t = inicio
    n_sensor = n_riego = 0

    while t < fin:
        dia = t.strftime("%Y%m%d")
        if dia != dia_actual:
            escribir(config.RAW_SENSOR, "sensor", dia_actual, sensor_dia)
            escribir(config.RAW_RIEGO, "riego", dia_actual, riego_dia)
            escribir(config.RAW_CLIMA, "clima", dia_actual, clima_dia)
            sensor_dia, riego_dia, clima_dia = [], [], []
            dia_actual = dia

        temp = temperatura(t)
        # Más calor => el suelo pierde humedad más rápido
        humedad -= 0.04 * max(temp, 5) / 20 * (PASO_S / 60) + random.gauss(0, 0.05)
        humedad = max(humedad, 0)

        sensor_dia.append({"ts": iso(t), "planta": config.PLANTA, "humedad": round(humedad, 1)})
        n_sensor += 1

        if humedad < config.UMBRAL_HUMEDAD and (t - ultimo_riego).total_seconds() >= config.ESPERA_ENTRE_RIEGOS:
            riego_dia.append({"ts": iso(t), "planta": config.PLANTA, "evento": "RIEGO_CMD",
                              "duracion_s": config.SEGUNDOS_RIEGO, "humedad_previa": round(humedad, 1)})
            riego_dia.append({"ts": iso(t + timedelta(seconds=1)), "planta": config.PLANTA,
                              "evento": "RIEGO_ON", "duracion_s": None, "humedad_previa": None})
            riego_dia.append({"ts": iso(t + timedelta(seconds=1 + config.SEGUNDOS_RIEGO)), "planta": config.PLANTA,
                              "evento": "RIEGO_OFF", "duracion_s": None, "humedad_previa": None})
            humedad += random.uniform(8, 14)      # el riego sube la humedad
            ultimo_riego = t
            n_riego += 1

        # Clima: un registro cada 10 min
        if t.minute % 10 == 0 and t.second == 0:
            hora_local = (t.hour - 6) % 24
            clima_dia.append({
                "ts": iso(t),
                "temperatura_c": round(temp, 1),
                "humedad_rel": round(max(10, min(95, 70 - 2 * (temp - 20) + random.gauss(0, 3))), 0),
                "precipitacion_mm": round(max(0, random.gauss(-2.0, 0.8)), 1),
                "nubosidad_pct": random.randint(0, 100),
                "viento_kmh": round(abs(random.gauss(8, 3)), 1),
                "prob_lluvia_pct": random.choice([0, 0, 5, 10, 30, 60]),
                "radiacion_wm2": round(max(0, 800 * math.sin((hora_local - 6) / 12 * math.pi)), 0)
                if 6 <= hora_local <= 18 else 0,
            })
        t += timedelta(seconds=PASO_S)

    escribir(config.RAW_SENSOR, "sensor", dia_actual, sensor_dia)
    escribir(config.RAW_RIEGO, "riego", dia_actual, riego_dia)
    escribir(config.RAW_CLIMA, "clima", dia_actual, clima_dia)
    print(f"Listo: {n_sensor} lecturas y {n_riego} riegos simulados en {args.dias} días -> {config.DATA_DIR / 'raw'}")


if __name__ == "__main__":
    main()
