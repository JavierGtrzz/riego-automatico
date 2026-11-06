"""Lee el Arduino por serial, decide el riego (igual que antes) y registra todo.

Fuente 1 (IoT): lecturas de humedad  -> data/raw/sensor/*.jsonl
Fuente 3 (logs): eventos de riego    -> data/raw/riego/*.jsonl
"""
import logging
import sys
import time
from pathlib import Path

import serial

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402
from ingesta.jsonl_writer import JsonlBatchWriter, ahora_iso  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingesta")


def main() -> None:
    w_sensor = JsonlBatchWriter(config.RAW_SENSOR, "sensor", config.FLUSH_SENSOR_S)
    w_riego = JsonlBatchWriter(config.RAW_RIEGO, "riego", config.FLUSH_RIEGO_S)

    arduino = serial.Serial(config.PUERTO_SERIAL, config.BAUDIOS, timeout=3)
    time.sleep(2)                  # el Arduino se reinicia al abrir el puerto
    arduino.reset_input_buffer()
    log.info("Ingesta iniciada en %s. Ctrl+C para salir.", config.PUERTO_SERIAL)

    ultimo_riego = 0.0
    try:
        while True:
            linea = arduino.readline().decode("utf-8", errors="ignore").strip()
            w_sensor.flush_si_toca()
            w_riego.flush_si_toca()
            if not linea:
                continue

            if linea.startswith("H:"):
                try:
                    humedad = float(linea[2:])
                except ValueError:
                    log.warning("Lectura inválida: %r", linea)
                    continue

                w_sensor.add({
                    "ts": ahora_iso(),
                    "planta": config.PLANTA,
                    "humedad": humedad,
                })
                log.info("Humedad: %.1f%%", humedad)

                toca_regar = humedad < config.UMBRAL_HUMEDAD
                paso_espera = time.time() - ultimo_riego >= config.ESPERA_ENTRE_RIEGOS
                if toca_regar and paso_espera:
                    arduino.write(f"R{config.SEGUNDOS_RIEGO}\n".encode())
                    ultimo_riego = time.time()
                    w_riego.add({
                        "ts": ahora_iso(),
                        "planta": config.PLANTA,
                        "evento": "RIEGO_CMD",
                        "duracion_s": config.SEGUNDOS_RIEGO,
                        "humedad_previa": humedad,
                    })
                    log.info("-> Riego solicitado (%d s)", config.SEGUNDOS_RIEGO)

            elif linea.startswith("EVT:"):
                w_riego.add({
                    "ts": ahora_iso(),
                    "planta": config.PLANTA,
                    "evento": linea[4:],      # RIEGO_ON / RIEGO_OFF
                    "duracion_s": None,
                    "humedad_previa": None,
                })
                log.info("Evento del Arduino: %s", linea)
            else:
                log.debug("Línea ignorada: %r", linea)

    except KeyboardInterrupt:
        log.info("Saliendo...")
    finally:
        w_sensor.flush()
        w_riego.flush()
        arduino.close()


if __name__ == "__main__":
    main()
