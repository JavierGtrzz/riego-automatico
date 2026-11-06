"""Escribe registros en archivos JSONL NUEVOS cada cierto tiempo.

Spark Structured Streaming procesa cada archivo una sola vez: si le añades
líneas a un archivo que ya vio, no las lee. Por eso aquí se acumulan los
registros en memoria y se escribe un archivo nuevo por lote. El archivo se
crea oculto (prefijo ".tmp_") y luego se renombra, para que Spark nunca lea
un archivo a medio escribir.
"""
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path


def ahora_iso() -> str:
    """Timestamp UTC en formato ISO 8601 con segundos (ej. 2026-11-05T18:30:00Z)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class JsonlBatchWriter:
    def __init__(self, directorio: Path, prefijo: str, flush_cada_s: float):
        self.directorio = Path(directorio)
        self.directorio.mkdir(parents=True, exist_ok=True)
        self.prefijo = prefijo
        self.flush_cada_s = flush_cada_s
        self.buffer: list[dict] = []
        self.ultimo_flush = time.time()
        self.contador = 0

    def add(self, registro: dict) -> None:
        self.buffer.append(registro)

    def flush_si_toca(self) -> None:
        if time.time() - self.ultimo_flush >= self.flush_cada_s:
            self.flush()

    def flush(self) -> None:
        self.ultimo_flush = time.time()
        if not self.buffer:
            return
        sello = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        self.contador += 1
        nombre = f"{self.prefijo}_{sello}_{self.contador:04d}.jsonl"
        tmp = self.directorio / f".tmp_{nombre}"
        final = self.directorio / nombre
        with open(tmp, "w", encoding="utf-8") as f:
            for r in self.buffer:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        os.replace(tmp, final)   # renombrado atómico
        self.buffer.clear()
