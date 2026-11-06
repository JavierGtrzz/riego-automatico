# Riego inteligente · Plataforma de analítica con PySpark

Escala tu sistema de riego (Arduino + Raspberry Pi) a una plataforma de datos:
ingesta de 3 fuentes, procesamiento en streaming y batch con PySpark,
almacenamiento en Parquet y un dashboard ejecutivo.

## 1. Arquitectura

```
Arduino ──USB──► riego_ingesta.py ──► data/raw/sensor/*.jsonl  ─┐  (Fuente 1: IoT)
                 (decide y riega)  ──► data/raw/riego/*.jsonl   ─┤  (Fuente 3: logs)
Open-Meteo ────► clima_ingesta.py  ──► data/raw/clima/*.jsonl   ─┤  (Fuente 2: API)
                                                                  │
                          ┌───────────────────────────────────────┘
                          ▼
        spark/streaming_humedad.py  ──► data/curated/humedad_ventanas/  (Parquet)
        spark/batch_analisis.py     ──► data/curated/resumen_horario/   (Parquet)
                                    ──► data/curated/resumen_diario/    (Parquet)
                          │
                          ▼
                dashboard/app.py (Streamlit)
```

- **Raw Zone** (`data/raw`): datos tal como llegan, en JSONL. Nunca se editan.
- **Curated Zone** (`data/curated`): datos limpios y agregados por Spark, en Parquet.
- Todos los timestamps se guardan en **UTC**; el dashboard los convierte a hora de México.

## 2. Estructura del proyecto

```
riego_bigdata/
├── config.py                  # rutas, umbrales, puerto serial, coordenadas
├── requirements.txt
├── arduino/riego.ino          # sin cambios: el que ya tienes
├── ingesta/
│   ├── jsonl_writer.py        # escritor por lotes compatible con Spark
│   ├── riego_ingesta.py       # serial + decisión de riego + logs
│   └── clima_ingesta.py       # API Open-Meteo
├── spark/
│   ├── streaming_humedad.py   # ventanas de 5 min + anomalías
│   └── batch_analisis.py      # histórico: humedad + riego + clima
├── dashboard/app.py           # KPIs, gráficos y alertas
└── tools/generar_datos_demo.py  # histórico simulado para probar
```

## 3. Dónde corre cada cosa

| Opción | Cuándo usarla |
|---|---|
| **A. Todo en la Raspberry Pi** | Pi 4 con 4 GB o más. Spark corre en modo local; es lento pero funciona. |
| **B. Ingesta en la Pi, Spark y dashboard en tu laptop** (recomendada) | Pi 3 o con poca RAM, o si quieres más fluidez. |

En la opción B, la Pi solo ejecuta `riego_ingesta.py` y `clima_ingesta.py`. Para llevar los datos a tu laptop:

```bash
# en la laptop, cada minuto o cuando quieras actualizar
rsync -av pi@IP_DE_LA_PI:~/riego_bigdata/data/raw/ ./data/raw/
```

## 4. Instalación

Para levantar clima, Spark y el dashboard con Docker, ve a la sección 12.
El sketch del Arduino se sigue cargando a mano.

### 4.1 En cualquier equipo que corra Spark o el dashboard

```bash
sudo apt update
sudo apt install -y default-jre-headless python3-venv   # Spark necesita Java 11 o 17
cd riego_bigdata
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
java -version     # verifica que Java esté instalado
```

> `pyspark==3.5.3` incluye Spark; no necesitas instalar Spark aparte.
> En la Pi, si solo vas a hacer la ingesta, basta con `pip install pyserial requests`.

### 4.2 En la Raspberry Pi (ingesta)

1. Confirma el puerto del Arduino: `ls /dev/tty*` (normalmente `/dev/ttyACM0`) y ajusta `PUERTO_SERIAL` en `config.py`.
2. Agrega tu usuario al grupo del puerto serial: `sudo usermod -aG dialout $USER` y reinicia sesión.
3. El Arduino ya debe tener cargado `arduino/riego.ino` (el mismo de antes).

## 5. Paso 0: calibra el consumo de agua

Todo el cálculo de litros depende de `CAUDAL_L_MIN` en `config.py` (viene en 1.0 como valor de ejemplo).
Para medirlo con tu bomba y tu manguera:

1. Conecta la manguera a la bomba y apunta la salida a un vaso medidor.
2. Envía el comando `R10` desde el Monitor Serie del Arduino IDE (10 segundos de riego).
3. Mide los mililitros que cayeron. Si fueron 150 ml en 10 s: `150 ml / 10 s × 60 = 900 ml/min = 0.9 L/min`.
4. Escribe ese valor en `CAUDAL_L_MIN`.

Sin este paso, los litros son estimaciones sin respaldo.

## 6. Prueba primero con datos simulados

Así validas Spark y el dashboard sin esperar días de datos reales:

```bash
python3 tools/generar_datos_demo.py --dias 14 --limpiar
python3 spark/batch_analisis.py
streamlit run dashboard/app.py
```

Antes de pasar a datos reales, borra los simulados: `rm -rf data/`.

## 7. Operación con datos reales

Abre una terminal por proceso (o usa `tmux`):

```bash
# Terminal 1 (Pi): ingesta del sensor y control del riego
python3 ingesta/riego_ingesta.py

# Terminal 2 (Pi): clima
python3 ingesta/clima_ingesta.py

# Terminal 3: streaming (deja correr)
python3 spark/streaming_humedad.py

# Terminal 4: batch (cuando quieras refrescar el histórico)
python3 spark/batch_analisis.py

# Terminal 5: dashboard
streamlit run dashboard/app.py
```

Para automatizar el batch, agrega con `crontab -e` una ejecución cada hora:

```
0 * * * * cd /home/pi/riego_bigdata && .venv/bin/python spark/batch_analisis.py >> batch.log 2>&1
```

Para depurar el streaming sin escribir Parquet: `python3 spark/streaming_humedad.py --consola`.

## 8. Qué hace cada módulo

**`riego_ingesta.py`**: es tu script anterior más el registro de datos. Cada lectura `H:` se guarda como
`{"ts", "planta", "humedad"}`. Cada orden de riego y cada confirmación del Arduino (`EVT:RIEGO_ON/OFF`)
se guarda como evento. La decisión de regar no cambió.

**`jsonl_writer.py`**: Spark procesa cada archivo una sola vez, así que no sirve añadir líneas a un archivo
existente. El escritor acumula registros y crea un archivo nuevo por lote (primero oculto y luego renombrado,
para que Spark no lea archivos a medio escribir).

**`clima_ingesta.py`**: consulta Open-Meteo cada 10 minutos (temperatura, humedad relativa, precipitación,
nubosidad, viento, probabilidad de lluvia y radiación solar). No requiere API key.

**`streaming_humedad.py`**: aplica Structured Streaming con limpieza (nulos y rango 0-100), un *watermark*
de 2 minutos y ventanas de 5 minutos que avanzan cada minuto. Calcula promedio, mínimo, máximo y marca anomalías:
`posible_sensor_desconectado` (promedio ≤ 1 %) y `cambio_brusco` (diferencia mayor a 30 puntos en la ventana).
En modo `append`, cada ventana se escribe cuando el watermark la cierra, así que verás los resultados con unos
minutos de retraso.

**`batch_analisis.py`**: une las tres fuentes por hora, calcula litros (`segundos de riego / 60 × caudal`) y
genera los resúmenes horario y diario. Imprime el consumo por grado de temperatura y las correlaciones.

**`dashboard/app.py`**: la sección de arriba (estado, alertas, gráfico de 6 h) se actualiza cada 10 s leyendo
los JSONL; la sección histórica lee el Parquet de Spark. Alertas incluidas: sensor sin datos, sequía prolongada,
falla del actuador (se pidió riego y el Arduino no confirmó) y anomalías detectadas por Spark.

## 9. Problemas frecuentes

| Síntoma | Causa y solución |
|---|---|
| `Permission denied: /dev/ttyACM0` | Falta el grupo `dialout` (sección 4.2). |
| `Device or resource busy` | El Monitor Serie del Arduino IDE está abierto: ciérralo. |
| Spark dice `JAVA_HOME is not set` o no encuentra Java | Instala `default-jre-headless` y revisa `java -version`. |
| El streaming no escribe Parquet | Espera unos minutos (watermark) o usa `--consola`. Verifica que existan archivos `.jsonl` no vacíos en `data/raw/sensor`. |
| Cambiaste el esquema o reinstalaste y Spark falla al arrancar | Borra `data/checkpoints/` y vuelve a lanzar el streaming (reprocesa todo el histórico). |
| El dashboard dice "Sin resúmenes" | Corre `python3 spark/batch_analisis.py` al menos una vez. |
| Clima vacío o con errores | Prueba la API a mano: `curl "https://api.open-meteo.com/v1/forecast?latitude=20.5888&longitude=-100.3899&current=temperature_2m"` |

## 10. Siguientes pasos

1. **Más plantas y sensores**: el campo `planta` ya está en todos los registros. Agrega más líneas `H:` con su nombre
   desde el Arduino y las agregaciones de Spark ya agrupan por planta.
2. **Broker de mensajes**: reemplaza los archivos por MQTT (Mosquitto) o Kafka. El job de Spark cambiaría solo
   en el `readStream`.
3. **Delta Lake**: sustituye Parquet por Delta para tener transacciones y *time travel* (requiere el paquete
   `delta-spark` compatible con tu versión de Spark).
4. **Umbrales dinámicos**: usa el histórico para ajustar `UMBRAL_HUMEDAD` según temperatura y probabilidad de lluvia.
5. **Notificaciones**: envía las alertas del dashboard a un bot de Telegram.

## 11. Alcance y límites

- Con un solo sensor, el volumen real de datos es pequeño (unas 43,000 lecturas al día con una cada 2 s, que son
  pocos MB). La arquitectura es de Big Data, pero para demostrar escala usa el generador con varios meses o multiplica
  plantas; no afirmes volúmenes que no tienes.
- Como el sistema riega cuando la humedad baja del umbral, la humedad del suelo se mantiene cerca de ese valor
  y su correlación con la temperatura sale débil. La relación se ve mejor en litros regados vs temperatura.
- Los litros son estimados a partir del tiempo de riego y un caudal nominal; no hay medidor de flujo.

## 12. Docker Compose

Los procesos de Python corren como servicios de un mismo compose y comparten
`riego_bigdata/data`. El sketch `arduino/riego.ino` no entra en la imagen:
se carga con el Arduino IDE y el puerto USB se pasa solo al servicio `riego`.

| Servicio | Qué ejecuta | Cuándo arranca |
|---|---|---|
| `clima` | `ingesta/clima_ingesta.py` | siempre |
| `streaming` | `spark/streaming_humedad.py` | siempre |
| `batch` | `spark/batch_analisis.py` cada hora | siempre |
| `dashboard` | Streamlit en el puerto 8501 | siempre |
| `riego` | `ingesta/riego_ingesta.py` | perfil `arduino`, con el USB conectado |

Desde la raíz del repositorio (donde está `docker-compose.yml`):

```bash
docker compose up -d --build
```

El panel queda en http://localhost:8501. La primera construcción tarda unos
minutos: la imagen trae Java 17 y PySpark.

Para probar sin esperar datos reales:

```bash
docker compose run --rm batch python tools/generar_datos_demo.py --dias 14 --limpiar
docker compose exec batch python spark/batch_analisis.py
```

El streaming de humedad tarda unos minutos en cerrar la primera ventana.

### Arduino por USB

En Linux o en la Raspberry Pi, confirma el puerto y el grupo:

```bash
ls -l /dev/ttyACM0    # o /dev/ttyUSB0
```

Copia `.env.example` a `.env` si el puerto o el GID no son los de ejemplo
(`/dev/ttyACM0` y `20`) y levanta también la ingesta:

```bash
docker compose --profile arduino up -d
```

Dentro del contenedor el puerto siempre es `/dev/ttyACM0`. En Windows, Docker
Desktop no ve los puertos COM: deja el perfil `arduino` apagado y corre la
ingesta en el host, escribiendo en la misma carpeta que el volumen:

```powershell
$env:PUERTO_SERIAL = "COM3"   # el puerto que muestre el Arduino IDE
python riego_bigdata\ingesta\riego_ingesta.py
```

Para parar y conservar los datos: `docker compose --profile arduino down`.
Los JSONL y Parquet siguen en `riego_bigdata/data`.
