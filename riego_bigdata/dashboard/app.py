"""Dashboard ejecutivo del sistema de riego.

Ejecutar:  streamlit run dashboard/app.py

- KPIs y alertas en tiempo casi real: leen los JSONL de la Raw Zone.
- Gráficos históricos: leen el Parquet que produce Spark (Curated Zone).
"""
import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402

st.set_page_config(page_title="Riego inteligente", layout="wide")


# ------------------------- Carga de datos -------------------------
def leer_jsonl_recientes(directorio: Path, n_archivos: int = 60) -> pd.DataFrame:
    archivos = sorted(p for p in directorio.glob("*.jsonl") if not p.name.startswith("."))
    partes = []
    for p in archivos[-n_archivos:]:
        try:
            partes.append(pd.read_json(p, lines=True))
        except ValueError:
            continue          # archivo vacío o dañado
    if not partes:
        return pd.DataFrame()
    df = pd.concat(partes, ignore_index=True)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df.sort_values("ts").reset_index(drop=True)


def leer_parquet(ruta: Path) -> pd.DataFrame:
    try:
        return pd.read_parquet(ruta)
    except Exception:         # carpeta inexistente o vacía todavía
        return pd.DataFrame()


def a_local(serie: pd.Series) -> pd.Series:
    return serie.dt.tz_convert(config.ZONA_HORARIA).dt.tz_localize(None)


# ------------------------- Lógica de alertas -------------------------
def calcular_alertas(sensor: pd.DataFrame, riego: pd.DataFrame, ventanas: pd.DataFrame) -> list[tuple[str, str]]:
    alertas = []
    ahora = pd.Timestamp.now(tz="UTC")

    if sensor.empty:
        return [("error", "Sin lecturas del sensor todavía.")]

    edad = (ahora - sensor["ts"].iloc[-1]).total_seconds()
    if edad > config.SENSOR_OFFLINE_S:
        alertas.append(("error", f"Sensor/ingesta sin datos hace {int(edad // 60)} min."))

    desde = ahora - timedelta(hours=config.SEQUIA_HORAS)
    reciente = sensor[sensor["ts"] >= desde]
    if len(reciente) > 10 and (reciente["humedad"] < config.UMBRAL_HUMEDAD).all():
        alertas.append(("warning", f"Sequía prolongada: humedad bajo {config.UMBRAL_HUMEDAD:.0f}% "
                                   f"durante {config.SEQUIA_HORAS} h."))

    # Actuador: cada RIEGO_CMD debe ir seguido de RIEGO_ON en pocos segundos
    if not riego.empty:
        hace_1h = riego[riego["ts"] >= ahora - timedelta(hours=1)]
        for _, cmd in hace_1h[hace_1h["evento"] == "RIEGO_CMD"].iterrows():
            if (ahora - cmd["ts"]).total_seconds() < 30:
                continue
            on = hace_1h[(hace_1h["evento"] == "RIEGO_ON") &
                         (hace_1h["ts"] >= cmd["ts"]) &
                         (hace_1h["ts"] <= cmd["ts"] + timedelta(seconds=15))]
            if on.empty:
                alertas.append(("error", "Falla del actuador: se pidió riego y el Arduino no confirmó "
                                         f"(a las {a_local(pd.Series([cmd['ts']])).iloc[0]:%H:%M})."))
                break

    if not ventanas.empty and "anomalia" in ventanas:
        rec = ventanas[(ventanas["ventana_fin"] >= ahora - timedelta(hours=1))
                       & (ventanas["anomalia"] != "ok")]
        if not rec.empty:
            tipos = ", ".join(sorted(rec["anomalia"].unique()))
            alertas.append(("warning", f"Anomalías detectadas por Spark en la última hora: {tipos}."))
    return alertas


# ------------------------- Interfaz -------------------------
st.title("Riego inteligente · Panel ejecutivo")


@st.fragment(run_every="10s")
def panel_tiempo_real() -> None:
    sensor = leer_jsonl_recientes(config.RAW_SENSOR)
    riego = leer_jsonl_recientes(config.RAW_RIEGO)
    clima = leer_jsonl_recientes(config.RAW_CLIMA, n_archivos=10)
    ventanas = leer_parquet(config.CURATED_VENTANAS)
    if not ventanas.empty:
        ventanas["ventana_fin"] = pd.to_datetime(ventanas["ventana_fin"], utc=True)

    st.subheader("Estado actual")
    c1, c2, c3, c4 = st.columns(4)

    if sensor.empty:
        c1.metric("Humedad del suelo", "—")
    else:
        h = sensor["humedad"].iloc[-1]
        delta = h - sensor["humedad"].iloc[-2] if len(sensor) > 1 else None
        c1.metric("Humedad del suelo", f"{h:.1f} %", f"{delta:+.1f}" if delta is not None else None)

    estado = "Inactiva"
    if not riego.empty:
        ev = riego[riego["evento"].isin(["RIEGO_ON", "RIEGO_OFF"])]
        if not ev.empty and ev["evento"].iloc[-1] == "RIEGO_ON":
            estado = "Regando"
    c2.metric("Bomba", estado)

    litros_hoy = 0.0
    if not riego.empty:
        hoy = pd.Timestamp.now(tz=config.ZONA_HORARIA).normalize().tz_convert("UTC")
        cmds = riego[(riego["evento"] == "RIEGO_CMD") & (riego["ts"] >= hoy)]
        litros_hoy = cmds["duracion_s"].fillna(0).sum() / 60 * config.CAUDAL_L_MIN
    c3.metric("Agua hoy (estimada)", f"{litros_hoy:.2f} L")

    if clima.empty or pd.isna(clima["temperatura_c"].iloc[-1]):
        c4.metric("Temperatura exterior", "—")
    else:
        c4.metric("Temperatura exterior", f"{clima['temperatura_c'].iloc[-1]:.1f} °C")

    st.subheader("Alertas")
    alertas = calcular_alertas(sensor, riego, ventanas)
    if not alertas:
        st.success("Sin alertas: sistema operando con normalidad.")
    for nivel, msg in alertas:
        (st.error if nivel == "error" else st.warning)(msg)

    st.subheader("Humedad del suelo · últimas 6 horas")
    if not sensor.empty:
        rec = sensor[sensor["ts"] >= pd.Timestamp.now(tz="UTC") - timedelta(hours=6)].copy()
        rec["hora"] = a_local(rec["ts"])
        rec["umbral"] = config.UMBRAL_HUMEDAD
        st.line_chart(rec.set_index("hora")[["humedad", "umbral"]])
    else:
        st.info("Aún no hay lecturas.")


panel_tiempo_real()

st.divider()
st.header("Análisis histórico (PySpark)")
if st.button("Recargar datos históricos"):
    st.cache_data.clear()


@st.cache_data(ttl=60)
def cargar_historico():
    return leer_parquet(config.CURATED_HORARIO), leer_parquet(config.CURATED_DIARIO)


horario, diario = cargar_historico()
if horario.empty or diario.empty:
    st.info("Todavía no hay resúmenes. Ejecuta: python3 spark/batch_analisis.py")
else:
    horario["hora"] = a_local(pd.to_datetime(horario["hora"], utc=True))
    diario["fecha"] = pd.to_datetime(diario["fecha"])
    horario = horario.sort_values("hora")
    diario = diario.sort_values("fecha")

    k1, k2, k3 = st.columns(3)
    k1.metric("Agua total estimada", f"{diario['litros'].sum():.2f} L")
    k2.metric("Riegos totales", int(diario["n_riegos"].sum()))
    temp_media = diario["temp_prom"].mean()
    if pd.notna(temp_media) and temp_media > 0:
        k3.metric("Consumo por °C", f"{diario['litros'].sum() / temp_media:.3f} L/°C")

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Humedad del suelo vs temperatura exterior")
        st.line_chart(horario.set_index("hora")[["humedad_prom", "temp_prom"]])
    with col_b:
        st.subheader("Litros por día (eficiencia hídrica)")
        st.bar_chart(diario.set_index("fecha")["litros"])

    st.subheader("Comparativa semanal de ciclos de riego")
    sem = diario.assign(semana=diario["fecha"].dt.to_period("W").dt.start_time)
    st.bar_chart(sem.groupby("semana")[["n_riegos", "litros"]].sum())

    with st.expander("Ver tabla diaria"):
        st.dataframe(diario)
