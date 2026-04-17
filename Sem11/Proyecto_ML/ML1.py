# %%
# ============================================================
# 0) INSTALAR (una vez) - ejecutá en terminal de VS Code
# ------------------------------------------------------------
# pip install pandas numpy plotly statsmodels scikit-learn joblib
# pip install xgboost             # paquete adicional (gradient boosting)
# pip install pyarrow             # parquet (paquete adicional)
# ============================================================

import os
import re
import html
import glob
import json
import sys
import warnings
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.io as pio
import plotly.graph_objects as go

from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.holtwinters import ExponentialSmoothing

from sklearn.neural_network import MLPRegressor
from sklearn.cluster import KMeans
import joblib


# %%
# ============================================================
# CONFIG "BATCH NOCTURNO" + PONDERACIÓN RECIENTE
# ============================================================

DATA_DIR = "data"  # carpeta con tus CSV
ARTIFACTS_DIR = "artifacts"  # donde se guardan resultados (batch)
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
RUN_DIR = os.path.join(ARTIFACTS_DIR, RUN_ID)
os.makedirs(RUN_DIR, exist_ok=True)
# Pronóstico / evaluación
H = 10  # test: últimos 10 meses
FORECAST_H = 3  # corto plazo (app / tableros existentes)
FORECAST_H_LONG = 60  # largo plazo: 5 años (60 meses)

# Ponderación de recencia (para "dar más peso a lo reciente")
RECENT_WINDOW_MONTHS = 24  # ventana reciente para entrenar el modelo "reciente"
RECENT_BLEND = 0.70  # 70% reciente + 30% histórico

# Métrica ponderada en test (más peso a lo más reciente del test)
TEST_DECAY = 0.25  # mayor => más peso al final (0.15-0.35 suele ir bien)


# %%
# ============================================================
# 1) LECTOR ROBUSTO PARA CSV DEL OIJ
# ============================================================

PROVINCIAS_CR = {
    "SAN JOSE",
    "ALAJUELA",
    "CARTAGO",
    "HEREDIA",
    "GUANACASTE",
    "PUNTARENAS",
    "LIMON",
}


def read_oij_csv_robust(file_path, encoding="utf-8"):
    """
    Lee CSV del OIJ aunque tenga:
    - coma final al final de cada línea
    - comas extra dentro de SubDelito
    - entidades HTML tipo &#211;
    Devuelve df con columnas crudas según el archivo.
    """
    rows = []
    with open(file_path, "r", encoding=encoding, errors="replace") as f:
        _header = f.readline()  # no dependemos del header
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            if line.endswith(","):
                line = line[:-1]

            # Queremos: Delito, SubDelito, + 9 campos desde la derecha
            parts = line.rsplit(",", maxsplit=9)  # => 10 partes
            if len(parts) < 10:
                continue

            left_text = parts[0]
            tail = parts[1:]  # 9 campos

            # Separar Delito y SubDelito (Delito casi nunca tiene coma)
            if "," in left_text:
                delito, subdelito = left_text.split(",", 1)
            else:
                delito, subdelito = left_text, ""

            row = [delito, subdelito] + tail
            row = [html.unescape(str(x)).strip() for x in row]
            rows.append(row)

    df = pd.DataFrame(
        rows,
        columns=[
            "Delito",
            "SubDelito",
            "Fecha",
            "Victima",
            "SubVictima",
            "Edad",
            "Sexo",
            "Nacionalidad",
            "Provincia",
            "Canton",
            "Distrito",
        ],
    )
    return df


def normalize_oij_schema(df):
    """
    Normaliza columnas porque en export OIJ típicamente:
    - Victima trae rango horario (Hora)
    - Provincia/Canton/Distrito vienen corridos
    """
    df = df.copy()

    # Heurística: si "Canton" contiene mayoritariamente provincias, está corrido.
    shifted = df["Canton"].astype(str).str.upper().isin(PROVINCIAS_CR).mean() > 0.80

    if shifted:
        df = df.rename(
            columns={
                "Victima": "Hora_Rango",
                "Edad": "Victima",
                "Sexo": "Edad",
                "Nacionalidad": "Sexo",
                "Provincia": "Nacionalidad",
                "Canton": "Provincia",
                "Distrito": "Canton",
            }
        )
    else:
        df = df.rename(columns={"Victima": "Hora_Rango"})

    # Parse fecha
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")

    # Limpieza básica de strings
    for c in df.columns:
        if df[c].dtype == object:
            s = df[c].astype(str).map(lambda x: html.unescape(x).strip())
            s = s.replace({"": np.nan, "None": np.nan, "nan": np.nan})
            df[c] = s

    # Extra: hora inicio desde texto tipo "06:00:00 - 08:59:59"
    def _parse_start_hour(x):
        if not isinstance(x, str):
            return np.nan
        m = re.match(r"^\s*(\d{2}):(\d{2}):(\d{2})\s*-", x)
        return int(m.group(1)) if m else np.nan

    df["Hora_Inicio"] = df["Hora_Rango"].apply(_parse_start_hour)

    return df


# %%
# ============================================================
# 2) CARGAR según modo (CLI: ML1.py [db|csv|auto|all_csv] [archivo.csv])
#    + source_file para auditar unión
#    all_csv: todos los *.csv en data/ (ignora SQLite; útil desde la web).
# ============================================================

_raw_mode = sys.argv[1].lower().strip() if len(sys.argv) > 1 else "auto"
if _raw_mode not in ("db", "csv", "auto", "all_csv"):
    print(f"Aviso: modo '{_raw_mode}' no reconocido; usando 'auto'.")
    _raw_mode = "auto"

mode = _raw_mode
selected_file = sys.argv[2] if len(sys.argv) > 2 else None
if selected_file:
    selected_file = os.path.basename(selected_file)

print(f"Modo seleccionado: {mode}")
print(f"Archivo seleccionado: {selected_file}")

files = []
data = pd.DataFrame()


def _load_from_csv_paths(path_list):
    """path_list: rutas absolutas o relativas a CSV existentes."""
    dfs = []
    for f in path_list:
        raw = read_oij_csv_robust(f)
        norm = normalize_oij_schema(raw)
        norm["source_file"] = os.path.basename(f)
        dfs.append(norm)
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()


if mode == "db":
    try:
        import db as _db

        cand = _db.get_all_data()
        if cand is not None and not cand.empty:
            data = cand
            if "source_file" in data.columns:
                files = sorted(
                    data["source_file"].dropna().astype(str).unique().tolist()
                )
            if not files:
                files = ["SQLite:data.db"]
            print("Datos cargados desde SQLite, filas:", len(data))
        else:
            raise ValueError("Base de datos vacía")
    except Exception as e:
        print("ERROR modo db:", e)
        raise SystemExit(
            "Modo 'db': la base SQLite está vacía o no se pudo leer. "
            "Cargue datos (web/upload) o use modo 'auto' o 'csv'."
        )

elif mode == "csv":
    if not selected_file:
        raise SystemExit(
            "Modo 'csv': debe indicar el nombre del archivo en data/ "
            "(ej. python ML1.py csv 2020-2021.csv)."
        )
    csv_path = os.path.join(DATA_DIR, selected_file)
    if not os.path.isfile(csv_path):
        raise SystemExit(f"Modo 'csv': no existe el archivo '{csv_path}'.")
    try:
        data = _load_from_csv_paths([csv_path])
        files = [selected_file]
        print(f"Datos cargados desde CSV único ({selected_file}), filas: {len(data)}")
    except Exception as e:
        print("ERROR leyendo CSV:", e)
        raise SystemExit(f"Modo 'csv': error al leer '{selected_file}': {e}")

elif mode == "all_csv":
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    if not files:
        raise SystemExit(
            "Modo 'all_csv': no hay archivos *.csv en la carpeta data/."
        )
    print("Modo all_csv — archivos detectados:")
    for f in files:
        print(" -", f)
    try:
        data = _load_from_csv_paths(files)
        print(
            f"Datos cargados desde {len(files)} CSV en data/, filas: {len(data)}"
        )
    except Exception as e:
        print("ERROR leyendo CSV (all_csv):", e)
        raise SystemExit(f"Modo 'all_csv': error al leer datos/: {e}")

else:
    # auto: SQLite primero; si vacío o error, todos los CSV en data/
    try:
        import db as _db

        cand = _db.get_all_data()
        if cand is not None and not cand.empty:
            data = cand
            if "source_file" in data.columns:
                files = sorted(
                    data["source_file"].dropna().astype(str).unique().tolist()
                )
            if not files:
                files = ["SQLite:data.db"]
            print("Datos cargados desde SQLite, filas:", len(data))
        else:
            raise ValueError("Base de datos vacía")
    except Exception as e:
        print("Usando CSV (SQLite vacía o error):", e)
        files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
        print("Archivos detectados:")
        for f in files:
            print(" -", f)
        if files:
            data = _load_from_csv_paths(files)
        else:
            data = pd.DataFrame()

# Quitar filas sin fecha
data = data.dropna(subset=["Fecha"])
if data.empty:
    raise SystemExit(
        "No hay datos válidos para el modo elegido: revise la base SQLite, "
        "el archivo CSV o la carpeta data/."
    )

# Normalizar mayúsculas en ubicación
for c in ["Provincia", "Canton", "Distrito"]:
    if c in data.columns:
        data[c] = data[c].astype(str).str.upper().str.strip()

print("Filas totales:", len(data))
print("Filas por archivo:\n", data["source_file"].value_counts())
print("Fecha min:", data["Fecha"].min(), "| Fecha max:", data["Fecha"].max())
data.head()


# %%
# ============================================================
# 3) FILTROS
# ============================================================

# data = data[data["Delito"].str.contains("ROBO|ASALTO|HURTO", case=False, na=False)]


# %%
# ============================================================
# 4) AGREGADO MENSUAL TOTAL (para tu demo actual de forecasting)
# ============================================================

monthly = (
    data.groupby(pd.Grouper(key="Fecha", freq="MS"))
    .size()
    .rename("incidentes")
    .asfreq("MS", fill_value=0)
    .astype(float)
)

print(monthly.head(), "\n")
print("Rango:", monthly.index.min(), "->", monthly.index.max(), " Meses:", len(monthly))


# %%
# ============================================================
# 4B) (NUEVO) AGREGADOS PARA MAPA / HEATMAP (batch-friendly)
#   - esto NO entrena modelos: solo deja listo lo que Flask consumiría
# ============================================================


def build_monthly_agg(df, geo_col):
    if geo_col not in df.columns:
        return None
    tmp = df.copy()
    tmp["Mes"] = tmp["Fecha"].dt.to_period("M").dt.to_timestamp()
    agg = (
        tmp.groupby(["Mes", geo_col, "Delito"], dropna=False)
        .size()
        .rename("incidentes")
        .reset_index()
    )
    # limpieza: zonas vacías
    agg = agg[agg[geo_col].notna() & (agg[geo_col].astype(str).str.len() > 0)]
    return agg


agg_prov = build_monthly_agg(data, "Provincia")
agg_cant = build_monthly_agg(data, "Canton")
agg_dist = build_monthly_agg(data, "Distrito") if "Distrito" in data.columns else None

print("Agg provincia:", None if agg_prov is None else agg_prov.shape)
print("Agg cantón:", None if agg_cant is None else agg_cant.shape)
print("Agg distrito:", None if agg_dist is None else agg_dist.shape)

# %%
# ============================================================
# 4C) (NUEVO) TOP DELITOS POR ZONA (para ranking en la app)
# ============================================================


def top_delitos_por_zona(agg, geo_col, top_n=10, last_months=None):
    """
    Devuelve top N delitos por cada zona (Provincia/Canton/Distrito).
    Si last_months se define, usa solo los últimos N meses (más "actual").
    """
    if agg is None or geo_col not in agg.columns:
        return None

    tmp = agg.copy()

    if last_months is not None:
        last_mes = tmp["Mes"].max()
        cutoff = (last_mes - pd.offsets.MonthBegin(last_months)).to_pydatetime()
        tmp = tmp[tmp["Mes"] >= cutoff]

    out = (
        tmp.groupby([geo_col, "Delito"], as_index=False)["incidentes"]
        .sum()
        .sort_values([geo_col, "incidentes"], ascending=[True, False])
        .groupby(geo_col, as_index=False)
        .head(top_n)
    )
    return out


# top histórico (2020-2025)
top_prov_hist = top_delitos_por_zona(agg_prov, "Provincia", top_n=10)
top_cant_hist = top_delitos_por_zona(agg_cant, "Canton", top_n=10)
top_dist_hist = (
    top_delitos_por_zona(agg_dist, "Distrito", top_n=10)
    if agg_dist is not None
    else None
)

# top reciente (ej: últimos 12 meses) -> útil para “lo más actual”
top_prov_12m = top_delitos_por_zona(agg_prov, "Provincia", top_n=10, last_months=12)
top_cant_12m = top_delitos_por_zona(agg_cant, "Canton", top_n=10, last_months=12)
top_dist_12m = (
    top_delitos_por_zona(agg_dist, "Distrito", top_n=10, last_months=12)
    if agg_dist is not None
    else None
)

print("Top prov hist:", None if top_prov_hist is None else top_prov_hist.shape)
print("Top prov 12m :", None if top_prov_12m is None else top_prov_12m.shape)


# === DESCRIPTIVAS ===
def gen_descriptive_stats(data, delito_col="Delito"):
    """
    Estadísticas descriptivas por delito y por zona.
    Retorna dict con DataFrames para exportar.
    """
    stats = {}

    # 1. Por delito (conteo total)
    delito_counts = (
        data.groupby(delito_col, dropna=False)
        .size()
        .sort_values(ascending=False)
        .rename("Conteo")
    )
    delito_pct = (delito_counts / delito_counts.sum() * 100).round(2)
    stats["delito_conteo"] = pd.DataFrame(
        {
            "Delito": delito_counts.index,
            "Conteo": delito_counts.values,
            "Porcentaje": delito_pct.values,
        }
    ).reset_index(drop=True)

    # 2. Series temporal: media y mediana por mes
    monthly_desc = (
        data.groupby(pd.Grouper(key="Fecha", freq="MS"))
        .size()
        .rename("incidentes")
        .asfreq("MS", fill_value=0)
    )
    stats["monthly_aggregate"] = pd.DataFrame(
        {
            "Fecha": monthly_desc.index,
            "Incidentes": monthly_desc.values,
        }
    ).reset_index(drop=True)

    stats["monthly_stats"] = pd.DataFrame(
        {
            "Métrica": ["Media", "Mediana", "Desv. Estándar", "Mín", "Máx"],
            "Valor": [
                float(monthly_desc.mean()),
                float(monthly_desc.median()),
                float(monthly_desc.std()),
                float(monthly_desc.min()),
                float(monthly_desc.max()),
            ],
        }
    )

    # 3. Por provincia
    if "Provincia" in data.columns:
        prov_counts = (
            data.groupby("Provincia", dropna=False)
            .size()
            .sort_values(ascending=False)
            .rename("Conteo")
        )
        prov_pct = (prov_counts / prov_counts.sum() * 100).round(2)
        stats["provincia_conteo"] = pd.DataFrame(
            {
                "Provincia": prov_counts.index,
                "Conteo": prov_counts.values,
                "Porcentaje": prov_pct.values,
            }
        ).reset_index(drop=True)

    # 4. Por categoría de edad (columna Edad)
    if "Edad" in data.columns:
        edad_valid = pd.to_numeric(data["Edad"], errors="coerce")
        stats["edad_stats"] = pd.DataFrame(
            {
                "Métrica": ["Media", "Mediana", "Desv. Estándar", "Mín", "Máx"],
                "Valor": [
                    float(edad_valid.mean()),
                    float(edad_valid.median()),
                    float(edad_valid.std()),
                    float(edad_valid.min()),
                    float(edad_valid.max()),
                ],
            }
        )

    # 5. Por sexo
    if "Sexo" in data.columns:
        sexo_counts = data["Sexo"].value_counts(dropna=False).rename("Conteo")
        sexo_pct = (sexo_counts / sexo_counts.sum() * 100).round(2)
        stats["sexo_conteo"] = pd.DataFrame(
            {
                "Sexo": sexo_counts.index,
                "Conteo": sexo_counts.values,
                "Porcentaje": sexo_pct.values,
            }
        ).reset_index(drop=True)

    return stats


# === DETECCIÓN ATÍPICOS ===
def detect_outliers(monthly, method="iqr"):
    """
    Detecta valores atípicos en la serie temporal.
    Métodos: 'iqr' (IQR), 'zscore' (Z-score > 2.5).
    Retorna dict con outliers detectados.
    """
    y = monthly.values
    outliers_info = {}

    if method == "iqr":
        Q1 = np.percentile(y, 25)
        Q3 = np.percentile(y, 75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        mask = (y < lower_bound) | (y > upper_bound)
        outliers_info["method"] = "IQR"
        outliers_info["Q1"] = float(Q1)
        outliers_info["Q3"] = float(Q3)
        outliers_info["IQR"] = float(IQR)
        outliers_info["lower_bound"] = float(lower_bound)
        outliers_info["upper_bound"] = float(upper_bound)

    elif method == "zscore":
        z_scores = np.abs((y - y.mean()) / y.std())
        mask = z_scores > 2.5
        outliers_info["method"] = "Z-score (> 2.5)"
        outliers_info["mean"] = float(y.mean())
        outliers_info["std"] = float(y.std())

    # Índices y valores outliers
    outlier_indices = np.where(mask)[0]
    outliers_data = pd.DataFrame(
        {
            "Índice": outlier_indices,
            "Fecha": monthly.index[outlier_indices],
            "Valor": y[outlier_indices],
            "Diferencia_de_Media": y[outlier_indices] - y.mean(),
        }
    )

    outliers_info["outliers"] = outliers_data.to_dict("records")
    outliers_info["cantidad"] = int(len(outlier_indices))

    return outliers_info


# === CLUSTERING ===
def cluster_crimes(agg_data, n_clusters=3):
    """
    Agrupa provincias/delitos por similaridad en patrones mensuales.
    Usa KMeans sobre matriz de incidentes normalizados.
    """
    if agg_data is None or agg_data.empty:
        return None

    # Pivot: filas = zona/delito, columnas = mes, valores = incidentes
    tmp = agg_data.copy()
    tmp["Mes"] = pd.to_datetime(tmp["Mes"], errors="coerce")

    # Agrega por (Provincia, Mes) o (Cantón, Mes) según columnas presentes
    if "Provincia" in tmp.columns:
        pivot = tmp.pivot_table(
            index="Provincia", columns="Mes", values="incidentes", aggfunc="sum"
        )
        entity_col = "Provincia"
    elif "Canton" in tmp.columns:
        pivot = tmp.pivot_table(
            index="Canton", columns="Mes", values="incidentes", aggfunc="sum"
        )
        entity_col = "Canton"
    else:
        return None

    pivot = pivot.fillna(0).astype(float)

    if len(pivot) < n_clusters:
        n_clusters = max(1, len(pivot) - 1)

    # Normalizar
    from sklearn.preprocessing import StandardScaler

    scaler = StandardScaler()
    pivot_scaled = scaler.fit_transform(pivot)

    # KMeans
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    labels = kmeans.fit_predict(pivot_scaled)

    # Resultado
    cluster_result = pd.DataFrame(
        {
            entity_col: pivot.index,
            "Cluster": labels,
            "Incidentes_Total": pivot.sum(axis=1).values,
            "Incidentes_Media": pivot.mean(axis=1).values,
            "Incidentes_Desv": pivot.std(axis=1).values,
        }
    ).sort_values("Cluster")

    return cluster_result


# === MÁS GRÁFICOS ===
def plot_descriptive_charts(data, run_dir):
    """
    Genera gráficos exploratorios (Plotly):
    - Histograma de delitos (top 15)
    - Boxplot de incidentes por provincia
    - Distribución de sexo
    - Scatter: correlación mes vs incidentes
    """
    os.makedirs(run_dir, exist_ok=True)

    # 1. TOP 15 DELITOS (bar chart)
    delito_counts = data[data["Delito"].notna()]["Delito"].value_counts().head(15)

    fig1 = go.Figure(
        data=[
            go.Bar(
                y=delito_counts.index,
                x=delito_counts.values,
                orientation="h",
                marker=dict(color="#0067b8"),
            )
        ]
    )
    fig1.update_layout(
        title="Top 15 Delitos (histórico)",
        xaxis_title="Conteo",
        yaxis_title="Delito",
        template="plotly_white",
        height=500,
        margin=dict(l=250),
    )
    fig1.write_html(os.path.join(run_dir, "01_top_delitos.html"))
    print("[ok] Gráfico Top Delitos guardado")

    # 2. BOXPLOT POR PROVINCIA
    if "Provincia" in data.columns:
        # Agregar mes
        temp_data = data.copy()
        temp_data["Mes"] = temp_data["Fecha"].dt.to_period("M")

        monthly_by_prov = (
            temp_data.groupby(["Mes", "Provincia"])
            .size()
            .reset_index(name="incidentes")
        )

        fig2 = go.Figure()
        for prov in sorted(monthly_by_prov["Provincia"].unique()):
            subset = monthly_by_prov[monthly_by_prov["Provincia"] == prov]
            fig2.add_trace(
                go.Box(
                    y=subset["incidentes"],
                    name=prov,
                    boxmean="sd",
                )
            )

        fig2.update_layout(
            title="Distribución de incidentes por Provincia",
            yaxis_title="Incidentes (mensual)",
            template="plotly_white",
            height=500,
        )
        fig2.write_html(os.path.join(run_dir, "02_boxplot_provincia.html"))
        print("[ok] Boxplot Provincia guardado")

    # 3. PIE SEXO
    if "Sexo" in data.columns:
        sexo_counts = data["Sexo"].value_counts()

        fig3 = go.Figure(
            data=[go.Pie(labels=sexo_counts.index, values=sexo_counts.values)]
        )
        fig3.update_layout(
            title="Distribución por Sexo",
            template="plotly_white",
            height=500,
        )
        fig3.write_html(os.path.join(run_dir, "03_pie_sexo.html"))
        print("[ok] Pie Sexo guardado")

    # 4. TENDENCIA MENSUAL CON MEDIA MÓVIL
    monthly_full = (
        data.groupby(pd.Grouper(key="Fecha", freq="MS"))
        .size()
        .asfreq("MS", fill_value=0)
    )

    # Media móvil 3 meses
    ma_3m = monthly_full.rolling(window=3, center=True).mean()

    fig4 = go.Figure()
    fig4.add_trace(
        go.Scatter(
            x=monthly_full.index,
            y=monthly_full.values,
            mode="lines+markers",
            name="Serie Original",
            line=dict(color="#c42b1c", width=1),
            marker=dict(size=4),
        )
    )
    fig4.add_trace(
        go.Scatter(
            x=ma_3m.index,
            y=ma_3m.values,
            mode="lines",
            name="Media Móvil (3m)",
            line=dict(color="#107c10", width=3, dash="solid"),
        )
    )
    fig4.update_layout(
        title="Serie Temporal con Media Móvil (3 meses)",
        xaxis_title="Fecha",
        yaxis_title="Incidentes",
        template="plotly_white",
        height=450,
    )
    fig4.write_html(os.path.join(run_dir, "04_serie_media_movil.html"))
    print("[ok] Serie Temporal (con MA) guardado")


# ============================================================
# EJECUTAR LAS 4 FUNCIONES (después del paso 4C)
# ============================================================

print("\n" + "=" * 60)
print("ANÁLISIS EXPLORATORIO (EDA)")
print("=" * 60)

# 1. Estadísticas descriptivas
print("\nGenerando estadísticas descriptivas...")
desc_stats = gen_descriptive_stats(data, delito_col="Delito")
for key, df in desc_stats.items():
    if isinstance(df, pd.DataFrame):
        print(f"\n{key}:")
        print(df)
        df.to_csv(os.path.join(RUN_DIR, f"desc_{key}.csv"), index=False)

# 2. Detección de atípicos
print("\nDetectando valores atípicos (IQR)...")
outliers = detect_outliers(monthly, method="iqr")
print(f"Atípicos detectados: {outliers['cantidad']}")
if outliers["cantidad"] > 0:
    print("Detalles:")
    for out in outliers["outliers"][:5]:  # primeros 5
        print(
            f"  - {out['Fecha']}: {out['Valor']} (diff: {out['Diferencia_de_Media']:.1f})"
        )
with open(os.path.join(RUN_DIR, "outliers_iqr.json"), "w", encoding="utf-8") as f:
    json.dump(outliers, f, indent=2, ensure_ascii=False, default=str)

# 3. Clustering
print("\nEjecutando clustering (K-Means, n=3)...")
if agg_prov is not None:
    clusters = cluster_crimes(agg_prov, n_clusters=3)
    if clusters is not None:
        print(clusters)
        clusters.to_csv(os.path.join(RUN_DIR, "clustering_provincia.csv"), index=False)
        print("[ok] Clustering guardado")

# 4. Gráficos exploratorios
print("\nGenerando gráficos exploratorios...")
plot_descriptive_charts(data, RUN_DIR)

print("\nAnálisis exploratorio completado.")

# %%
# ============================================================
# 5) TRAIN / TEST (últimos H meses como test)
# ============================================================

if len(monthly) <= H + 12:
    print("[aviso] Pocos meses: con 2020-2025 deberías tener ~72 meses.")

train_full = monthly.iloc[:-H]
test = monthly.iloc[-H:]

# Ventana reciente para ponderar más lo actual
train_recent = (
    train_full.iloc[-RECENT_WINDOW_MONTHS:]
    if len(train_full) > RECENT_WINDOW_MONTHS
    else train_full
)

print("Train full meses:", len(train_full))
print("Train recent meses:", len(train_recent))
print("Test meses:", len(test))


# %%
# ============================================================
# 6) MÉTRICAS (incluye WRMSE ponderada por recencia en el TEST)
# ============================================================


def MSE(pred, real):
    pred = np.asarray(pred, dtype=float)
    real = np.asarray(real, dtype=float)
    return np.mean((real - pred) ** 2)


def RMSE(pred, real):
    return float(np.sqrt(MSE(pred, real)))


def weighted_rmse(pred, real, decay=0.25):
    """
    WRMSE: pondera más lo reciente del TEST.
    decay alto => más peso en los últimos puntos.
    """
    pred = np.asarray(pred, dtype=float)
    real = np.asarray(real, dtype=float)
    n = len(real)
    # pesos crecientes en el tiempo (último punto peso 1.0)
    w = np.exp(np.linspace(-decay * (n - 1), 0, n))
    return float(np.sqrt(np.sum(w * (real - pred) ** 2) / np.sum(w)))


def PFA(pred, real):
    pred = np.asarray(pred, dtype=float)
    real = np.asarray(real, dtype=float)
    return float(np.mean(pred > real))


def PTFA(pred, real):
    pred = np.asarray(pred, dtype=float)
    real = np.asarray(real, dtype=float)
    mask = pred > real
    total = np.sum((pred - real)[mask])
    sreal = np.sum(np.abs(real))
    if total == 0:
        sreal = 1.0
    return float(total / sreal)


def score_model(name, pred, real):
    return {
        "Modelo": name,
        "MSE": MSE(pred, real),
        "RMSE": RMSE(pred, real),
        "WRMSE": weighted_rmse(pred, real, decay=TEST_DECAY),
        "PFA": PFA(pred, real),
        "PTFA": PTFA(pred, real),
    }


def blend_preds(pred_full, pred_recent, alpha=0.70):
    """
    Combina predicciones para ponderar más lo reciente.
    """
    pred_full = pd.Series(pred_full, index=test.index)
    pred_recent = pd.Series(pred_recent, index=test.index)
    return alpha * pred_recent + (1 - alpha) * pred_full


# %%
# ============================================================
# 7) MODELO 1: HOLT-WINTERS (full + recent) -> BLEND
# ============================================================


def fit_hw(train_series, seasonal_periods=12):
    n = len(train_series)
    use_seasonal = n >= 2 * seasonal_periods

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        if use_seasonal:
            model = ExponentialSmoothing(
                train_series,
                trend="add",
                seasonal="add",
                seasonal_periods=seasonal_periods,
            ).fit(optimized=True)
        else:
            model = ExponentialSmoothing(train_series, trend="add", seasonal=None).fit(
                optimized=True
            )

    return model


hw_model_full = fit_hw(train_full)
hw_pred_full = hw_model_full.forecast(H)

hw_model_recent = fit_hw(train_recent)
hw_pred_recent = hw_model_recent.forecast(H)

# Pred final ponderando reciente
hw_pred = blend_preds(hw_pred_full.values, hw_pred_recent.values, alpha=RECENT_BLEND)


# %%
# ============================================================
# 8) MODELO 2: SARIMA (grid search) full + recent -> BLEND
# ============================================================


def sarima_grid_search(
    train_series,
    test_series,
    m=12,
    p_range=range(0, 3),
    d_range=range(0, 2),
    q_range=range(0, 3),
    P_range=range(0, 2),
    D_range=range(0, 2),
    Q_range=range(0, 2),
):

    best = {
        "rmse": np.inf,
        "order": None,
        "seasonal_order": None,
        "model": None,
        "pred": None,
    }

    for p in p_range:
        for d in d_range:
            for q in q_range:
                for P in P_range:
                    for D in D_range:
                        for Q in Q_range:
                            order = (p, d, q)
                            seas = (P, D, Q, m)
                            try:
                                with warnings.catch_warnings():
                                    warnings.simplefilter("ignore")
                                    model = SARIMAX(
                                        train_series,
                                        order=order,
                                        seasonal_order=seas,
                                        enforce_stationarity=False,
                                        enforce_invertibility=False,
                                    ).fit(disp=False)
                                pred = model.forecast(len(test_series))
                                rmse = RMSE(pred, test_series)
                                if rmse < best["rmse"]:
                                    best.update(
                                        {
                                            "rmse": rmse,
                                            "order": order,
                                            "seasonal_order": seas,
                                            "model": model,
                                            "pred": pred,
                                        }
                                    )
                            except Exception:
                                continue

    return best


sarima_best_full = sarima_grid_search(train_full, test, m=12)
sarima_pred_full = sarima_best_full["pred"]

sarima_best_recent = sarima_grid_search(train_recent, test, m=12)
sarima_pred_recent = sarima_best_recent["pred"]

sarima_pred = blend_preds(
    sarima_pred_full.values, sarima_pred_recent.values, alpha=RECENT_BLEND
)

print(
    "Mejor SARIMA full  :",
    sarima_best_full["order"],
    sarima_best_full["seasonal_order"],
    "RMSE:",
    sarima_best_full["rmse"],
)
print(
    "Mejor SARIMA recent:",
    sarima_best_recent["order"],
    sarima_best_recent["seasonal_order"],
    "RMSE:",
    sarima_best_recent["rmse"],
)


# %%
# ============================================================
# 9) MODELO 3: "NNETAR" aproximado con MLP (lags) full + recent -> BLEND
# ============================================================


def make_lag_matrix(series, max_lag=12):
    y = np.asarray(series, dtype=float)
    X, Y = [], []
    for i in range(max_lag, len(y)):
        X.append(y[i - max_lag : i])
        Y.append(y[i])
    return np.array(X), np.array(Y)


def forecast_recursive(model, history, h, max_lag=12):
    hist = list(np.asarray(history, dtype=float))
    preds = []
    for _ in range(h):
        x = np.array(hist[-max_lag:]).reshape(1, -1)
        yhat = float(model.predict(x)[0])
        preds.append(yhat)
        hist.append(yhat)
    return np.array(preds)


MAX_LAG = 12 if len(train_full) >= 24 else min(6, max(2, len(train_full) // 2))

# full
Xf, yf = make_lag_matrix(train_full.values, max_lag=MAX_LAG)
mlp_full = MLPRegressor(
    hidden_layer_sizes=(64, 32), activation="relu", random_state=7, max_iter=5000
)
mlp_full.fit(Xf, yf)
mlp_pred_full = forecast_recursive(mlp_full, train_full.values, H, max_lag=MAX_LAG)

# recent
Xr, yr = make_lag_matrix(
    train_recent.values, max_lag=min(MAX_LAG, max(2, len(train_recent) // 3))
)
lag_recent = min(MAX_LAG, max(2, len(train_recent) // 3))

mlp_recent = MLPRegressor(
    hidden_layer_sizes=(64, 32), activation="relu", random_state=7, max_iter=5000
)
mlp_recent.fit(Xr, yr)
mlp_pred_recent = forecast_recursive(
    mlp_recent, train_recent.values, H, max_lag=lag_recent
)

mlp_pred = blend_preds(mlp_pred_full, mlp_pred_recent, alpha=RECENT_BLEND)


# %%
# ============================================================
# 10) MODELO 4: XGBOOST full + recent -> BLEND
# ============================================================

try:
    import xgboost as xgb

    # full
    Xf, yf = make_lag_matrix(train_full.values, max_lag=MAX_LAG)
    xgb_full = xgb.XGBRegressor(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="reg:squarederror",
        random_state=7,
    )
    xgb_full.fit(Xf, yf)
    xgb_pred_full = forecast_recursive(xgb_full, train_full.values, H, max_lag=MAX_LAG)

    # recent
    lag_recent = min(MAX_LAG, max(2, len(train_recent) // 3))
    Xr, yr = make_lag_matrix(train_recent.values, max_lag=lag_recent)
    xgb_recent = xgb.XGBRegressor(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="reg:squarederror",
        random_state=7,
    )
    xgb_recent.fit(Xr, yr)
    xgb_pred_recent = forecast_recursive(
        xgb_recent, train_recent.values, H, max_lag=lag_recent
    )

    xgb_pred = blend_preds(xgb_pred_full, xgb_pred_recent, alpha=RECENT_BLEND)

    HAS_XGB = True
except Exception as e:
    print("XGBoost no disponible. Detalle:", e)
    HAS_XGB = False
    xgb_pred = None


# %%
# ============================================================
# 11) COMPARAR ERRORES (incluye WRMSE para ponderar lo reciente)
# ============================================================

scores = []
scores.append(score_model("Holt-Winters (blend)", hw_pred.values, test.values))
scores.append(
    score_model(
        f"SARIMA (blend) full={sarima_best_full['order']}/{sarima_best_full['seasonal_order']}",
        sarima_pred.values,
        test.values,
    )
)
scores.append(score_model(f"MLP (blend) lags={MAX_LAG}", mlp_pred.values, test.values))
if HAS_XGB:
    scores.append(
        score_model(f"XGBoost (blend) lags={MAX_LAG}", xgb_pred.values, test.values)
    )

errores = pd.DataFrame(scores).set_index("Modelo").sort_values("WRMSE")
errores


# %%
# ============================================================
# 12) SELECCIONAR EL MEJOR (por WRMSE -> favorece meses recientes)
# ============================================================

best_name = errores.index[0]
print("[ok] Mejor modelo (por WRMSE):", best_name)

pred_map = {
    "Holt-Winters (blend)": hw_pred,
    f"SARIMA (blend) full={sarima_best_full['order']}/{sarima_best_full['seasonal_order']}": sarima_pred,
    f"MLP (blend) lags={MAX_LAG}": mlp_pred,
}
if HAS_XGB:
    pred_map[f"XGBoost (blend) lags={MAX_LAG}"] = xgb_pred

best_pred = pred_map[best_name]

# %%
# ============================================================
# 12.2) SARIMAX - Banda 95% (para el SARIMA FULL)
# ============================================================
res = sarima_best_full["model"]
fc = res.get_forecast(steps=len(test))

pred_mean = fc.predicted_mean
ci = fc.conf_int(alpha=0.05)  # 95%

# Alinear índices con el test (importante)
pred_mean = pd.Series(pred_mean.values, index=test.index, name="pred")
ci = pd.DataFrame(ci.values, index=test.index, columns=["lower", "upper"])

lower = ci["lower"]
upper = ci["upper"]


# %%
# ============================================================
# 13) GRAFICO INTERACTIVO (Plotly, banda 95%, sin range slider)
# ============================================================


def plot_forecast_with_band(
    train, test, pred, lower, upper, title="Forecast", out_html="forecast_plot.html"
):
    out_dir = os.path.dirname(out_html)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    fig = go.Figure()

    # Banda primero (detrás de las líneas)
    fig.add_trace(
        go.Scatter(
            x=upper.index,
            y=upper.values,
            mode="lines",
            line=dict(width=0),
            showlegend=False,
            hoverinfo="skip",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=lower.index,
            y=lower.values,
            mode="lines",
            line=dict(width=0),
            fill="tonexty",
            fillcolor="rgba(90, 90, 100, 0.2)",
            name="IC 95%",
            hoverinfo="skip",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=train.index,
            y=train.values,
            mode="lines",
            name="Train",
            line=dict(color="#0067b8", width=2),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=test.index,
            y=test.values,
            mode="lines",
            name="Test",
            line=dict(color="#c42b1c", width=2, dash="dot"),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=pred.index,
            y=pred.values,
            mode="lines",
            name="Predicción",
            line=dict(color="#107c10", width=3),
        )
    )

    if len(test) > 0:
        vline_x = test.index[0]
        fig.add_vline(
            x=vline_x,
            line_width=1,
            line_dash="solid",
            line_color="#767676",
        )

    fig.update_layout(
        title=dict(
            text=title,
            font=dict(size=18, family="Arial, sans-serif", color="#1f1f1f"),
        ),
        paper_bgcolor="#fafafa",
        plot_bgcolor="#fafafa",
        font=dict(family="Arial, sans-serif", size=12, color="#1f1f1f"),
        xaxis=dict(
            rangeslider=dict(visible=False),
            showgrid=True,
            gridcolor="#e5e5e5",
            zeroline=False,
        ),
        yaxis=dict(
            title="Incidentes (mensual)",
            showgrid=True,
            gridcolor="#e5e5e5",
            zeroline=False,
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=56, r=28, t=64, b=48),
    )

    # auto_open=False: la app Flask muestra estos HTML en iframes; no abrir pestañas al ejecutar el pipeline.
    fig.write_html(out_html, auto_open=False)
    print("[ok] Gráfico guardado en:", os.path.abspath(out_html))


plot_forecast_with_band(
    train_full,
    test,
    pred_mean,
    lower,
    upper,
    title="Pronóstico SARIMA con banda 95%",
    out_html=os.path.join(RUN_DIR, "forecast_band.html"),
)
# %%
# %%
# ============================================================
# 14) "BATCH NOCTURNO": guardar artefactos a disco
#   - simula lo que luego Flask consumiría sin recalcular todo
# ============================================================

# IMPORTANTE: crear carpeta del run ANTES de escribir archivos
os.makedirs(RUN_DIR, exist_ok=True)

# -----------------------------
# A) Guardar data limpia
# -----------------------------
try:
    data.to_parquet(os.path.join(RUN_DIR, "data_limpia.parquet"), index=False)
except Exception:
    data.to_csv(os.path.join(RUN_DIR, "data_limpia.csv"), index=False)

# Serie mensual + errores
monthly.to_frame().to_csv(os.path.join(RUN_DIR, "serie_mensual_total.csv"))
errores.to_csv(os.path.join(RUN_DIR, "errores_modelos.csv"))

# -----------------------------
# B) Forecast 3 meses hacia adelante (SIN recalcular en vivo)
# -----------------------------
hist_complete = pd.concat([train_full, test]).astype(float)


def forecast_next_months_best(forecast_h: int):
    """
    Pronóstico ``forecast_h`` meses hacia adelante (misma lógica de modelo ganador).
    - NO usa grid search con ceros (eso sesga a predecir 0).
    - Si gana SARIMA: refit con los mejores parámetros encontrados en evaluación.
    - Si gana HW: refit HW.
    - Si gana MLP/XGB: el pronóstico se obtiene con MLP.
    """
    last_idx = hist_complete.index[-1]
    future_index = pd.date_range(
        last_idx + pd.offsets.MonthBegin(1), periods=forecast_h, freq="MS"
    )

    # -------- Holt-Winters --------
    if best_name.startswith("Holt-Winters"):
        m_full = fit_hw(hist_complete)
        pred_full = m_full.forecast(forecast_h)

        recent_series = (
            hist_complete.iloc[-RECENT_WINDOW_MONTHS:]
            if len(hist_complete) > RECENT_WINDOW_MONTHS
            else hist_complete
        )
        m_recent = fit_hw(recent_series)
        pred_recent = m_recent.forecast(forecast_h)

        pred = RECENT_BLEND * pred_recent.values + (1 - RECENT_BLEND) * pred_full.values
        pred = np.clip(pred, 0, None)  # conteos: no negativos
        return pd.Series(pred, index=future_index)

    # -------- SARIMA --------
    if best_name.startswith("SARIMA"):
        order_full = sarima_best_full["order"]
        seas_full = sarima_best_full["seasonal_order"]

        order_recent = sarima_best_recent["order"]
        seas_recent = sarima_best_recent["seasonal_order"]

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            model_full = SARIMAX(
                hist_complete,
                order=order_full,
                seasonal_order=seas_full,
                enforce_stationarity=False,
                enforce_invertibility=False,
            ).fit(disp=False)
            pred_full = model_full.forecast(forecast_h)

            recent_series = (
                hist_complete.iloc[-RECENT_WINDOW_MONTHS:]
                if len(hist_complete) > RECENT_WINDOW_MONTHS
                else hist_complete
            )
            model_recent = SARIMAX(
                recent_series,
                order=order_recent,
                seasonal_order=seas_recent,
                enforce_stationarity=False,
                enforce_invertibility=False,
            ).fit(disp=False)
            pred_recent = model_recent.forecast(forecast_h)

        pred = RECENT_BLEND * np.asarray(pred_recent) + (1 - RECENT_BLEND) * np.asarray(
            pred_full
        )
        pred = np.clip(pred, 0, None)
        return pd.Series(pred, index=future_index)

    # -------- MLP (reserva si gana MLP o XGB) --------
    lag = 12 if len(hist_complete) >= 24 else min(6, max(2, len(hist_complete) // 2))
    X, y = make_lag_matrix(hist_complete.values, max_lag=lag)

    m = MLPRegressor(
        hidden_layer_sizes=(64, 32), activation="relu", random_state=7, max_iter=5000
    )
    m.fit(X, y)

    pred = forecast_recursive(m, hist_complete.values, forecast_h, max_lag=lag)
    pred = np.clip(pred, 0, None)
    return pd.Series(pred, index=future_index)


future_pred_3m = forecast_next_months_best(FORECAST_H)
future_pred_60m = forecast_next_months_best(FORECAST_H_LONG)

# redondear a enteros para lectura tipo conteo
future_pred_3m_int = future_pred_3m.round().astype(int)
future_pred_60m_int = future_pred_60m.round().astype(int)

# Guardar forecast (3m compatible con consumo actual; 60m nuevo)
future_pred_3m.to_csv(os.path.join(RUN_DIR, "forecast_3m.csv"))
future_pred_3m_int.to_csv(os.path.join(RUN_DIR, "forecast_3m_int.csv"))
future_pred_60m.to_csv(os.path.join(RUN_DIR, "forecast_60m.csv"))
future_pred_60m_int.to_csv(os.path.join(RUN_DIR, "forecast_60m_int.csv"))

# -----------------------------
# C) Guardar agregados para mapa
# -----------------------------
if agg_prov is not None:
    agg_prov.to_csv(os.path.join(RUN_DIR, "agg_mes_provincia_delito.csv"), index=False)
if agg_cant is not None:
    agg_cant.to_csv(os.path.join(RUN_DIR, "agg_mes_canton_delito.csv"), index=False)
if agg_dist is not None:
    agg_dist.to_csv(os.path.join(RUN_DIR, "agg_mes_distrito_delito.csv"), index=False)


# -----------------------------
# C2) TOP delitos por provincia/cantón/distrito
#      - útil para gráficos rápidos en la app
# -----------------------------
def top_delitos_por_zona(agg, geo_col, top_n=10, last_months=None):
    if agg is None or geo_col not in agg.columns:
        return None
    tmp = agg.copy()
    if last_months is not None:
        last_mes = tmp["Mes"].max()
        cutoff = (last_mes - pd.offsets.MonthBegin(last_months)).to_pydatetime()
        tmp = tmp[tmp["Mes"] >= cutoff]

    out = (
        tmp.groupby([geo_col, "Delito"], as_index=False)["incidentes"]
        .sum()
        .sort_values([geo_col, "incidentes"], ascending=[True, False])
        .groupby(geo_col, as_index=False)
        .head(top_n)
    )
    return out


top_prov_hist = top_delitos_por_zona(agg_prov, "Provincia", top_n=10)
top_prov_12m = top_delitos_por_zona(agg_prov, "Provincia", top_n=10, last_months=12)

if top_prov_hist is not None:
    top_prov_hist.to_csv(
        os.path.join(RUN_DIR, "top_delitos_provincia_hist.csv"), index=False
    )
if top_prov_12m is not None:
    top_prov_12m.to_csv(
        os.path.join(RUN_DIR, "top_delitos_provincia_12m.csv"), index=False
    )

# -----------------------------
# D) Guardar metadata del batch
# -----------------------------
meta = {
    "run_id": RUN_ID,
    "files": [os.path.basename(x) for x in files],
    "rows": int(len(data)),
    "date_min": str(data["Fecha"].min()),
    "date_max": str(data["Fecha"].max()),
    "best_model": best_name,
    "recent_window_months": RECENT_WINDOW_MONTHS,
    "recent_blend": RECENT_BLEND,
    "test_decay": TEST_DECAY,
    "H_test": H,
    "forecast_h": FORECAST_H,
    "forecast_h_long": FORECAST_H_LONG,
}
with open(os.path.join(RUN_DIR, "meta.json"), "w", encoding="utf-8") as f:
    json.dump(meta, f, indent=2, ensure_ascii=False)

print("[ok] Batch guardado en:", RUN_DIR)
print("[ok] Forecast 3 meses (float):\n", future_pred_3m)
print("[ok] Forecast 3 meses (int):\n", future_pred_3m_int)
print("[ok] Forecast 60 meses (float) head/tail:\n", future_pred_60m.head(3), "\n...", future_pred_60m.tail(3))
print("[ok] Forecast 60 meses (int) guardado en forecast_60m_int.csv")


# %%
# ============================================================
# 15) (NUEVO) "SERVING SIMULADO": cargar lo último sin recalcular
#   - esto es lo que luego haría Flask en milisegundos
# ============================================================


def get_latest_run(artifacts_dir="artifacts"):
    if not os.path.exists(artifacts_dir):
        return None
    runs = [
        d
        for d in os.listdir(artifacts_dir)
        if os.path.isdir(os.path.join(artifacts_dir, d))
    ]
    if not runs:
        return None
    runs.sort()
    return os.path.join(artifacts_dir, runs[-1])


LATEST = get_latest_run(ARTIFACTS_DIR)
print("Latest run:", LATEST)

if LATEST:
    fc = pd.read_csv(os.path.join(LATEST, "forecast_3m.csv"), index_col=0)
    print("Forecast 3m (desde artifacts):")
    print(fc)


# %%
# ============================================================
# 16) MAPA “HEAT” POR PROVINCIA (MEJORADO CON FILTROS).
# ============================================================

PROV_CENTERS = {
    "SAN JOSE": (9.932, -84.080),
    "ALAJUELA": (10.016, -84.217),
    "CARTAGO": (9.864, -83.919),
    "HEREDIA": (10.002, -84.116),
    "GUANACASTE": (10.495, -85.354),
    "PUNTARENAS": (9.976, -84.833),
    "LIMON": (9.991, -83.036),
}


def plot_heat_provincia_bubbles_pro(
    agg_prov, month=None, delito=None, provincia=None, out_html=None, theme="dark"
):
    """
    Mapa por provincia mejorado, sin GeoJSON, con filtros.

    Parámetros:
    - agg_prov: DataFrame con columnas Mes, Provincia, Delito, incidentes
    - month: mes a mostrar; si None usa el último
    - delito: filtro por delito exacto
    - provincia: filtro por provincia exacta
    - out_html: ruta HTML de salida
    - theme: 'dark' o 'light'
    """
    if agg_prov is None or agg_prov.empty:
        raise ValueError("agg_prov está vacío. Revisá el paso 4B.")

    tmp = agg_prov.copy()
    tmp["Mes"] = pd.to_datetime(tmp["Mes"], errors="coerce")

    # Mes objetivo
    if month is None:
        month = tmp["Mes"].max()
    month = pd.to_datetime(month)

    tmp = tmp[tmp["Mes"] == month]

    # Filtro por delito
    if delito is not None:
        tmp = tmp[tmp["Delito"] == delito]

    # Filtro por provincia
    if provincia is not None:
        tmp = tmp[
            tmp["Provincia"].astype(str).str.upper().str.strip()
            == str(provincia).upper().strip()
        ]

    # Sumar por provincia
    prov = (
        tmp.groupby("Provincia", as_index=False)["incidentes"]
        .sum()
        .sort_values("incidentes", ascending=False)
        .copy()
    )

    # Lat/Lon
    prov["lat"] = prov["Provincia"].map(
        lambda x: PROV_CENTERS.get(str(x).upper(), (np.nan, np.nan))[0]
    )
    prov["lon"] = prov["Provincia"].map(
        lambda x: PROV_CENTERS.get(str(x).upper(), (np.nan, np.nan))[1]
    )
    prov = prov.dropna(subset=["lat", "lon"]).copy()

    if prov.empty:
        raise ValueError("No hay datos para los filtros seleccionados.")

    # Escala de tamaños
    max_inc = prov["incidentes"].max()
    min_inc = prov["incidentes"].min()

    if max_inc == min_inc:
        prov["size"] = 35
    else:
        prov["size"] = (
            18 + 42 * ((prov["incidentes"] - min_inc) / (max_inc - min_inc)) ** 0.65
        )

    # Hover
    prov["hover"] = prov.apply(
        lambda r: (
            f"<b>{r['Provincia']}</b><br>"
            f"Incidentes: {int(r['incidentes']):,}<br>"
            f"Lat: {r['lat']:.3f}<br>"
            f"Lon: {r['lon']:.3f}"
        ),
        axis=1,
    )

    # Tema
    if theme == "dark":
        paper_bg = "#0d1117"
        geo_bg = "#0d1117"
        land = "#1f2937"
        country = "#4b5563"
        coastline = "#6b7280"
        font_col = "white"
        title_col = "white"
        value_col = "white"
    else:
        paper_bg = "white"
        geo_bg = "white"
        land = "#f3f4f6"
        country = "#9ca3af"
        coastline = "#9ca3af"
        font_col = "#111827"
        title_col = "#111827"
        value_col = "#111827"

    # Título dinámico
    title = (
        f"Mapa de intensidad de incidentes por provincia - {month.strftime('%Y-%m')}"
    )
    subt = []

    if delito is not None:
        subt.append(f"Delito: {delito}")
    if provincia is not None:
        subt.append(f"Provincia: {provincia}")

    if subt:
        title += "<br><sup>" + " | ".join(subt) + "</sup>"

    fig = go.Figure()

    # Glow
    fig.add_trace(
        go.Scattergeo(
            lon=prov["lon"],
            lat=prov["lat"],
            hovertext=prov["hover"],
            hoverinfo="text",
            mode="markers",
            marker=dict(
                size=prov["size"] * 1.9,
                color=prov["incidentes"],
                colorscale="YlOrRd",
                opacity=0.15,
                line=dict(width=0),
            ),
            showlegend=False,
        )
    )

    # Burbuja principal
    fig.add_trace(
        go.Scattergeo(
            lon=prov["lon"],
            lat=prov["lat"],
            hovertext=prov["hover"],
            hoverinfo="text",
            mode="markers+text",
            text=prov["Provincia"],
            textposition="top center",
            textfont=dict(size=11, color=font_col, family="Arial, sans-serif"),
            marker=dict(
                size=prov["size"],
                color=prov["incidentes"],
                colorscale="YlOrRd",
                opacity=0.88,
                line=dict(color="white", width=1.3),
                colorbar=dict(
                    title=dict(text="Incidentes"), thickness=18, len=0.7, x=0.96
                ),
            ),
            name="Incidentes",
        )
    )

    # Valor encima
    fig.add_trace(
        go.Scattergeo(
            lon=prov["lon"],
            lat=prov["lat"],
            mode="text",
            text=prov["incidentes"].astype(int).astype(str),
            textfont=dict(size=10, color=value_col, family="Arial, sans-serif"),
            showlegend=False,
            hoverinfo="skip",
        )
    )

    fig.update_layout(
        title=dict(
            text=title,
            x=0.5,
            xanchor="center",
            font=dict(size=18, family="Arial, sans-serif", color=title_col),
        ),
        paper_bgcolor=paper_bg,
        plot_bgcolor=paper_bg,
        font=dict(family="Arial, sans-serif", color=font_col, size=12),
        margin=dict(l=20, r=20, t=72, b=20),
        width=1000,
        height=760,
        geo=dict(
            scope="north america",
            projection_type="mercator",
            showland=True,
            landcolor=land,
            showcountries=True,
            countrycolor=country,
            showcoastlines=True,
            coastlinecolor=coastline,
            showocean=True,
            oceancolor=geo_bg,
            bgcolor=geo_bg,
            lonaxis=dict(range=[-86.2, -82.4]),
            lataxis=dict(range=[8.0, 11.4]),
        ),
    )

    # Salida
    if out_html is None:
        suf = month.strftime("%Y_%m")
        partes = [f"heat_prov_pro_{suf}"]

        if delito is not None:
            delito_safe = re.sub(r"[^A-Z0-9]+", "_", str(delito).upper())
            partes.append(delito_safe)

        if provincia is not None:
            prov_safe = re.sub(r"[^A-Z0-9]+", "_", str(provincia).upper())
            partes.append(prov_safe)

        out_html = os.path.join(RUN_DIR, "_".join(partes) + ".html")

    os.makedirs(os.path.dirname(out_html), exist_ok=True)

    # auto_open=False: evita lanzar el navegador al generar heatmaps desde el pipeline en servidor/IDE.
    fig.write_html(
        out_html, auto_open=False, config={"displayModeBar": True, "scrollZoom": True}
    )

    print("[ok] Mapa mejorado guardado en:", os.path.abspath(out_html))
    return prov


# %%
prov_tabla = plot_heat_provincia_bubbles_pro(
    agg_prov,
    month=None,
    delito=None,
    provincia=None,
    theme="light",
)

# Si querés un mes fijo:
# plot_heat_provincia_bubbles(agg_prov, month="2025-12-01", delito=None)

# Si querés filtrar un delito exacto (tiene que existir igualito en tus datos):
# plot_heat_provincia_bubbles(agg_prov, month="2025-12-01", delito="DELITOS AMBIENTALES")

# %%
