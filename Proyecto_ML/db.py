"""
Persistencia SQLite para Proyecto_ML (tabla delitos).
Rutas relativas al directorio del proyecto (mismo nivel que ML1.py).
"""
from __future__ import annotations

import hashlib
import html
import os
import re
import sqlite3
from typing import Any, Optional, Tuple

import numpy as np
import pandas as pd

_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_PROJECT_ROOT, "data.db")

PROVINCIAS_CR = {
    "SAN JOSE",
    "ALAJUELA",
    "CARTAGO",
    "HEREDIA",
    "GUANACASTE",
    "PUNTARENAS",
    "LIMON",
}


def get_connection() -> sqlite3.Connection:
    """Abre data.db, crea el archivo si no existe y asegura la tabla delitos."""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
    except sqlite3.Error:
        pass
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS delitos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            row_hash TEXT NOT NULL UNIQUE,
            delito TEXT,
            subdelito TEXT,
            fecha TEXT NOT NULL,
            hora_rango TEXT,
            victima TEXT,
            edad TEXT,
            sexo TEXT,
            nacionalidad TEXT,
            provincia TEXT,
            canton TEXT,
            distrito TEXT,
            hora_inicio REAL,
            source_file TEXT
        )
        """
    )
    conn.commit()


def read_oij_csv_robust(file_path: str, encoding: str = "utf-8") -> pd.DataFrame:
    """Lee CSV estilo OIJ (misma lógica que ML1.py)."""
    rows = []
    with open(file_path, "r", encoding=encoding, errors="replace") as f:
        _header = f.readline()
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            if line.endswith(","):
                line = line[:-1]

            parts = line.rsplit(",", maxsplit=9)
            if len(parts) < 10:
                continue

            left_text = parts[0]
            tail = parts[1:]

            if "," in left_text:
                delito, subdelito = left_text.split(",", 1)
            else:
                delito, subdelito = left_text, ""

            row = [delito, subdelito] + tail
            row = [html.unescape(str(x)).strip() for x in row]
            rows.append(row)

    return pd.DataFrame(
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


def normalize_oij_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza columnas OIJ (misma lógica que ML1.py)."""
    df = df.copy()

    shifted = (
        df["Canton"].astype(str).str.upper().isin(PROVINCIAS_CR).mean() > 0.80
    )

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

    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")

    for c in df.columns:
        if df[c].dtype == object:
            s = df[c].astype(str).map(lambda x: html.unescape(x).strip())
            s = s.replace({"": np.nan, "None": np.nan, "nan": np.nan})
            df[c] = s

    def _parse_start_hour(x: Any) -> float:
        if not isinstance(x, str):
            return np.nan
        m = re.match(r"^\s*(\d{2}):(\d{2}):(\d{2})\s*-", x)
        return float(int(m.group(1))) if m else np.nan

    df["Hora_Inicio"] = df["Hora_Rango"].apply(_parse_start_hour)

    return df


def _row_hash(
    delito: Any,
    subdelito: Any,
    fecha_iso: str,
    hora_rango: Any,
    victima: Any,
    edad: Any,
    sexo: Any,
    nacionalidad: Any,
    provincia: Any,
    canton: Any,
    distrito: Any,
) -> str:
    parts = [
        str(delito or ""),
        str(subdelito or ""),
        fecha_iso,
        str(hora_rango or ""),
        str(victima or ""),
        str(edad or ""),
        str(sexo or ""),
        str(nacionalidad or ""),
        str(provincia or ""),
        str(canton or ""),
        str(distrito or ""),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _fecha_iso(val: Any) -> Optional[str]:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    ts = pd.Timestamp(val)
    if pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%d")


def _insert_normalized_rows(work: pd.DataFrame) -> Tuple[int, int]:
    """Inserta filas ya normalizadas; evita duplicados con row_hash UNIQUE + INSERT OR IGNORE."""
    conn = get_connection()
    inserted = 0
    skipped = 0
    try:
        cur = conn.cursor()
        sql = """
        INSERT OR IGNORE INTO delitos (
            row_hash, delito, subdelito, fecha, hora_rango, victima, edad, sexo, nacionalidad,
            provincia, canton, distrito, hora_inicio, source_file
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        for _, row in work.iterrows():
            fecha_iso = _fecha_iso(row.get("Fecha"))
            if not fecha_iso:
                skipped += 1
                continue

            hi = row.get("Hora_Inicio")
            if hi is None or (isinstance(hi, float) and np.isnan(hi)):
                hora_inicio = None
            else:
                try:
                    hora_inicio = float(hi)
                except (TypeError, ValueError):
                    hora_inicio = None

            src = row.get("source_file")
            if src is None or (isinstance(src, float) and np.isnan(src)):
                src_s = ""
            else:
                src_s = str(src)

            rh = _row_hash(
                row.get("Delito"),
                row.get("SubDelito"),
                fecha_iso,
                row.get("Hora_Rango"),
                row.get("Victima"),
                row.get("Edad"),
                row.get("Sexo"),
                row.get("Nacionalidad"),
                row.get("Provincia"),
                row.get("Canton"),
                row.get("Distrito"),
            )

            cur.execute(
                sql,
                (
                    rh,
                    str(row.get("Delito") or ""),
                    str(row.get("SubDelito") or ""),
                    fecha_iso,
                    str(row.get("Hora_Rango") or "")
                    if pd.notna(row.get("Hora_Rango"))
                    else "",
                    str(row.get("Victima") or "")
                    if pd.notna(row.get("Victima"))
                    else "",
                    str(row.get("Edad") or "")
                    if pd.notna(row.get("Edad"))
                    else "",
                    str(row.get("Sexo") or "")
                    if pd.notna(row.get("Sexo"))
                    else "",
                    str(row.get("Nacionalidad") or "")
                    if pd.notna(row.get("Nacionalidad"))
                    else "",
                    str(row.get("Provincia") or "")
                    if pd.notna(row.get("Provincia"))
                    else "",
                    str(row.get("Canton") or "")
                    if pd.notna(row.get("Canton"))
                    else "",
                    str(row.get("Distrito") or "")
                    if pd.notna(row.get("Distrito"))
                    else "",
                    hora_inicio,
                    src_s,
                ),
            )
            if cur.rowcount == 1:
                inserted += 1
            else:
                skipped += 1
        conn.commit()
        return inserted, skipped
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def insert_dataframe(df: pd.DataFrame, source_file: Optional[str] = None) -> Tuple[int, int]:
    """
    Inserta un DataFrame en delitos.
    Acepta esquema crudo OIJ (post read_oij_csv_robust) o ya normalizado (con Hora_Rango).
    """
    try:
        if df is None or df.empty:
            return 0, 0
        work = df.copy()
        if "Hora_Rango" not in work.columns:
            need = {
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
            }
            if not need.issubset(set(work.columns)):
                raise ValueError(
                    "Columnas insuficientes para normalizar; use insert_from_csv_path()."
                )
            work = normalize_oij_schema(work)
        if source_file is not None:
            work["source_file"] = source_file
        elif "source_file" not in work.columns:
            work["source_file"] = ""
        return _insert_normalized_rows(work)
    except Exception:
        raise


def insert_from_csv_path(path: str, source_file: Optional[str] = None) -> Tuple[int, int]:
    """Lee CSV con parser OIJ robusto e inserta filas normalizadas."""
    try:
        base = os.path.basename(path) if source_file is None else source_file
        raw = read_oij_csv_robust(path)
        norm = normalize_oij_schema(raw)
        norm["source_file"] = base
        return _insert_normalized_rows(norm)
    except Exception:
        raise


def get_all_data() -> pd.DataFrame:
    """
    Devuelve todas las filas como DataFrame con columnas compatibles con ML1.py
    (nombres en PascalCase y Fecha como datetime).
    """
    expected_cols = [
        "Delito",
        "SubDelito",
        "Fecha",
        "Hora_Rango",
        "Victima",
        "Edad",
        "Sexo",
        "Nacionalidad",
        "Provincia",
        "Canton",
        "Distrito",
        "Hora_Inicio",
        "source_file",
    ]
    try:
        conn = get_connection()
        df = pd.read_sql_query(
            """
            SELECT delito, subdelito, fecha, hora_rango, victima, edad, sexo, nacionalidad,
                   provincia, canton, distrito, hora_inicio, source_file
            FROM delitos
            """,
            conn,
        )
        conn.close()
        if df.empty:
            return pd.DataFrame(columns=expected_cols)
        df = df.rename(
            columns={
                "delito": "Delito",
                "subdelito": "SubDelito",
                "fecha": "Fecha",
                "hora_rango": "Hora_Rango",
                "victima": "Victima",
                "edad": "Edad",
                "sexo": "Sexo",
                "nacionalidad": "Nacionalidad",
                "provincia": "Provincia",
                "canton": "Canton",
                "distrito": "Distrito",
                "hora_inicio": "Hora_Inicio",
                "source_file": "source_file",
            }
        )
        df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
        return df
    except Exception as e:
        print("db.get_all_data:", e)
        return pd.DataFrame(columns=expected_cols)
