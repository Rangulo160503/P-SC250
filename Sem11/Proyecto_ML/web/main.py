import flask as fl
from werkzeug.utils import secure_filename
import os
import re
import subprocess
import warnings
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from pandas.api import types as pdt
import sys
import json
import hmac
import hashlib
import threading
import shutil
import uuid
from datetime import datetime
from html import escape as html_escape

_WEB_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_WEB_DIR, ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

app = fl.Flask(__name__)
app.secret_key = "your_secret_key_here"
# Límite total del cuerpo (multipart con varios CSV). Ajustar también nginx: client_max_body_size.
app.config["MAX_CONTENT_LENGTH"] = 300 * 1024 * 1024
# Tamaño máximo por parte/archivo cuando el cliente envía Content-Length por archivo.
_MAX_PER_FILE_CSV_BYTES = 100 * 1024 * 1024

ALLOWED_EXTENSIONS = {"csv"}
DEFAULT_SAVE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "data")
)
ARTIFACTS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "artifacts")
)
HISTORY_FILE = os.path.join(os.path.dirname(__file__), "run_history.json")
HISTORY_MAX = 25
_history_file_lock = threading.Lock()
_history_nombre_backfill_done = False

# Per-run_id + mode memoization for /types (``analysis_cache_key``).
_types_cache = {}

# Per-run_id + mode memoization for /relations.
_relations_cache = {}

ANALYTICAL_MODE_SESSION_KEY = "analytical_mode"
# CSV elegido en Carga de datos (tarjetas) → usado en ``/run`` para ejecutar el pipeline.
SESSION_ACTIVE_CSV_KEY = "active_csv_dataset"
# ``single`` = ML1 modo csv + archivo activo; ``all`` = ML1 modo all_csv (todos data/*.csv).
SESSION_ML_CSV_STRATEGY = "ml_csv_strategy"

# Salida de fusión de varios CSV antes de un único ``ML1.py csv …``.
MERGED_DATASET_BASENAME = "merged_dataset.csv"


def get_analytical_mode():
    """Modo de análisis (siempre ``normal``; ``experimental`` quedó retirado de la UI)."""
    m = fl.session.get(ANALYTICAL_MODE_SESSION_KEY, "normal")
    if m == "experimental":
        return "normal"
    return m if m == "normal" else "normal"


def analysis_cache_key(run_id, mode=None):
    """Clave de caché ``{run_id}_{mode}``; ``None`` si ``run_id`` no es seguro."""
    if not run_id or not is_safe_run_id(str(run_id)):
        return None
    return f"{run_id}_normal"


@app.errorhandler(413)
def request_entity_too_large(_error):
    """413 suele ser HTML por defecto; devolvemos JSON para que el cliente `fetch` no falle al parsear."""
    return fl.jsonify(
        {
            "ok": False,
            "error": "El archivo o conjunto de archivos excede el tamaño permitido (300 MB en total).",
        }
    ), 413


@app.before_request
def _sync_analytical_mode_from_query():
    m = fl.request.args.get("mode")
    if m == "experimental":
        fl.session[ANALYTICAL_MODE_SESSION_KEY] = "normal"
        print("[mode] query mode=experimental ignored (retirado); using normal")
    elif m == "normal":
        fl.session[ANALYTICAL_MODE_SESSION_KEY] = "normal"
        print("[mode] set analytical_mode=normal (query)")


def is_safe_run_id(rid):
    if not rid or not isinstance(rid, str) or len(rid) > 80:
        return False
    for c in rid:
        if not (c.isalnum() or c in "_-"):
            return False
    return True


def _default_run_nombre(iso_ts, run_id):
    d = (iso_ts or "")[:10]
    if (not d) and run_id and len(str(run_id)) >= 8 and str(run_id)[:8].isdigit():
        rid = str(run_id)
        d = f"{rid[:4]}-{rid[4:6]}-{rid[6:8]}"
    if not d:
        d = datetime.now().strftime("%Y-%m-%d")
    return f"Ejecución - {d}"


def _load_history_raw():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
        except (json.JSONDecodeError, OSError):
            return []
    return []


def _save_history_raw(history):
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2)


def load_history():
    """Lista persistente de ejecuciones (con bloqueo y relleno único de ``nombre``)."""
    global _history_nombre_backfill_done
    with _history_file_lock:
        data = _load_history_raw()
        if not _history_nombre_backfill_done:
            changed = False
            for row in data:
                if not isinstance(row, dict):
                    continue
                if not str(row.get("nombre") or "").strip():
                    row["nombre"] = _default_run_nombre(
                        row.get("timestamp") or "", row.get("run_id") or ""
                    )
                    changed = True
            if changed:
                _save_history_raw(data)
            _history_nombre_backfill_done = True
        return data


def save_history(history):
    with _history_file_lock:
        _save_history_raw(history)


def normalize_history_row(row):
    rid = (row.get("run_id") or "").strip()
    ts = row.get("timestamp") or ""
    nombre = (row.get("nombre") or "").strip()
    if not nombre:
        nombre = _default_run_nombre(ts, rid)
    return {
        "run_id": rid,
        "nombre": nombre,
        "timestamp": ts,
        "best_model": row.get("best_model"),
        "wrmse": row.get("wrmse"),
        "artifacts_dir": row.get("artifacts_dir") or rid,
        "source_mode": row.get("source_mode") or "auto",
        "source_file": row.get("source_file") or "",
    }


def history_sidebar():
    rows = [normalize_history_row(r) for r in load_history()]
    rows.sort(key=lambda x: x["timestamp"], reverse=True)
    return rows


def _dataset_subtitle(row):
    mode = (row.get("source_mode") or "").strip() or "auto"
    fn = (row.get("source_file") or "").strip()
    if fn:
        return f"{mode} · {fn}"
    if mode and mode != "auto":
        return mode
    return "Sin archivo de origen"


def _fmt_run_sidebar_datetime(iso_ts):
    if not iso_ts:
        return ""
    s = str(iso_ts).replace("T", " ")
    return s[:16]


def resolve_sidebar_active_run_id():
    """Run cuyos artefactos se consideran «activos» para resaltar en el historial."""
    rv = getattr(fl.request, "view_args", None) or {}
    rid = rv.get("run_id")
    if rid and is_safe_run_id(str(rid)):
        return str(rid)
    ep = fl.request.endpoint
    latest_stack = (
        "show_forecast",
        "show_metrics",
        "show_clustering",
        "show_relations",
        "show_charts",
        "show_types",
        "dashboard",
    )
    if ep in latest_stack:
        lr = get_latest_run()
        if lr:
            return os.path.basename(lr)
    return ""


def runs_api_payload(active_run_id=None):
    """Lista para JSON (más reciente primero). ``active_run_id`` marca la fila activa."""
    aid = (active_run_id or "").strip()
    out = []
    for row in history_sidebar():
        rid = row.get("run_id")
        if not rid:
            continue
        if not os.path.isdir(os.path.join(ARTIFACTS_DIR, rid)):
            continue
        out.append(
            {
                "id": rid,
                "nombre": row.get("nombre") or _default_run_nombre(
                    row.get("timestamp") or "", rid
                ),
                "fecha": _fmt_run_sidebar_datetime(row.get("timestamp")),
                "dataset": _dataset_subtitle(row),
                "url": fl.url_for("results_run", run_id=rid),
                "is_active": bool(aid and aid == rid),
            }
        )
    return out


def add_to_history(
    run_id,
    best_model,
    wrmse,
    source_mode="auto",
    source_file="",
    nombre=None,
    *,
    pipeline_session_id=None,
    skip_if_duplicate=True,
):
    """
    Añade una fila al historial persistente.

    ``skip_if_duplicate``: si ya existe ``run_id`` en el historial, no vuelve a añadir.
    ``pipeline_session_id``: correlación de una corrida end-to-end (opcional, JSON).
    """
    rid = str(run_id or "").strip()
    if skip_if_duplicate and rid and run_id_in_history(rid):
        print(f"[history] omitido: run_id ya registrado ({rid!r})")
        return False
    ts = datetime.now().isoformat()
    entry = {
        "run_id": run_id,
        "nombre": (nombre or "").strip()
        or _default_run_nombre(ts, run_id),
        "timestamp": ts,
        "best_model": best_model,
        "wrmse": wrmse,
        "artifacts_dir": run_id,
        "source_mode": source_mode,
        "source_file": source_file or "",
    }
    if pipeline_session_id:
        entry["pipeline_session_id"] = str(pipeline_session_id).strip()
    history = load_history()
    history.append(entry)
    history = history[-HISTORY_MAX:]
    save_history(history)
    return True


def _register_history_from_execute_output(out, *, skip_if_duplicate=True):
    """Una sola escritura al historial a partir del resultado de ``_execute_ml1_for_csv_dataset``."""
    if not out.get("ok") or not isinstance(out.get("history"), dict):
        return False
    h = out["history"]
    return bool(
        add_to_history(
            h["run_id"],
            h["best_model"],
            h["wrmse"],
            source_mode=h.get("source_mode") or "auto",
            source_file=h.get("source_file") or "",
            pipeline_session_id=h.get("pipeline_session_id"),
            skip_if_duplicate=skip_if_duplicate,
        )
    )


@app.context_processor
def inject_sidebar():
    mode = get_analytical_mode()
    return {
        "sidebar_active_run_id": resolve_sidebar_active_run_id(),
        "analytical_mode": mode,
        "is_experimental": False,
        "has_relations_heatmap": _latest_run_has_relations_heatmap(),
    }


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def list_csv_files():
    if not os.path.exists(DEFAULT_SAVE_DIR):
        return []
    return [f for f in os.listdir(DEFAULT_SAVE_DIR) if f.endswith(".csv")]


def _csv_basename_on_disk(name):
    """Nombre de CSV que existe realmente en ``data/`` (sin rutas)."""
    if not name or not isinstance(name, str):
        return None
    n = name.strip()
    if not n or n != os.path.basename(n):
        return None
    if os.sep in n or (os.altsep and os.altsep in n):
        return None
    if n not in list_csv_files():
        return None
    return n


def _read_csv_for_merge(path):
    """Lectura tolerante para concatenar CSV de ``data/`` (encoding / líneas raras)."""
    try:
        return pd.read_csv(
            path,
            encoding="utf-8",
            encoding_errors="replace",
            low_memory=False,
            on_bad_lines="skip",
        )
    except TypeError:
        try:
            return pd.read_csv(path, encoding="utf-8", low_memory=False)
        except Exception:
            return pd.read_csv(path, encoding="latin-1", low_memory=False)
    except Exception:
        try:
            return pd.read_csv(path, encoding="latin-1", low_memory=False)
        except Exception as e2:
            print(f"[merge] lectura fallida {path!r}: {e2}")
            raise


def merge_csv_datasets(file_names):
    """
    Une varios CSV existentes en ``DEFAULT_SAVE_DIR`` en un solo
    ``MERGED_DATASET_BASENAME`` y devuelve ese basename.

    No ejecuta ML1; solo escribe el archivo unificado (sobrescribe si ya existía).
    """
    ordered = []
    for raw in file_names or []:
        if not raw or not isinstance(raw, str):
            continue
        fn = _csv_basename_on_disk(raw.strip())
        if not fn:
            continue
        if fn.lower() == MERGED_DATASET_BASENAME.lower():
            continue
        if fn not in ordered:
            ordered.append(fn)
    if not ordered:
        raise ValueError("No hay CSV válidos para unir (excluyendo el consolidado previo).")

    dfs = []
    for fn in ordered:
        path = os.path.join(DEFAULT_SAVE_DIR, fn)
        try:
            dfs.append(_read_csv_for_merge(path))
        except Exception as e:
            print(f"[merge] error leyendo {path!r}: {e}")
            raise

    df_all = pd.concat(dfs, ignore_index=True)

    date_keys = ("fecha", "date", "timestamp")
    for col in df_all.columns:
        key = str(col).strip().lower()
        if key in date_keys:
            try:
                df_all = df_all.copy()
                df_all[col] = pd.to_datetime(df_all[col], errors="coerce")
                df_all = df_all.sort_values(col, na_position="last")
                break
            except Exception:
                pass

    merged_path = os.path.join(DEFAULT_SAVE_DIR, MERGED_DATASET_BASENAME)
    os.makedirs(DEFAULT_SAVE_DIR, exist_ok=True)
    df_all.to_csv(merged_path, index=False)
    print(
        f"[merge] dataset unificado creado: {MERGED_DATASET_BASENAME} "
        f"({len(df_all)} filas, desde {len(ordered)} archivo(s))"
    )
    return MERGED_DATASET_BASENAME


def persist_uploaded_csv_filestorage(csv_file, set_active=True):
    """
    Guarda un ``FileStorage`` en ``data/`` e intenta insertar en SQLite.
    Retorna ``{"ok", "message", "status", "filename"}``; ``filename`` solo en respuestas exitosas.
    """
    out = {
        "ok": False,
        "message": "",
        "status": "danger",
        "filename": None,
    }
    if csv_file is None or getattr(csv_file, "filename", None) in (None, ""):
        out["message"] = "Nombre vacío."
        return out
    if not allowed_file(csv_file.filename):
        out["message"] = "Solo .csv"
        return out
    filename = secure_filename(csv_file.filename)
    if not filename:
        out["message"] = "Nombre inválido."
        return out
    target_file = os.path.join(DEFAULT_SAVE_DIR, filename)
    try:
        os.makedirs(DEFAULT_SAVE_DIR, exist_ok=True)
        csv_file.save(target_file)
    except Exception as e:
        out["message"] = str(e)
        return out
    message = "Subido."
    try:
        import db as dbmod

        ins, skp = 0, 0
        try:
            df_csv = pd.read_csv(target_file)
            ins, skp = dbmod.insert_dataframe(df_csv, source_file=filename)
        except Exception:
            ins, skp = dbmod.insert_from_csv_path(
                target_file, source_file=filename
            )
        if ins or skp:
            message += f" {ins} nuevos, {skp} omitidos."
    except Exception as db_err:
        message += f" ({db_err})"
    out["ok"] = True
    out["message"] = message
    out["status"] = "success"
    out["filename"] = filename
    if set_active:
        fl.session[SESSION_ACTIVE_CSV_KEY] = filename
    return out


def persist_multiple_csv_filestorage_list(filestorages):
    """
    Guarda varios ``FileStorage`` en ``data/``. La sesión queda con el **último**
    nombre subido correctamente (mismo criterio que una sola subida reciente).

    Retorna ``ok`` si al menos un archivo se guardó; ``filenames`` / ``last_filename``,
    ``failed`` lista de ``(nombre_original, mensaje)``.
    """
    out = {
        "ok": False,
        "message": "",
        "status": "danger",
        "filenames": [],
        "last_filename": None,
        "failed": [],
    }
    rows = [f for f in filestorages if f and getattr(f, "filename", None)]
    for f in rows:
        cl = getattr(f, "content_length", None)
        if cl is not None and int(cl) > _MAX_PER_FILE_CSV_BYTES:
            fn = getattr(f, "filename", "") or "?"
            mb = _MAX_PER_FILE_CSV_BYTES // (1024 * 1024)
            out["message"] = f"{fn} es demasiado grande (máximo {mb} MB por archivo)."
            return out
    if not rows:
        out["message"] = "No se seleccionaron archivos."
        return out
    oks = []
    fails = []
    for f in rows:
        res = persist_uploaded_csv_filestorage(f, set_active=False)
        if res["ok"]:
            oks.append(res["filename"])
        else:
            fails.append((getattr(f, "filename", "") or "?", res.get("message") or "Error"))
    out["filenames"] = oks
    out["failed"] = fails
    if oks:
        fl.session[SESSION_ACTIVE_CSV_KEY] = oks[-1]
        out["last_filename"] = oks[-1]
        out["ok"] = True
        out["status"] = "success"
        out["message"] = f"{len(oks)} archivo(s) subido(s) correctamente."
        if fails:
            out["message"] += f" {len(fails)} archivo(s) no se pudieron subir."
    else:
        out["message"] = "; ".join(f"{a}: {b}" for a, b in fails) if fails else "Ningún archivo válido."
    return out


def _upload_filestorage_list_from_request():
    """Unifica ``files`` (múltiple) y ``csv_file`` (compatibilidad)."""
    files = fl.request.files.getlist("files")
    files = [f for f in files if f and getattr(f, "filename", None)]
    if not files and "csv_file" in fl.request.files:
        leg = fl.request.files["csv_file"]
        if leg and getattr(leg, "filename", None):
            files = [leg]
    return files


def _paths_from_history_rows(rows):
    """Ordena por ``timestamp`` descendente y devuelve rutas ``artifacts/<run_id>/`` existentes."""
    norm = [normalize_history_row(r) for r in rows if isinstance(r, dict)]
    norm.sort(key=lambda x: x.get("timestamp") or "", reverse=True)
    out = []
    seen = set()
    for row in norm:
        rid = (row.get("run_id") or "").strip()
        if not rid or rid in seen or not is_safe_run_id(rid):
            continue
        full = os.path.join(ARTIFACTS_DIR, rid)
        if os.path.isdir(full):
            seen.add(rid)
            out.append(full)
    return out


def get_valid_history_run_paths_ordered():
    """Carpetas de run válidas: solo entradas de ``run_history.json`` con directorio existente (más reciente primero)."""
    return _paths_from_history_rows(load_history())


def run_id_in_history(run_id):
    rid = str(run_id or "").strip()
    if not rid:
        return False
    for row in load_history():
        if isinstance(row, dict) and str(row.get("run_id")) == rid:
            return True
    return False


def get_latest_run():
    """Ruta absoluta del run más reciente según **solo** ``run_history.json`` (sin heurística mtime ni orfanos)."""
    if not os.path.isdir(ARTIFACTS_DIR):
        print(f"[artifacts] get_latest_run: not a directory: {ARTIFACTS_DIR!r}")
        return None
    paths = get_valid_history_run_paths_ordered()
    if not paths:
        print("[artifacts] get_latest_run: no valid run in history")
        return None
    chosen = paths[0]
    print("[artifacts] get_latest_run:", os.path.basename(chosen), "(history)")
    return chosen


def _newest_artifact_dir_mtime():
    """
    Carpeta bajo ``artifacts/`` con mtime más reciente.
    Solo para enlazar la salida de ``ML1.py`` con ``add_to_history`` antes de que exista fila en el historial.
    """
    if not os.path.isdir(ARTIFACTS_DIR):
        return None
    paths = []
    try:
        for name in os.listdir(ARTIFACTS_DIR):
            full = os.path.join(ARTIFACTS_DIR, name)
            if os.path.isdir(full) and is_safe_run_id(name):
                paths.append(full)
    except OSError:
        return None
    if not paths:
        return None
    paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return paths[0]


def _snapshot_artifact_run_ids():
    """Nombres de carpeta bajo ``artifacts/`` que cumplen ``is_safe_run_id``."""
    if not os.path.isdir(ARTIFACTS_DIR):
        return set()
    out = set()
    try:
        for name in os.listdir(ARTIFACTS_DIR):
            if is_safe_run_id(name) and os.path.isdir(os.path.join(ARTIFACTS_DIR, name)):
                out.add(name)
    except OSError:
        pass
    return out


def _resolve_run_dir_after_ml1(before_ids):
    """
    Después de ``ML1.py``, detecta la carpeta nueva comparando con ``before_ids``.
    Si hay varias carpetas nuevas, se usa la de ``mtime`` más reciente.
    """
    after = _snapshot_artifact_run_ids()
    new = after - before_ids
    if len(new) == 1:
        rid = next(iter(new))
        return os.path.join(ARTIFACTS_DIR, rid)
    if len(new) > 1:
        paths = [os.path.join(ARTIFACTS_DIR, n) for n in new]
        paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        return paths[0]
    return _newest_artifact_dir_mtime()


def _execute_ml1_for_csv_dataset(
    csv_basename: str,
    *,
    history_source_mode=None,
    history_source_file=None,
    pipeline_session_id=None,
):
    """
    Ejecuta ``ML1.py csv <archivo>`` para un CSV en ``data/`` (sin modificar ML1).

    **No** escribe en el historial de ejecuciones: el llamador debe invocar
    ``add_to_history`` una vez por corrida completa con el bloque ``history``
    devuelto en caso de éxito.

    ``history_source_mode`` / ``history_source_file``: valores a guardar en
    historial (p. ej. dataset fusionado + lista de fuentes); por defecto ``csv``
    y el basename del CSV ejecutado.

    Retorna ``dict`` con ``ok``, ``run_id``, ``dataset``, ``error`` y, si
    ``ok`` es verdadero, ``history`` (argumentos listos para ``add_to_history``).
    """
    fn = _csv_basename_on_disk(csv_basename)
    if not fn:
        return {
            "ok": False,
            "run_id": None,
            "dataset": None,
            "error": "Dataset no encontrado.",
        }
    source_mode = "csv"
    selected_file = fn
    ml_script = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "ML1.py")
    )
    before_ids = _snapshot_artifact_run_ids()
    cmd = [sys.executable, ml_script, source_mode, selected_file]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=os.path.dirname(ml_script),
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
        )
    except Exception as exc:
        return {"ok": False, "run_id": None, "dataset": fn, "error": str(exc)}
    if result.returncode != 0:
        err = (result.stderr or "").strip() or (result.stdout or "").strip()
        return {
            "ok": False,
            "run_id": None,
            "dataset": fn,
            "error": err or "Error de ejecución.",
        }
    latest_run = _resolve_run_dir_after_ml1(before_ids)
    if not latest_run:
        return {
            "ok": False,
            "run_id": None,
            "dataset": fn,
            "error": "No se detectó carpeta de artefactos.",
        }
    rid_new = os.path.basename(latest_run)
    meta_path = os.path.join(latest_run, "meta.json")
    best_model = "Unknown"
    wrmse = None
    meta_rid = rid_new
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as mf:
            meta = json.load(mf)
        best_model = meta.get("best_model", "Unknown")
        meta_rid = meta.get("run_id") or rid_new
        errors_path = os.path.join(latest_run, "errores_modelos.csv")
        if os.path.exists(errors_path):
            errors_df = pd.read_csv(errors_path, index_col=0)
            if best_model in errors_df.index:
                wrmse = errors_df.loc[best_model, "WRMSE"]
    hist_id = meta_rid if is_safe_run_id(str(meta_rid)) else rid_new
    hist_mode = (
        history_source_mode if history_source_mode is not None else source_mode
    )
    hist_file = (
        history_source_file if history_source_file is not None else selected_file
    )
    fl.session[SESSION_ACTIVE_CSV_KEY] = fn
    out_rid = hist_id if is_safe_run_id(str(hist_id)) else rid_new
    return {
        "ok": True,
        "run_id": str(out_rid),
        "dataset": fn,
        "error": None,
        "history": {
            "run_id": hist_id,
            "best_model": best_model,
            "wrmse": wrmse,
            "source_mode": hist_mode,
            "source_file": hist_file,
            "pipeline_session_id": pipeline_session_id,
        },
    }


def render_empty_runs_state(page_title="Análisis", hint=None):
    """Vista cuando no hay ejecuciones registradas o ninguna carpeta válida en historial."""
    mode = get_analytical_mode()
    act = get_contextual_actions(
        "empty_runs", None, None, None, extra={}, analytical_mode=mode
    )
    msg = guidance_message_for_module(
        "empty_runs", None, None, None, extra={}, analytical_mode=mode
    )
    return fl.render_template(
        "empty_runs_state.html",
        page_title=page_title,
        hint=hint
        or "Selecciona un dataset en carga de datos y ejecuta el procesamiento para generar una ejecución.",
        actions=act,
        guidance_message=msg,
        analytical_mode=mode,
    )


def load_forecast_3m(run_dir_path):
    if not run_dir_path:
        print("[artifacts] load_forecast_3m: no run directory")
        return None
    path = os.path.join(run_dir_path, "forecast_3m.csv")
    if not os.path.isfile(path):
        print(f"[artifacts] load_forecast_3m: missing {path}")
        return None
    try:
        df = pd.read_csv(path)
        print(
            "[artifacts] load_forecast_3m: loaded",
            f"rows={len(df)}",
            f"cols={list(df.columns)}",
        )
        return df
    except Exception as exc:
        print(f"[artifacts] load_forecast_3m: failed to read CSV: {exc}")
        return None


def load_forecast_3m_int(run_dir_path):
    if not run_dir_path:
        print("[artifacts] load_forecast_3m_int: no run directory")
        return None
    path = os.path.join(run_dir_path, "forecast_3m_int.csv")
    if not os.path.isfile(path):
        print(f"[artifacts] load_forecast_3m_int: missing {path}")
        return None
    try:
        df = pd.read_csv(path)
        print(
            "[artifacts] load_forecast_3m_int: loaded",
            f"rows={len(df)}",
            f"cols={list(df.columns)}",
        )
        return df
    except Exception as exc:
        print(f"[artifacts] load_forecast_3m_int: failed to read CSV: {exc}")
        return None


def load_errores_modelos(run_dir_path):
    if not run_dir_path:
        print("[artifacts] load_errores_modelos: no run directory")
        return None
    path = os.path.join(run_dir_path, "errores_modelos.csv")
    if not os.path.isfile(path):
        print(f"[artifacts] load_errores_modelos: missing {path}")
        return None
    try:
        df = pd.read_csv(path)
        print(
            "[artifacts] load_errores_modelos: loaded",
            f"rows={len(df)}",
            f"cols={list(df.columns)}",
        )
        return df
    except Exception as exc:
        print(f"[artifacts] load_errores_modelos: failed to read CSV: {exc}")
        return None


def load_clustering_provincia(run_dir_path):
    if not run_dir_path:
        print("[artifacts] load_clustering_provincia: no run directory")
        return None
    path = os.path.join(run_dir_path, "clustering_provincia.csv")
    if not os.path.isfile(path):
        print(f"[artifacts] load_clustering_provincia: missing {path}")
        return None
    try:
        df = pd.read_csv(path)
        print(
            "[artifacts] load_clustering_provincia: loaded",
            f"rows={len(df)}",
            f"cols={list(df.columns)}",
        )
        return df
    except Exception as exc:
        print(f"[artifacts] load_clustering_provincia: failed to read CSV: {exc}")
        return None


def dataframe_to_html_table(df):
    if df is None or getattr(df, "empty", True):
        return None
    try:
        return df.to_html(index=False, escape=True)
    except Exception as exc:
        print(f"[artifacts] dataframe_to_html_table: {exc}")
        return None


_MONTH_ES = (
    "Ene",
    "Feb",
    "Mar",
    "Abr",
    "May",
    "Jun",
    "Jul",
    "Ago",
    "Sep",
    "Oct",
    "Nov",
    "Dic",
)


def _forecast_extract_date_value_series(df):
    """
    A partir de ``forecast_3m*.csv`` (columnas heterogéneas, p. ej. ``Unnamed: 0`` + ``0``),
    devuelve ``(fechas datetime64, valores float)`` ordenados por fecha.
    """
    if df is None or getattr(df, "empty", True):
        return None, None
    date_col = None
    dates = None
    for c in df.columns:
        parsed = pd.to_datetime(df[c], errors="coerce")
        if int(parsed.notna().sum()) >= max(1, len(df) - 1):
            date_col = c
            dates = parsed
            break
    if dates is None:
        return None, None
    val_col = None
    vals = None
    for c in df.columns:
        if c == date_col:
            continue
        s = pd.to_numeric(df[c], errors="coerce")
        if int(s.notna().sum()) >= max(1, len(df) - 1):
            val_col = c
            vals = s.astype(float)
            break
    if vals is None:
        return None, None
    tmp = pd.DataFrame({"d": dates, "v": vals}).dropna(subset=["d", "v"])
    if tmp.empty:
        return None, None
    tmp = tmp.sort_values("d").reset_index(drop=True)
    return tmp["d"], tmp["v"]


def _fmt_es_integer(n):
    try:
        v = int(round(float(n)))
        return f"{v:,}".replace(",", " ")
    except Exception:
        return str(n)


def _forecast_pick_band_html(run_path):
    if not run_path or not os.path.isdir(run_path):
        return None
    preferred = os.path.join(run_path, "forecast_band.html")
    if os.path.isfile(preferred):
        return "forecast_band.html"
    try:
        for f in sorted(os.listdir(run_path)):
            if f.endswith(".html") and "forecast" in f.lower():
                return f
    except OSError:
        pass
    return None


def _forecast_table_for_details(df):
    """Renombra columnas legibles solo para la tabla colapsable (no toca disco)."""
    if df is None or getattr(df, "empty", True):
        return df
    out = df.copy()
    cols = list(out.columns)
    if len(cols) >= 2:
        out = out.rename(columns={cols[0]: "Mes", cols[1]: "Valor estimado"})
    return out


def build_forecast_insight_context(df_main, df_int, run_path, run_id):
    """
    Variables Jinja para la vista de pronóstico (solo presentación).
    """
    out = {
        "fc_has_forecast": False,
        "fc_trend_label": "",
        "fc_change_pct": None,
        "fc_change_display": "",
        "fc_next_value_display": "",
        "fc_rows_summary": [],
        "fc_chart_file": None,
        "fc_note": "",
    }
    d_main, v_main = _forecast_extract_date_value_series(df_main)
    d_int, v_int = _forecast_extract_date_value_series(df_int)
    dates, vals = d_main, v_main
    if dates is None and d_int is not None:
        dates, vals = d_int, v_int
    if dates is None or vals is None or len(vals) == 0:
        return out

    out["fc_has_forecast"] = True
    v_first = float(vals.iloc[0])
    v_last = float(vals.iloc[-1])
    if len(vals) < 2:
        chg = 0.0
        out["fc_change_pct"] = chg
        out["fc_change_display"] = "0%"
        out["fc_trend_label"] = "estable"
    else:
        raw_pct = (v_last - v_first) / max(abs(v_first), 1e-9) * 100.0
        chg = round(raw_pct, 1)
        out["fc_change_pct"] = chg
        if chg > 0:
            out["fc_change_display"] = f"+{chg}%"
        elif chg < 0:
            out["fc_change_display"] = f"{chg}%"
        else:
            out["fc_change_display"] = "0%"
        if abs(chg) < 0.5:
            out["fc_trend_label"] = "estable"
        elif v_last > v_first:
            out["fc_trend_label"] = "creciente"
        else:
            out["fc_trend_label"] = "decreciente"

    out["fc_next_value_display"] = _fmt_es_integer(vals.iloc[0])

    int_vals = None
    if d_int is not None and v_int is not None and len(v_int) == len(vals):
        int_vals = v_int
    for i in range(len(vals)):
        ts = dates.iloc[i]
        try:
            mi = int(pd.Timestamp(ts).month)
            label = (
                _MONTH_ES[mi - 1]
                if 1 <= mi <= 12
                else pd.Timestamp(ts).strftime("%Y-%m")
            )
        except Exception:
            label = str(ts)[:10]
        vdisp = int_vals.iloc[i] if int_vals is not None else vals.iloc[i]
        out["fc_rows_summary"].append(
            {"label": label, "value": _fmt_es_integer(vdisp)}
        )

    out["fc_chart_file"] = _forecast_pick_band_html(run_path)
    if not out["fc_chart_file"]:
        out["fc_note"] = "No se encontró ``forecast_band.html`` en este run; revise los gráficos del análisis exploratorio."
    return out


def _hub_forecast_context(run_path, run_id):
    """
    Mismo contexto que la vista de pronóstico (``fc_*``) + token embed para ``forecast_band.html``,
    para reutilizar en paneles del hub sin recalcular reglas de negocio.
    """
    out = {}
    if not run_path:
        return out
    rid = str(run_id or "").strip()
    df_m = load_forecast_3m(run_path)
    df_i = load_forecast_3m_int(run_path)
    out.update(build_forecast_insight_context(df_m, df_i, run_path, rid or None))
    band = out.get("fc_chart_file") or _forecast_pick_band_html(run_path)
    rid_ok = rid and is_safe_run_id(rid)
    out["fc_band_embed_token"] = (
        _chart_html_embed_token(rid, band) if (band and rid_ok) else ""
    )
    return out


def _metrics_model_column(df):
    if df is None or getattr(df, "empty", True):
        return None
    lower = {str(c).lower(): c for c in df.columns}
    for key in ("modelo", "model", "nombre", "algoritmo"):
        if key in lower:
            return lower[key]
    return df.columns[0]


def _metrics_score_column(df):
    """
    Columna de error principal (menor es mejor): WRMSE, luego RMSE, luego MSE.
    Retorna ``(columna, etiqueta_legible)``.
    """
    if df is None or getattr(df, "empty", True):
        return None, None
    modelo = _metrics_model_column(df)
    pool = [c for c in df.columns if c != modelo]
    labels = {
        0: "Error de predicción con comparación ponderada en el tiempo (menor es mejor)",
        1: "Error en las mismas unidades que los datos (menor es mejor)",
        2: "Error cuadrático medio (menor es mejor)",
        9: "Indicador numérico de error (menor es mejor)",
    }
    pick, rank = None, 9
    for c in pool:
        if "WRMSE" in str(c).upper().replace(" ", ""):
            pick, rank = c, 0
            break
    if pick is None:
        for c in pool:
            su = str(c).upper().strip()
            if su == "RMSE":
                pick, rank = c, 1
                break
    if pick is None:
        for c in pool:
            su = str(c).upper().strip()
            if su == "MSE" or (su.endswith("MSE") and "RMSE" not in su and "WRMSE" not in su):
                pick, rank = c, 2
                break
    if pick is None:
        for c in pool:
            if pd.api.types.is_numeric_dtype(df[c]):
                pick, rank = c, 9
                break
    if pick is None:
        return None, None
    return pick, labels.get(rank, labels[9])


def _metrics_precision_level(best, sorted_asc):
    """
    Nivel de calidad del ajuste del ganador: ``Alta`` / ``Media`` / ``Baja``.
    Combina separación frente al segundo y magnitud del mejor error.
    """
    if best is None or not sorted_asc:
        return "—"
    b = float(best)
    if len(sorted_asc) == 1:
        if b < 400:
            return "Alta"
        if b < 1200:
            return "Media"
        return "Baja"
    second = float(sorted_asc[1])
    gap = second / max(b, 1e-12)
    if gap >= 1.18:
        sep = 3
    elif gap >= 1.06:
        sep = 2
    else:
        sep = 1
    if b < 400:
        absv = 3
    elif b < 1200:
        absv = 2
    else:
        absv = 1
    combo = int(round((sep + absv) / 2))
    combo = max(1, min(3, combo))
    return ("Baja", "Media", "Alta")[combo - 1]


def _metrics_name_family(name):
    n = (name or "").lower()
    classical = any(
        k in n
        for k in (
            "sarima",
            "arima",
            "holt",
            "winter",
            "hw ",
            "ets",
            "theta",
            "prophet",
        )
    )
    ml = any(
        k in n
        for k in (
            "xgb",
            "boost",
            "mlp",
            "neural",
            "forest",
            "lstm",
        )
    )
    return classical, ml


def build_metrics_insight_context(df):
    """Contexto Jinja para la vista de evaluación de modelos (solo presentación)."""
    out = {
        "mx_has_metrics": False,
        "mx_best_model": "",
        "mx_best_error_display": "",
        "mx_primary_metric_label": "",
        "mx_precision_level": "",
        "mx_rows": [],
        "mx_interpretation": "",
        "mx_explanation": (
            "Este modelo fue seleccionado porque presenta el menor error de predicción "
            "comparado con los demás."
        ),
    }
    if df is None or getattr(df, "empty", True):
        return out
    mcol = _metrics_model_column(df)
    scol, slab = _metrics_score_column(df)
    if mcol is None or scol is None:
        return out
    tmp = df[[mcol, scol]].copy()
    tmp.columns = ["_m", "_s"]
    tmp["_s"] = pd.to_numeric(tmp["_s"], errors="coerce")
    tmp = tmp.dropna(subset=["_s"])
    if tmp.empty:
        return out
    tmp = tmp.sort_values("_s", ascending=True).reset_index(drop=True)
    scores = [float(x) for x in tmp["_s"].tolist()]
    best_name = str(tmp.iloc[0]["_m"]).strip()
    best_score = float(tmp.iloc[0]["_s"])
    out["mx_has_metrics"] = True
    out["mx_best_model"] = best_name
    out["mx_best_error_display"] = f"{best_score:.2f}".replace(".", ",")
    out["mx_primary_metric_label"] = slab
    out["mx_precision_level"] = _metrics_precision_level(best_score, scores)
    for i, row in tmp.iterrows():
        out["mx_rows"].append(
            {
                "name": str(row["_m"]).strip(),
                "error": f"{float(row['_s']):.2f}".replace(".", ","),
                "is_best": i == 0,
            }
        )
    best_c, best_ml = _metrics_name_family(best_name)
    n_class = sum(1 for _, r in tmp.iterrows() if _metrics_name_family(str(r["_m"]))[0])
    n_ml = sum(1 for _, r in tmp.iterrows() if _metrics_name_family(str(r["_m"]))[1])
    if best_c and n_ml > 0:
        out["mx_interpretation"] = (
            "En esta ejecución, un modelo clásico de series temporales obtuvo el menor error "
            "frente a opciones de aprendizaje automático también evaluadas."
        )
    elif best_c:
        out["mx_interpretation"] = (
            "En esta ejecución, los enfoques clásicos de series temporales obtuvieron "
            "mejor desempeño relativo según el error principal de comparación."
        )
    elif best_ml and n_class > 0:
        out["mx_interpretation"] = (
            "En esta ejecución, un modelo de aprendizaje automático obtuvo el menor error "
            "frente a modelos clásicos de series temporales evaluados."
        )
    elif best_ml:
        out["mx_interpretation"] = (
            "En esta ejecución, los modelos de aprendizaje automático muestran el mejor "
            "resultado relativo según el error principal de comparación."
        )
    else:
        out["mx_interpretation"] = (
            "En esta ejecución, el modelo destacado superó al resto según el indicador "
            "principal de error usado para ordenar la comparación."
        )
    return out


def _cl_normalize_cluster_id(v):
    try:
        if pd.isna(v):
            return None
        f = float(v)
        if abs(f - round(f)) < 1e-9:
            return int(round(f))
        return f
    except (TypeError, ValueError):
        s = str(v).strip()
        return s if s else None


def _cl_find_column(df, names):
    lower = {str(c).lower(): c for c in df.columns}
    for n in names:
        nl = n.lower()
        if nl in lower:
            return lower[nl]
    for c in df.columns:
        cl = str(c).lower()
        for n in names:
            if n.lower() in cl:
                return c
    return None


def _cl_find_zone_column(df):
    return _cl_find_column(df, ("Provincia", "provincia", "zona", "canton", "región", "region"))


def _cl_incidence_metric_columns(df):
    tot = _cl_find_column(df, ("Incidentes_Total", "incidentes_total"))
    med = _cl_find_column(df, ("Incidentes_Media", "incidentes_media"))
    if tot is None:
        for c in df.columns:
            u = str(c).lower()
            if "incident" in u and "total" in u:
                tot = c
                break
    if med is None:
        for c in df.columns:
            u = str(c).lower()
            if "incident" in u and "media" in u:
                med = c
                break
    return tot, med


def _cl_tiers_for_sorted_clusters(sorted_ids):
    """``sorted_ids``: del mayor nivel de casos al menor."""
    k = len(sorted_ids)
    out = {}
    if k == 0:
        return out
    if k == 1:
        out[sorted_ids[0]] = "Incidencia media"
        return out
    if k == 2:
        out[sorted_ids[0]] = "Alta incidencia"
        out[sorted_ids[1]] = "Baja incidencia"
        return out
    alta_n = max(1, k // 3)
    baja_n = max(1, k // 3)
    for i, cid in enumerate(sorted_ids):
        if i < alta_n:
            out[cid] = "Alta incidencia"
        elif i >= k - baja_n:
            out[cid] = "Baja incidencia"
        else:
            out[cid] = "Incidencia media"
    return out


def _cl_tier_short(tier_name):
    if tier_name == "Alta incidencia":
        return "Alta"
    if tier_name == "Baja incidencia":
        return "Baja"
    if tier_name == "Incidencia media":
        return "Media"
    return tier_name


def _cl_bar_svg(chart_rows, width=440, height=200, title=None):
    """
    ``chart_rows``: lista de ``{"lab": str, "val": float}`` (promedio de casos por grupo).
    SVG ligero sin Plotly.
    """
    if not chart_rows:
        return ""
    vals = [max(float(r["val"]), 0.0) for r in chart_rows]
    maxcv = max(v ** 0.5 for v in vals) if vals else 1.0
    if maxcv <= 0:
        maxcv = 1.0
    n = len(chart_rows)
    pad_l, pad_r, pad_b, pad_t = 44, 16, 52, 28
    inner_w = width - pad_l - pad_r
    inner_h = height - pad_b - pad_t
    slot = inner_w / max(n, 1)
    bw = slot * 0.62
    gap = slot * 0.38
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
        f'width="100%" style="max-width:{width}px;height:auto" role="img" '
        f'aria-label="Gráfico de barras resumido">'
    ]
    cap = title or "Promedio de casos por grupo (referencia visual)"
    parts.append(
        f'<text x="{pad_l}" y="20" font-size="13" font-weight="600" '
        f'font-family="Segoe UI,system-ui,sans-serif" fill="currentColor">'
        f"{html_escape(cap)}</text>"
    )
    base_y = pad_t + inner_h
    for i, row in enumerate(chart_rows):
        x = pad_l + i * slot + gap * 0.35
        v = float(row["val"])
        cv = max(v, 0.0) ** 0.5
        bar_h = inner_h * (cv / maxcv)
        bar_h = max(bar_h, 10.0)
        if bar_h > inner_h:
            bar_h = inner_h
        y = base_y - bar_h
        parts.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{bar_h:.1f}" '
            f'rx="4" fill="currentColor" opacity="0.88"/>'
        )
        lx = x + bw / 2
        parts.append(
            f'<text x="{lx:.1f}" y="{base_y + 16:.1f}" text-anchor="middle" font-size="11" '
            f'font-family="Segoe UI,system-ui,sans-serif" fill="currentColor">'
            f"{html_escape(row['lab'])}</text>"
        )
        parts.append(
            f'<text x="{lx:.1f}" y="{base_y + 32:.1f}" text-anchor="middle" font-size="10" '
            f'font-family="Segoe UI,system-ui,sans-serif" fill="currentColor" opacity="0.65">'
            f"{html_escape(f'{v:.0f}')}</text>"
        )
    parts.append("</svg>")
    return "".join(parts)


def _cl_build_insight_text(tier_to_provinces):
    alta = tier_to_provinces.get("Alta incidencia", [])
    media = tier_to_provinces.get("Incidencia media", [])
    baja = tier_to_provinces.get("Baja incidencia", [])
    n = len(alta) + len(media) + len(baja)
    if n == 0:
        return ""
    parts = []
    if len(media) >= max(len(alta), len(baja)) and len(media) >= 2:
        parts.append(
            "La mayoría de las provincias se concentran en un nivel medio de casos."
        )
    if len(alta) == 1:
        parts.append(
            f"{str(alta[0]).strip()} destaca como la única zona con el nivel más alto de casos."
        )
    elif len(alta) > 1:
        parts.append(
            "Varias zonas comparten el nivel más alto de casos en este agrupamiento."
        )
    if not parts:
        parts.append(
            "Las provincias quedan repartidas en grupos claros según el nivel de casos."
        )
    text = " ".join(parts[:2])
    if len(text) > 220:
        text = text[:217].rstrip() + "…"
    return text


def build_clustering_insight_context(df):
    """Contexto Jinja para agrupamientos por provincia (solo presentación)."""
    empty = {
        "cl_has_data": False,
        "cl_summary_rows": [],
        "cl_tier_blocks": [],
        "cl_insight": "",
        "cl_chart_svg": "",
        "cl_cluster_col": None,
        "cl_tier_by_cluster": {},
        "cl_tier_map_int": {},
    }
    if df is None or getattr(df, "empty", True):
        return empty
    prov_col = _cl_find_zone_column(df)
    cl_col = _cl_find_column(df, ("Cluster", "cluster", "grupo", "Grupo"))
    tot_col, med_col = _cl_incidence_metric_columns(df)
    if prov_col is None or cl_col is None:
        return empty
    score_col = med_col if med_col is not None else tot_col
    if score_col is None:
        return empty
    work = df[[prov_col, cl_col, score_col]].copy()
    work.columns = ["_p", "_c", "_s"]
    work["_s"] = pd.to_numeric(work["_s"], errors="coerce")
    work["_cid"] = work["_c"].map(_cl_normalize_cluster_id)
    work = work.dropna(subset=["_cid", "_s"])
    if work.empty:
        return empty
    try:
        work["_cid"] = work["_cid"].astype(int)
    except (ValueError, TypeError):
        pass
    agg = work.groupby("_cid", as_index=False)["_s"].mean()
    agg["_cid"] = pd.to_numeric(agg["_cid"], errors="coerce").round()
    agg = agg.dropna(subset=["_cid"])
    if agg.empty:
        return empty
    agg["_cid"] = agg["_cid"].astype(int)
    agg = agg.sort_values("_s", ascending=False).reset_index(drop=True)
    sorted_ids = []
    for i in range(len(agg)):
        raw = agg.iloc[i]["_cid"]
        try:
            sorted_ids.append(int(round(float(raw))))
        except (TypeError, ValueError):
            sorted_ids.append(raw)
    tier_map = _cl_tiers_for_sorted_clusters(sorted_ids)
    summary_rows = []
    for cid in sorted_ids:
        summary_rows.append(
            {
                "cluster_display": str(cid),
                "tier": tier_map.get(cid, "Incidencia media"),
            }
        )
    tier_to_provs = {"Alta incidencia": [], "Incidencia media": [], "Baja incidencia": []}
    for _, r in work.iterrows():
        try:
            cid = int(round(float(r["_cid"])))
        except (TypeError, ValueError):
            cid = r["_cid"]
        tier = tier_map.get(cid)
        if tier is None:
            continue
        name = str(r["_p"]).strip()
        if name and name not in tier_to_provs[tier]:
            tier_to_provs[tier].append(name)
    for t in tier_to_provs:
        tier_to_provs[t].sort(key=lambda x: str(x).lower())
    tier_titles = {
        "Alta incidencia": "Provincias con mayor nivel de casos",
        "Incidencia media": "Provincias con nivel intermedio de casos",
        "Baja incidencia": "Provincias con menor nivel de casos",
    }
    tier_blocks = []
    for tier in ("Alta incidencia", "Incidencia media", "Baja incidencia"):
        tier_blocks.append(
            {
                "tier": tier,
                "section_title": tier_titles[tier],
                "provinces": tier_to_provs.get(tier, []),
            }
        )
    chart_rows = []
    for cid in sorted_ids:
        vm = float(agg.loc[agg["_cid"] == cid, "_s"].iloc[0])
        tier = tier_map.get(cid, "")
        chart_rows.append(
            {
                "lab": f"Grupo {cid} ({_cl_tier_short(tier)})",
                "val": vm,
            }
        )
    insight = _cl_build_insight_text(tier_to_provs)
    out = {
        "cl_has_data": True,
        "cl_summary_rows": summary_rows,
        "cl_tier_blocks": tier_blocks,
        "cl_insight": insight,
        "cl_chart_svg": _cl_bar_svg(chart_rows),
        "cl_cluster_col": cl_col,
        "cl_tier_by_cluster": {str(k): v for k, v in tier_map.items()},
        "cl_tier_map_int": tier_map,
    }
    return out


_PREVIEW_ROWS_CAP = 10


def dataframe_to_preview_records(df, max_rows=None):
    """
    Columnas + filas (dict) para vista previa en plantillas; máximo ``_PREVIEW_ROWS_CAP``.
    """
    if df is None or getattr(df, "empty", True):
        return [], []
    n = min(int(max_rows or _PREVIEW_ROWS_CAP), _PREVIEW_ROWS_CAP)
    view = df.iloc[:n]
    cols = [str(c) for c in view.columns]
    try:
        recs = json.loads(view.to_json(orient="records", date_format="iso"))
    except Exception as exc:
        print(f"[preview] dataframe_to_preview_records: {exc}")
        return cols, []
    return cols, recs


def _preview_display_str(v):
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v).strip()


def _is_placeholder_raw_value(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return True
    s = str(v).strip()
    if not s or s.lower() in ("nan", "null", "none", "n/a", "na", "-"):
        return True
    if re.fullmatch(r"#+", s):
        return True
    return False


def _heuristic_clean_cell_value(v):
    """Limpieza ligera solo para vista previa (no sustituye al pipeline ML1)."""
    if pd.isna(v):
        return pd.NA
    if isinstance(v, (int, np.integer)) and not isinstance(v, bool):
        return int(v)
    if isinstance(v, (float, np.floating)) and not isinstance(v, bool):
        if pd.isna(v):
            return pd.NA
        return float(v)
    t = str(v).strip()
    if not t or t.lower() in ("nan", "null", "none", "n/a", "na", "-"):
        return pd.NA
    if re.fullmatch(r"#+", t):
        return pd.NA
    if t.lower() in ("desconoc", "desconocido", "unknown"):
        return "DESCONOCIDO"
    t = re.sub(r"\s+", " ", t).strip()
    if len(t) <= 48 and t.islower() and not t.isdigit():
        return t.title()
    return t


def build_transformation_preview_pair(raw_df, max_rows=None):
    """
    Par crudo / heurístico ``limpio`` + clases por celda para la UI de transformación.
    """
    out = {
        "raw_columns": [],
        "raw_records": [],
        "clean_columns": [],
        "clean_records": [],
        "raw_cell_classes": [],
        "clean_cell_classes": [],
    }
    if raw_df is None or getattr(raw_df, "empty", True):
        return out
    n = min(int(max_rows or _PREVIEW_ROWS_CAP), _PREVIEW_ROWS_CAP)
    view = raw_df.iloc[:n].copy()
    clean = view.copy()
    for c in view.columns:
        if pd.api.types.is_numeric_dtype(view[c]):
            clean[c] = view[c]
        else:
            clean[c] = view[c].map(_heuristic_clean_cell_value)
    cols = [str(c) for c in view.columns]
    try:
        raw_recs = json.loads(view.to_json(orient="records", date_format="iso"))
        clean_recs = json.loads(clean.to_json(orient="records", date_format="iso"))
    except Exception as exc:
        print(f"[preview] build_transformation_preview_pair: {exc}")
        return out
    raw_flags, clean_flags = [], []
    for rr, cr in zip(raw_recs, clean_recs):
        rf, cf = {}, {}
        for col in cols:
            rv, cv = rr.get(col), cr.get(col)
            dr = _preview_display_str(rv)
            dc = _preview_display_str(cv)
            if dr == dc:
                rf[col], cf[col] = "", ""
            elif _is_placeholder_raw_value(rv) and dc != "":
                rf[col] = "cell-muted"
                cf[col] = "cell-new"
            elif (not _is_placeholder_raw_value(rv)) and (
                dc == "" or _is_placeholder_raw_value(cv)
            ):
                rf[col] = "cell-removed"
                cf[col] = "cell-removed"
            else:
                rf[col] = "cell-changed"
                cf[col] = "cell-changed"
        raw_flags.append(rf)
        clean_flags.append(cf)
    out["raw_columns"] = cols
    out["raw_records"] = raw_recs
    out["clean_columns"] = cols
    out["clean_records"] = clean_recs
    out["raw_cell_classes"] = raw_flags
    out["clean_cell_classes"] = clean_flags
    return out


def _meta_first_source_csv(run_path):
    """Primer CSV de ``meta.json`` ``files`` presente en ``data/``."""
    meta_path = os.path.join(run_path, "meta.json")
    if not os.path.isfile(meta_path):
        return None
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
    files = meta.get("files")
    if not isinstance(files, list) or not files:
        return None
    raw_name = str(files[0]).strip()
    if not raw_name:
        return None
    allowed = set(list_csv_files())
    if raw_name in allowed:
        return raw_name
    sec = secure_filename(raw_name)
    if sec in allowed:
        return sec
    return None


def resolve_raw_csv_filename_for_run(run_id):
    """
    Basename del CSV fuente en ``data/`` para este run: ``meta.json``, historial y sesión activa.
    """
    if not run_id or not is_safe_run_id(str(run_id)):
        return None
    run_path = os.path.join(ARTIFACTS_DIR, run_id)
    if not os.path.isdir(run_path):
        return None
    fn = _meta_first_source_csv(run_path)
    if fn:
        return fn
    for row in load_history():
        if not isinstance(row, dict):
            continue
        if row.get("run_id") != run_id:
            continue
        sf = (row.get("source_file") or "").strip()
        if sf and sf in list_csv_files():
            return sf
    latest = get_latest_run()
    if latest and os.path.basename(os.path.normpath(latest)) == run_id:
        sess = (fl.session.get(SESSION_ACTIVE_CSV_KEY) or "").strip()
        if sess and sess in list_csv_files():
            return sess
    return None


def load_raw_dataset_from_session(max_rows=None):
    """Carga las primeras filas del CSV activo en sesión desde ``data/``."""
    name = (fl.session.get(SESSION_ACTIVE_CSV_KEY) or "").strip()
    if not name or name not in list_csv_files():
        return None
    path = os.path.join(DEFAULT_SAVE_DIR, name)
    if not os.path.isfile(path):
        return None
    n = min(int(max_rows or _PREVIEW_ROWS_CAP), _PREVIEW_ROWS_CAP)
    try:
        return pd.read_csv(path, nrows=n)
    except Exception as exc:
        print(f"[preview] load_raw_dataset_from_session: {exc}")
        return None


def load_clean_dataset_from_run(run_id, max_rows=None):
    """Primeras filas de ``data_limpia.csv`` o ``data_limpia.parquet`` en ``artifacts/<run_id>/``."""
    if not run_id or not is_safe_run_id(str(run_id)):
        return None
    n = min(int(max_rows or _PREVIEW_ROWS_CAP), _PREVIEW_ROWS_CAP)
    run_path = os.path.join(ARTIFACTS_DIR, run_id)
    csv_p = os.path.join(run_path, "data_limpia.csv")
    parquet_p = os.path.join(run_path, "data_limpia.parquet")
    try:
        if os.path.isfile(csv_p):
            return pd.read_csv(csv_p, nrows=n)
        if os.path.isfile(parquet_p):
            return pd.read_parquet(parquet_p).iloc[:n].copy()
    except Exception as exc:
        print(f"[preview] load_clean_dataset_from_run: {exc}")
    return None


def build_real_comparison_preview(df_raw, df_clean, max_rows=None):
    """Previews comparables (JSON-safe) para plantillas: crudo vs salida del pipeline."""
    mr = min(int(max_rows or _PREVIEW_ROWS_CAP), _PREVIEW_ROWS_CAP)
    rc_raw, rr_raw = dataframe_to_preview_records(df_raw, mr)
    rc_clean, rr_clean = dataframe_to_preview_records(df_clean, mr)
    return {
        "raw_records": rr_raw,
        "clean_records": rr_clean,
        "raw_columns": rc_raw,
        "clean_columns": rc_clean,
        "columns": rc_clean,
    }


def build_real_pipeline_comparison_context(run_dir_path, run_id, max_rows=None):
    """Variables Jinja para el bloque «antes / después» real en ``/results``."""
    mr = min(int(max_rows or _PREVIEW_ROWS_CAP), _PREVIEW_ROWS_CAP)
    empty = {
        "real_cmp_has_data": False,
        "real_cmp_raw_columns": [],
        "real_cmp_raw_records": [],
        "real_cmp_clean_columns": [],
        "real_cmp_clean_records": [],
        "real_cmp_source_name": "",
        "real_cmp_clean_label": "",
    }
    if not run_id or not is_safe_run_id(str(run_id)) or not run_dir_path:
        return empty
    df_clean = load_clean_dataset_from_run(run_id, max_rows=mr)
    if df_clean is None or getattr(df_clean, "empty", True):
        return empty
    raw_name = resolve_raw_csv_filename_for_run(run_id)
    df_raw = None
    if raw_name:
        rp = os.path.join(DEFAULT_SAVE_DIR, raw_name)
        if os.path.isfile(rp):
            try:
                df_raw = pd.read_csv(rp, nrows=mr)
            except Exception as exc:
                print(f"[preview] build_real_pipeline_comparison_context raw: {exc}")
    if df_raw is None or getattr(df_raw, "empty", True):
        return empty
    pv = build_real_comparison_preview(df_raw, df_clean, max_rows=mr)
    if not pv.get("raw_records") or not pv.get("clean_records"):
        return empty
    if os.path.isfile(os.path.join(run_dir_path, "data_limpia.csv")):
        clean_label = "data_limpia.csv"
    elif os.path.isfile(os.path.join(run_dir_path, "data_limpia.parquet")):
        clean_label = "data_limpia.parquet"
    else:
        clean_label = "data_limpia"
    return {
        "real_cmp_has_data": True,
        "real_cmp_raw_columns": pv["raw_columns"],
        "real_cmp_raw_records": pv["raw_records"],
        "real_cmp_clean_columns": pv["clean_columns"],
        "real_cmp_clean_records": pv["clean_records"],
        "real_cmp_source_name": raw_name or "",
        "real_cmp_clean_label": f"artifacts/{run_id}/{clean_label}",
    }


def home_latest_raw_csv_preview(csv_files):
    """
    Vista previa del CSV más reciente en ``data/`` (por mtime), o vacío.
    Retorna ``(nombre_archivo | None, columnas, filas)``.
    """
    if not csv_files:
        return None, [], []
    best_path, best_name = None, None
    best_mtime = -1.0
    for name in csv_files:
        path = os.path.join(DEFAULT_SAVE_DIR, name)
        if not os.path.isfile(path):
            continue
        try:
            mt = os.path.getmtime(path)
        except OSError:
            continue
        if mt > best_mtime:
            best_mtime = mt
            best_path = path
            best_name = name
    if not best_path:
        return None, [], []
    try:
        raw = pd.read_csv(best_path, nrows=_PREVIEW_ROWS_CAP)
        cols, rows = dataframe_to_preview_records(raw, _PREVIEW_ROWS_CAP)
        return best_name, cols, rows
    except Exception as exc:
        print(f"[preview] home_latest_raw_csv_preview: {exc}")
        return None, [], []


def _enrich_relations_preview_dataframe(df, payload):
    """Añade extracto tabular de las columnas usadas en correlación (solo informativo)."""
    payload.setdefault("relation_preview_columns", [])
    payload.setdefault("relation_preview_records", [])
    if not payload.get("has_corr") or df is None:
        return
    cols = payload.get("correlation_columns") or []
    use = [c for c in cols if c in df.columns]
    if len(use) < 2:
        return
    try:
        sub = df.loc[:, use].head(_PREVIEW_ROWS_CAP)
        c, r = dataframe_to_preview_records(sub, _PREVIEW_ROWS_CAP)
        payload["relation_preview_columns"] = c
        payload["relation_preview_records"] = r
    except Exception as exc:
        print(f"[preview] _enrich_relations_preview_dataframe: {exc}")


def _relations_append_interpretive(df, pl):
    """
    Añade textos y gráficos ligeros para ``/relations`` (solo presentación; no altera correlaciones).
    """
    pl["rel_detected"] = []
    pl["rel_patterns"] = []
    pl["rel_limitation_text"] = None
    pl["rel_cat_chart_svg"] = ""
    pl["rel_heatmap_caption"] = ""
    pl["rel_show_technical_details"] = bool(pl.get("has_corr"))
    if df is None or getattr(df, "empty", True):
        return
    detected = []
    patterns = []
    prov_col = _cl_find_zone_column(df)
    if prov_col is not None:
        pv = df[prov_col].astype(str).str.strip().replace("", pd.NA).dropna()
        if len(pv) > 0:
            vc = pv.value_counts()
            if len(vc):
                top = str(vc.index[0]).strip()
                top_n = int(vc.iloc[0])
                rest = int(vc.iloc[1:].sum()) if len(vc) > 1 else 0
                if top_n > max(rest, 1):
                    detected.append(
                        {
                            "headline": "Provincia y volumen de casos",
                            "body": (
                                f"Los registros se inclinan con claridad hacia {top}; "
                                "el resto de provincias queda en segundo plano en volumen."
                            ),
                        }
                    )
                patterns.append(
                    "Las provincias no aparecen repartidas por igual: hay zonas con más actividad registrada."
                )
    delito_col = _cl_find_column(df, ("Delito", "delito"))
    if delito_col is not None:
        dv = df[delito_col].astype(str).str.strip().replace("", pd.NA).dropna()
        if len(dv) > 3:
            patterns.append(
                "Los tipos de hecho se repiten con frecuencia; unos pocos concentran mucha de la actividad."
            )
    hi = _cl_find_column(df, ("Hora_Inicio", "hora_inicio"))
    if hi is not None:
        hnum = pd.to_numeric(df[hi], errors="coerce")
        hok = hnum.dropna()
        if len(hok) > max(10, len(df) // 10):
            hh = (np.floor(hok).astype(int) % 24).values
            day = int(((hh >= 9) & (hh < 18)).sum())
            if day > len(hh) * 0.42:
                detected.append(
                    {
                        "headline": "Horario y actividad",
                        "body": (
                            "Los eventos entre las 9:00 y las 18:00 concentran "
                            "la mayor parte de los casos frente a otras franjas del día."
                        ),
                    }
                )
                patterns.append(
                    "Los horarios muestran franjas del día con más actividad que otras."
                )
    if pl.get("has_corr") and pl.get("top_pairs"):
        a, b, r = pl["top_pairs"][0]
        try:
            ar = float(r)
        except (TypeError, ValueError):
            ar = 0.0
        if abs(ar) >= 0.5:
            if ar > 0:
                detected.append(
                    {
                        "headline": "Comportamiento conjunto",
                        "body": (
                            f"Cuando {a} sube, {b} también tiende a subir en este conjunto; "
                            "cuando bajan, suelen hacerlo las dos."
                        ),
                    }
                )
            else:
                detected.append(
                    {
                        "headline": "Tendencias opuestas",
                        "body": (
                            f"Cuando {a} aumenta, {b} suele moverse en sentido contrario "
                            "en los datos analizados."
                        ),
                    }
                )
    if not patterns and prov_col is None and delito_col is None:
        patterns.append(
            "Los datos mezclan distintos tipos de información; conviene leerlos junto al análisis exploratorio."
        )
    elif len(df.select_dtypes(include="number").columns) <= 1 and not pl.get("has_corr"):
        patterns.append(
            "Pocas columnas son numéricas; la lectura de relaciones pasa más por categorías y fechas."
        )
    if not patterns:
        patterns.append(
            "Varios factores (lugar, tiempo y descripción del hecho) conviven en cada registro."
        )
    if not detected:
        detected.append(
            {
                "headline": "Lectura general",
                "body": (
                    "Los datos permiten comparar categorías y tendencias con más detalle "
                    "en el análisis exploratorio y en las gráficas del mismo run."
                    if not pl.get("has_corr")
                    else "Las variables numéricas disponibles se pueden contrastar en la vista resumida y en el detalle ampliado."
                ),
            }
        )
    pl["rel_detected"] = detected[:3]
    pl["rel_patterns"] = patterns[:4]
    if not pl.get("has_corr"):
        pl["rel_limitation_text"] = (
            "No se encontraron relaciones significativas entre variables numéricas.\n\n"
            "El dataset es mayormente categórico, por lo que las relaciones se interpretan mejor "
            "en el análisis exploratorio."
        )
    if pl.get("has_corr") and pl.get("heatmap_path"):
        pl["rel_heatmap_caption"] = (
            "Este gráfico resume de un vistazo qué variables numéricas se mueven a la vez "
            "y cuáles lo hacen en direcciones distintas."
        )
    if (not pl.get("has_corr") or pl.get("correlation_blocked_normal")) and prov_col:
        try:
            vc = (
                df[prov_col]
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .value_counts()
                .head(8)
            )
            if len(vc) > 0:
                rows = [{"lab": str(i)[:20], "val": float(v)} for i, v in vc.items()]
                pl["rel_cat_chart_svg"] = _cl_bar_svg(
                    rows, title="Casos por provincia (vista resumida)"
                )
        except Exception as exc:
            print(f"[relations] rel_cat_chart_svg: {exc}")


def _relations_payload_hit_enrich(run_path, mode, pl):
    """Completa extractos en aciertos de caché antiguos de relaciones."""
    df_l, _ = load_analysis_dataset(run_path, mode)
    if df_l is None:
        return
    _relations_append_interpretive(df_l, pl)
    if not pl.get("has_corr"):
        return
    if pl.get("relation_preview_records"):
        return
    if not pl.get("correlation_columns"):
        num = df_l.select_dtypes(include="number").columns
        pl["correlation_columns"] = [str(x) for x in num.tolist()]
    _enrich_relations_preview_dataframe(df_l, pl)


def _non_null_count(series):
    return int(series.notna().sum())


def _ratio_datetime_parse_ok(series):
    """Share of non-null values that parse as datetimes (object/string columns)."""
    mask = series.notna()
    sub = series.loc[mask]
    if sub.empty:
        return 0.0
    # Cap rows: full-frame datetime guessing is slow and noisy on wide tables.
    cap = 10_000
    if len(sub) > cap:
        sub = sub.iloc[:cap]
    n = len(sub)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        parsed = pd.to_datetime(sub, errors="coerce", utc=False)
    return float(parsed.notna().sum()) / float(n)


def _is_boolean_like_numeric(series):
    """True/False or 0/1 stored as integers/floats."""
    if not pdt.is_numeric_dtype(series):
        return False
    vals = pd.unique(series.dropna())
    if len(vals) > 2:
        return False
    try:
        as_float = {float(v) for v in vals}
    except (TypeError, ValueError):
        return False
    return as_float.issubset({0.0, 1.0})


def _is_boolean_like_string(series):
    """Common string encodings of booleans."""
    if not (pdt.is_object_dtype(series) or pdt.is_string_dtype(series)):
        return False
    sl = series.dropna().astype(str).str.strip().str.lower()
    if sl.empty:
        return False
    allowed = {
        "true",
        "false",
        "0",
        "1",
        "t",
        "f",
        "yes",
        "no",
        "y",
        "n",
        "si",
        "sí",
    }
    return float(sl.isin(allowed).mean()) >= 0.95


def _eligible_id_heuristic(series):
    """ID-like heuristic applies to discrete / text keys, not arbitrary floats."""
    if pdt.is_object_dtype(series) or pdt.is_string_dtype(series):
        return True
    if isinstance(series.dtype, pd.CategoricalDtype) or pdt.is_categorical_dtype(
        series
    ):
        return True
    if pdt.is_integer_dtype(series):
        return True
    # pandas nullable integers (Int64, etc.)
    try:
        if str(series.dtype).startswith("Int") or str(series.dtype).startswith(
            "UInt"
        ):
            return True
    except Exception:
        pass
    return False


def detect_variable_types(df: pd.DataFrame) -> dict:
    """
    Classify columns into mutually exclusive groups for UI reporting.

    Priority: temporales → booleanas → ids → numéricas → categóricas.
    """
    empty = {
        "numericas": [],
        "categoricas": [],
        "temporales": [],
        "booleanas": [],
        "ids": [],
    }
    if df is None or getattr(df, "empty", True):
        return empty

    numericas = []
    categoricas = []
    temporales = []
    booleanas = []
    ids = []
    n = len(df)

    for col in df.columns:
        s = df[col]

        # 1) Native or obvious datetimes
        if pdt.is_datetime64_any_dtype(s):
            temporales.append(col)
            continue

        # 2) Strings that are mostly parseable datetimes
        if pdt.is_object_dtype(s) or pdt.is_string_dtype(s):
            if _non_null_count(s) > 0 and _ratio_datetime_parse_ok(s) >= 0.8:
                temporales.append(col)
                continue

        # 3) Boolean dtype
        if pdt.is_bool_dtype(s):
            booleanas.append(col)
            continue

        # 4) Numeric 0/1 (and bool-like numeric)
        if _is_boolean_like_numeric(s):
            booleanas.append(col)
            continue

        # 5) String/object boolean tokens
        if _is_boolean_like_string(s):
            booleanas.append(col)
            continue

        # 6) High-cardinality identifier-like (text / int keys, not floats)
        nn = _non_null_count(s)
        if nn > 0 and _eligible_id_heuristic(s):
            nunique = int(s.nunique(dropna=True))
            if (nunique / max(n, 1)) > 0.9:
                ids.append(col)
                continue

        # 7) Remaining numerics
        if pdt.is_numeric_dtype(s):
            numericas.append(col)
            continue

        # 8) Categorical / object leftovers
        if (
            pdt.is_categorical_dtype(s)
            or pdt.is_object_dtype(s)
            or pdt.is_string_dtype(s)
        ):
            categoricas.append(col)
            continue

        # Rare dtypes (e.g. struct): treat as categorical for display
        categoricas.append(col)

    return {
        "numericas": numericas,
        "categoricas": categoricas,
        "temporales": temporales,
        "booleanas": booleanas,
        "ids": ids,
    }


def tipos_from_run_cache(run_id, mode=None):
    """Tipos memorizados para ``run_id`` y modo actual (o explícito), o ``None``."""
    ck = analysis_cache_key(run_id, mode)
    if ck and ck in _types_cache:
        entry = _types_cache[ck]
        if isinstance(entry.get("tipos"), dict):
            return entry["tipos"]
    return None


def get_contextual_actions(
    module_name,
    df,
    tipos=None,
    relations_data=None,
    extra=None,
    analytical_mode=None,
):
    """
    Acciones sugeridas (``label`` + nombre de vista Flask ``endpoint`` para ``url_for``)
    para módulos con datos incompletos o análisis no disponible.
    """
    extra = extra or {}
    am = get_analytical_mode()
    tipos = tipos if isinstance(tipos, dict) else {}
    relations_data = relations_data if isinstance(relations_data, dict) else {}

    rel_has_dataset = bool(
        module_name == "relations"
        and relations_data.get("data_source")
    )
    missing_df = df is None or (hasattr(df, "empty") and df.empty)

    if module_name == "empty_runs":
        return [
            {"label": "Ir a carga de datos", "endpoint": "home"},
            {"label": "Procesamiento de datos", "endpoint": "run"},
        ]

    if module_name == "types":
        if extra.get("no_data_source"):
            return [
                {"label": "Ir a carga de datos", "endpoint": "home"},
                {"label": "Procesamiento de datos", "endpoint": "run"},
            ]
        return []

    if missing_df and not rel_has_dataset and module_name in (
        "relations",
        "forecast",
        "metrics",
        "dashboard",
    ):
        return [
            {"label": "Ir a carga de datos", "endpoint": "home"},
            {"label": "Procesamiento de datos", "endpoint": "run"},
        ]

    if module_name == "relations":
        if not relations_data.get("data_source"):
            return [
                {"label": "Ir a carga de datos", "endpoint": "home"},
                {"label": "Procesamiento de datos", "endpoint": "run"},
            ]
        if not relations_data.get("has_corr"):
            if relations_data.get("correlation_blocked_normal"):
                return [
                    {"label": "Ver tipos de variables", "endpoint": "show_types"},
                    {"label": "Análisis exploratorio (EDA)", "endpoint": "results"},
                    {"label": "Visualizaciones", "endpoint": "show_charts"},
                ]
            return [
                {"label": "Ver tipos de variables", "endpoint": "show_types"},
                {"label": "Ir a análisis exploratorio", "endpoint": "results"},
                {"label": "Procesamiento de datos", "endpoint": "run"},
            ]
        return []

    if module_name == "clustering" and missing_df:
        return [
            {"label": "Ir a carga de datos", "endpoint": "home"},
            {"label": "Procesamiento de datos", "endpoint": "run"},
        ]

    if module_name == "clustering":
        if len(tipos.get("numericas", [])) < 2:
            return [
                {"label": "Revisar tipos de variables", "endpoint": "show_types"},
                {"label": "Ver análisis exploratorio", "endpoint": "results"},
            ]
        return []

    if module_name == "forecast" and missing_df:
        return [
            {"label": "Evaluación de modelos", "endpoint": "show_metrics"},
            {"label": "Análisis exploratorio", "endpoint": "results"},
            {"label": "Procesamiento de datos", "endpoint": "run"},
        ]

    if module_name == "metrics" and missing_df:
        return [
            {"label": "Procesamiento de datos", "endpoint": "run"},
            {"label": "Análisis exploratorio", "endpoint": "results"},
        ]

    if module_name == "charts" and extra.get("no_charts"):
        return [
            {"label": "Procesamiento de datos", "endpoint": "run"},
            {"label": "Análisis exploratorio", "endpoint": "results"},
        ]

    if module_name == "dashboard" and extra.get("no_dataset"):
        return [
            {"label": "Ir a carga de datos", "endpoint": "home"},
            {"label": "Procesamiento de datos", "endpoint": "run"},
        ]

    return []


def guidance_message_for_module(
    module_name,
    df,
    tipos=None,
    relations_data=None,
    extra=None,
    analytical_mode=None,
):
    """Texto académico breve que explica *por qué* se muestran acciones (o cadena vacía)."""
    extra = extra or {}
    am = get_analytical_mode()
    tipos = tipos if isinstance(tipos, dict) else {}
    relations_data = relations_data if isinstance(relations_data, dict) else {}
    rel_has_dataset = bool(
        module_name == "relations" and relations_data.get("data_source")
    )
    missing_df = df is None or (hasattr(df, "empty") and df.empty)

    if module_name == "empty_runs":
        return (
            "No hay ejecuciones en el historial con carpeta de artefactos válida. "
            "El análisis solo usa runs registrados en run_history.json."
        )

    actions = get_contextual_actions(
        module_name,
        df,
        tipos,
        relations_data,
        extra=extra,
        analytical_mode=am,
    )
    if not actions:
        return ""

    if module_name == "types" and extra.get("no_data_source"):
        return (
            "No hay dataset limpio asociado al último run. "
            "Debe ejecutarse el pipeline tras la carga de datos para materializar "
            "`data_limpia` en `artifacts/`."
        )
    if missing_df and not rel_has_dataset and module_name in (
        "relations",
        "clustering",
        "forecast",
        "metrics",
    ):
        return (
            "Faltan artefactos o tablas requeridas para esta etapa. "
            "El flujo metodológico exige completar la carga y el procesamiento previos."
        )
    if module_name == "dashboard" and extra.get("no_dataset"):
        return (
            "El reporte integrado no dispone de un conjunto limpio analizable en el último run. "
            "Revise la carga y vuelva a ejecutar el procesamiento."
        )
    if module_name == "charts" and extra.get("no_charts"):
        return (
            "Aún no se han generado gráficos HTML en el run actual. "
            "El EDA del pipeline produce estas salidas en `artifacts/`."
        )
    if module_name == "relations" and relations_data.get("data_source"):
        if not relations_data.get("has_corr"):
            if relations_data.get("correlation_blocked_normal"):
                return (
                    "Este conjunto se describe mejor con categorías y fechas que con mediciones "
                    "numéricas cruzadas. Siga en tipos, análisis exploratorio y gráficos."
                )
            return (
                "Con los datos de este run no apareció una comparación numérica estable entre columnas. "
                "Revise tipos y el procesamiento, o use las gráficas del mismo run."
            )
    if module_name == "clustering" and not missing_df:
        if len(tipos.get("numericas", [])) < 2:
            return (
                "Para interpretar los agrupamientos junto con técnicas multivariantes suele "
                "necesitarse más de una variable numérica; revise la clasificación de tipos y el EDA."
            )
    if module_name == "forecast" and missing_df:
        return (
            "No hay series de pronóstico exportadas (`forecast_3m.csv` / intervalos). "
            "El pronóstico depende de la evaluación y entrenamiento del pipeline."
        )
    if module_name == "metrics" and missing_df:
        return (
            "No se encontró `errores_modelos.csv`; sin métricas no es posible comparar modelos. "
            "Ejecute nuevamente el procesamiento tras entrenar."
        )
    return (
        "Las condiciones actuales limitan esta vista. Use las acciones sugeridas para "
        "retroceder o avanzar en el pipeline de análisis."
    )


def get_types_cached(run_id, df, mode=None):
    """
    Return ``detect_variable_types(df)``, usando caché por ``analysis_cache_key(run_id)``.
    """
    m = "normal"
    if df is None:
        print("[artifacts] get_types_cached: df is None, bypass cache")
        return detect_variable_types(None)
    if not run_id or not is_safe_run_id(str(run_id)):
        print("[artifacts] get_types_cached: invalid run_id, bypass cache")
        return detect_variable_types(df)
    ck = analysis_cache_key(run_id, m)
    if not ck:
        return detect_variable_types(df)
    entry = _types_cache.get(ck)
    if entry is not None and "tipos" in entry:
        print(f"[artifacts] get_types_cached: HIT key={ck}")
        return entry["tipos"]
    print(f"[artifacts] get_types_cached: MISS key={ck}, computing")
    return detect_variable_types(df)


def load_data_limpia(run_dir_path):
    """
    Load ``data_limpia.csv`` if present, else ``data_limpia.parquet``.
    Returns ``(DataFrame | None, source_label | None)``.
    """
    if not run_dir_path:
        print("[artifacts] load_data_limpia: no run directory")
        return None, None
    csv_path = os.path.join(run_dir_path, "data_limpia.csv")
    parquet_path = os.path.join(run_dir_path, "data_limpia.parquet")
    if os.path.isfile(csv_path):
        try:
            df = pd.read_csv(csv_path)
            print(
                "[artifacts] load_data_limpia: loaded CSV",
                csv_path,
                f"shape={df.shape}",
            )
            return df, "data_limpia.csv"
        except Exception as exc:
            print(f"[artifacts] load_data_limpia: CSV read failed: {exc}")
            return None, None
    if os.path.isfile(parquet_path):
        try:
            df = pd.read_parquet(parquet_path)
            print(
                "[artifacts] load_data_limpia: loaded Parquet",
                parquet_path,
                f"shape={df.shape}",
            )
            return df, "data_limpia.parquet"
        except Exception as exc:
            print(f"[artifacts] load_data_limpia: Parquet read failed: {exc}")
            return None, None
    print(
        "[artifacts] load_data_limpia: no data_limpia.csv or data_limpia.parquet in",
        run_dir_path,
    )
    return None, None


def load_analysis_dataset(run_dir_path, mode=None):
    """Dataset principal del run: ``data_limpia`` (parquet o CSV)."""
    return load_data_limpia(run_dir_path)


def compute_correlations(df):
    """Pearson correlation matrix for numeric columns only (``numeric_only=True``)."""
    if df is None or getattr(df, "empty", True):
        print("[relations] compute_correlations: empty or None DataFrame")
        return None
    try:
        corr = df.corr(numeric_only=True, min_periods=1)
        print(f"[relations] compute_correlations: matrix shape={corr.shape}")
        return corr
    except Exception as exc:
        print(f"[relations] compute_correlations: failed: {exc}")
        return None


def extract_top_correlations(corr_df, threshold=0.5, top_n=50):
    """
    Pares (var_a, var_b, r) con |r| >= threshold, sin diagonal, ordenados por |r| descendente.
    """
    if corr_df is None or corr_df.empty:
        return []
    pairs = []
    cols = list(corr_df.columns)
    for i, ci in enumerate(cols):
        for j in range(i + 1, len(cols)):
            cj = cols[j]
            val = corr_df.iloc[i, j]
            if pd.isna(val):
                continue
            fv = float(val)
            if abs(fv) >= threshold:
                pairs.append((str(ci), str(cj), fv))
    pairs.sort(key=lambda t: abs(t[2]), reverse=True)
    return pairs[:top_n]


def generate_insights(correlations):
    """
    ``correlations``: lista de tuplas (nombre_a, nombre_b, r) como en ``extract_top_correlations``.
    """
    lines = []
    for a, b, r in correlations:
        ar = float(r)
        if ar > 0:
            if ar >= 0.7:
                frag = "una fuerte correlación positiva"
            elif ar >= 0.5:
                frag = "una correlación positiva"
            else:
                frag = "una correlación positiva moderada"
            lines.append(f"{a} tiene {frag} con {b} ({ar:.2f}).")
        elif ar < 0:
            if ar <= -0.7:
                frag = "una fuerte correlación negativa"
            elif ar <= -0.5:
                frag = "una correlación negativa"
            else:
                frag = "una correlación negativa moderada"
            lines.append(f"{a} tiene {frag} con {b} ({ar:.2f}).")
        else:
            lines.append(f"{a} y {b} están prácticamente incorrelacionados (0.00).")
    return lines


def generate_heatmap(corr_df, output_path):
    """Escribe un HTML Plotly (heatmap) en ``output_path``."""
    if corr_df is None or corr_df.empty:
        print("[relations] generate_heatmap: skip (empty matrix)")
        return False
    if corr_df.shape[0] < 2:
        print("[relations] generate_heatmap: skip (need at least 2 numeric variables)")
        return False
    try:
        fig = go.Figure(
            data=go.Heatmap(
                z=corr_df.values,
                x=list(corr_df.columns),
                y=list(corr_df.index),
                zmin=-1,
                zmax=1,
                colorscale="RdBu",
                colorbar=dict(title="r"),
            )
        )
        fig.update_layout(
            title="Matriz de correlación (Pearson)",
            margin=dict(l=120, r=40, t=60, b=80),
        )
        out_dir = os.path.dirname(output_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        fig.write_html(output_path, include_plotlyjs="cdn", full_html=True)
        print(f"[relations] generate_heatmap: wrote {output_path}")
        return True
    except Exception as exc:
        print(f"[relations] generate_heatmap: failed: {exc}")
        return False


def _build_relations_page_dict(run_id, df, source_name, analytical_mode="normal"):
    """
    Calcula correlaciones, insights y heatmap; devuelve dict listo para ``render_template``.
    No escribe en caché global (lo hace la ruta).

    El heatmap se guarda como ``correlation_heatmap.html`` en el run.
    """
    heatmap_fname = "correlation_heatmap.html"
    corr_df = compute_correlations(df)
    top_pairs = extract_top_correlations(corr_df) if corr_df is not None else []
    insights = generate_insights(top_pairs)
    heatmap_relpath = None
    if run_id and is_safe_run_id(run_id) and corr_df is not None and corr_df.shape[0] >= 2:
        heatmap_abs = os.path.join(ARTIFACTS_DIR, run_id, heatmap_fname)
        if not os.path.isfile(heatmap_abs):
            generate_heatmap(corr_df, heatmap_abs)
        else:
            print(f"[relations] heatmap already exists: {heatmap_abs}")
        if os.path.isfile(heatmap_abs):
            heatmap_relpath = f"{run_id}/{heatmap_fname}"
    numeric_cols = int(corr_df.shape[0]) if corr_df is not None else 0
    # Pearson off-diagonal needs at least 2 numeric features (symmetric n×n, n≥2).
    has_corr = (
        corr_df is not None
        and not corr_df.empty
        and corr_df.shape[0] >= 2
        and corr_df.shape[1] >= 2
    )
    corr_table_html = None
    if corr_df is not None and not corr_df.empty:
        try:
            corr_table_html = corr_df.to_html(
                border=0,
                escape=True,
                float_format=lambda v: (f"{v:.3f}" if pd.notna(v) else ""),
            )
        except Exception as exc:
            print(f"[relations] corr table html failed: {exc}")
    corr_cols = (
        [str(c) for c in corr_df.columns.tolist()]
        if corr_df is not None and not corr_df.empty
        else []
    )
    out = {
        "run_id": run_id,
        "data_source": source_name,
        "insights": insights,
        "top_pairs": top_pairs,
        "corr_table_html": corr_table_html,
        "heatmap_path": heatmap_relpath,
        "has_corr": has_corr,
        "numeric_cols": numeric_cols,
        "correlation_columns": corr_cols,
        "relation_preview_columns": [],
        "relation_preview_records": [],
    }
    _relations_append_interpretive(df, out)
    return out


def _core_relations_payload_from_df(run_id, df, source_name, mode):
    """
    Payload de correlaciones coherente con ``/relations`` (sin acciones ni mensajes).
    Con menos de dos numéricas tipadas, bloquea Pearson sin calcular matriz.
    """
    m = "normal"
    src = (source_name or "").strip()
    if df is None or not src:
        out = _empty_relations_payload(run_id)
        out["data_source"] = src
        return out
    tipos_early = get_types_cached(run_id, df, m)
    if len(tipos_early.get("numericas", [])) < 2:
        blocked = {
            "run_id": run_id,
            "data_source": source_name,
            "insights": [],
            "top_pairs": [],
            "corr_table_html": None,
            "heatmap_path": None,
            "has_corr": False,
            "numeric_cols": int(df.select_dtypes(include="number").shape[1]),
            "correlation_blocked_normal": True,
            "correlation_columns": list(tipos_early.get("numericas", [])),
            "relation_preview_columns": [],
            "relation_preview_records": [],
        }
        _relations_append_interpretive(df, blocked)
        return blocked
    pl = _build_relations_page_dict(run_id, df, source_name, analytical_mode=m)
    pl["correlation_blocked_normal"] = False
    _enrich_relations_preview_dataframe(df, pl)
    return pl


def _empty_tipos():
    return {
        "numericas": [],
        "categoricas": [],
        "temporales": [],
        "booleanas": [],
        "ids": [],
    }


def _empty_relations_payload(run_id=None):
    return {
        "run_id": run_id,
        "data_source": "",
        "insights": [],
        "top_pairs": [],
        "corr_table_html": None,
        "heatmap_path": None,
        "has_corr": False,
        "numeric_cols": 0,
        "correlation_blocked_normal": False,
        "correlation_columns": [],
        "relation_preview_columns": [],
        "relation_preview_records": [],
        "rel_detected": [],
        "rel_patterns": [],
        "rel_limitation_text": None,
        "rel_cat_chart_svg": "",
        "rel_heatmap_caption": "",
        "rel_show_technical_details": False,
    }


def _summary_from_tipos_df(df, tipos):
    """Misma estructura que la vista /types (conteos por tipo)."""
    ncols = len(df.columns) if df is not None else sum(
        len(tipos.get(k, [])) for k in _empty_tipos()
    )
    return {
        "total_columnas": ncols,
        "num_numericas": len(tipos.get("numericas", [])),
        "num_categoricas": len(tipos.get("categoricas", [])),
        "num_temporales": len(tipos.get("temporales", [])),
        "num_booleanas": len(tipos.get("booleanas", [])),
        "num_ids": len(tipos.get("ids", [])),
    }


def generate_final_report(df, tipos, relations_data):
    """
    Sintetiza tipos, correlaciones y estadísticos básicos en conclusiones legibles.

    ``relations_data`` es el mismo dict que usa ``relations_view.html`` (payload).
    """
    tipos = tipos if isinstance(tipos, dict) else _empty_tipos()
    for k in _empty_tipos():
        tipos.setdefault(k, [])
    rel = relations_data if isinstance(relations_data, dict) else _empty_relations_payload()

    conclusiones = []
    hallazgos_clave = []

    total_cols = len(df.columns) if df is not None else sum(
        len(tipos[k]) for k in _empty_tipos()
    )
    n_num = len(tipos["numericas"])
    n_cat = len(tipos["categoricas"])
    n_temp = len(tipos["temporales"])
    n_bool = len(tipos["booleanas"])
    n_ids = len(tipos["ids"])

    if total_cols == 0:
        resumen_general = (
            "No hay datos limpios disponibles para el último run; "
            "ejecute el pipeline o cargue datos para generar un informe."
        )
        return {
            "conclusiones": [
                "Sin columnas analizables no es posible emitir conclusiones automáticas.",
            ],
            "resumen_general": resumen_general,
            "hallazgos_clave": ["Dataset vacío o sin archivo data_limpia."],
        }

    resumen_general = (
        f"El conjunto analizado incluye {total_cols} variable(s). "
        f"Desde el punto de vista de tipos: {n_num} numérica(s), {n_cat} categórica(s), "
        f"{n_temp} temporal(es), {n_bool} booleana(s) y {n_ids} identificador(es) de alta cardinalidad."
    )

    if n_cat >= n_num and n_cat > 0:
        conclusiones.append(
            "La mayoría de variables son categóricas, lo que orienta el análisis "
            "hacia perfiles, zonas y clasificación de incidentes."
        )
    if n_num >= 3:
        conclusiones.append(
            "Hay varias variables numéricas, adecuadas para tendencias, agregaciones "
            "y modelos de pronóstico."
        )
    elif n_num > 0:
        conclusiones.append(
            "El número de variables numéricas es limitado; las correlaciones entre pares "
            "pueden ser escasas."
        )

    if n_temp > 0:
        conclusiones.append(
            "Se detectaron variables temporales, lo que permite estudiar tendencias "
            "y estacionalidad en el tiempo."
        )

    if df is not None:
        prov_col = next(
            (c for c in df.columns if "prov" in str(c).lower()),
            None,
        )
        if prov_col is not None:
            try:
                vc = df[prov_col].astype(str).value_counts(dropna=False).head(1)
                if not vc.empty:
                    top_val, top_cnt = vc.index[0], int(vc.iloc[0])
                    conclusiones.append(
                        f"Los registros se concentran con mayor frecuencia en "
                        f"«{prov_col}» = «{top_val}» ({top_cnt} ocurrencias en los datos analizados)."
                    )
            except Exception as exc:
                print(f"[dashboard] generate_final_report provincia stats: {exc}")

    if rel.get("correlation_blocked_normal") and not rel.get("has_corr"):
        conclusiones.append(
            "No se aplicó la correlación de Pearson: el conjunto es predominantemente categórico. "
            "Puede revisar tipos, EDA y visualizaciones."
        )

    # Evitar duplicar todo el bloque de la sección «Correlaciones» (máx. 2 frases aquí).
    for ins in rel.get("insights", [])[:2]:
        conclusiones.append(ins)

    if rel.get("has_corr") and not rel.get("insights"):
        conclusiones.append(
            "La matriz de correlación está disponible, pero no hay pares que superen "
            "el umbral de correlación destacada (|r| ≥ 0.5)."
        )

    hallazgos_clave.append(
        f"Dimensionalidad: {total_cols} columnas clasificadas en tipos disjuntos."
    )
    if rel.get("top_pairs"):
        a, b, r = rel["top_pairs"][0]
        hallazgos_clave.append(
            f"Correlación más fuerte (|r|): {a} ↔ {b} ({float(r):.2f})."
        )

    if not conclusiones:
        conclusiones.append(
            "El informe automático se basó en la distribución de tipos de variables "
            "y en la disponibilidad de datos limpios."
        )

    if not hallazgos_clave:
        hallazgos_clave.append("No se extrajeron correlaciones destacadas en este run.")

    return {
        "conclusiones": conclusiones[:12],
        "resumen_general": resumen_general,
        "hallazgos_clave": hallazgos_clave[:8],
    }


def _load_forecast_summary(run_path):
    """Lectura ligera de meta + primeras filas de pronóstico para el dashboard."""
    out = {"meta_best": None, "forecast_note": None, "forecast_table_html": None}
    if not run_path:
        return out
    meta_path = os.path.join(run_path, "meta.json")
    if os.path.isfile(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            out["meta_best"] = meta.get("best_model")
        except Exception as exc:
            print(f"[dashboard] meta.json: {exc}")
    fc = os.path.join(run_path, "forecast_3m.csv")
    if os.path.isfile(fc):
        try:
            fdf = pd.read_csv(fc, nrows=8)
            out["forecast_table_html"] = fdf.to_html(index=False, escape=True)
            out["forecast_note"] = (
                f"Pronóstico a 3 meses disponible ({len(fdf)} filas mostradas)."
            )
        except Exception as exc:
            print(f"[dashboard] forecast_3m.csv: {exc}")
    return out


def _dash_humanize_period_label(period_str):
    if not period_str or not str(period_str).strip():
        return ""
    years = re.findall(r"\d{4}", str(period_str))
    if len(years) >= 2 and years[0] != years[-1]:
        return f"{years[0]} – {years[-1]}"
    if len(years) == 1:
        return years[0]
    return str(period_str).strip()[:48]


def _dash_strip_pct_parentheticals(text):
    if not text:
        return ""
    return re.sub(r"\s*\(\s*[0-9.,]+%\s*\)\s*", " ", str(text)).strip()


def _dash_error_band_word(precision_level):
    """Convierte precisión del ajuste (Alta/Media/Baja) a banda de error en lenguaje simple."""
    p = (precision_level or "").strip().lower()
    if p.startswith("alta"):
        return "bajo"
    if p.startswith("media"):
        return "medio"
    if p.startswith("baja"):
        return "alto"
    return ""


def _dash_pick_chart_file(chart_files, pred):
    for f in sorted(chart_files or []):
        if not f or not str(f).lower().endswith(".html"):
            continue
        lo = str(f).lower()
        try:
            if pred(lo):
                return f
        except Exception:
            continue
    return None


def build_dashboard_story_context(
    df,
    run_id,
    run_path,
    summary,
    rel_payload,
    chart_files,
    meta_data,
    analytical_mode=None,
):
    """
    Narrativa principal del reporte final (resumen, hallazgos, 1–2 gráficos, modelo, pronóstico).
    El detalle técnico amplio queda en la plantilla bajo ``<details>``.
    """
    meta_data = meta_data or {}
    summary = summary if isinstance(summary, dict) else {}
    rel_payload = rel_payload if isinstance(rel_payload, dict) else {}
    rid = str(run_id or "")
    rid_ok = rid and is_safe_run_id(rid)

    empty = {
        "ready": False,
        "exec": {
            "dataset_label": "",
            "records_display": "",
            "main_insight": "",
            "best_model": "",
            "forecast_trend_line": "",
        },
        "findings": [],
        "visuals": [],
        "model": {
            "name": "",
            "error_band": "",
            "reason": "",
            "has_block": False,
        },
        "forecast": {
            "has": False,
            "rows": [],
            "trend_word": "",
            "change_display": "",
            "band_token": "",
            "band_caption": "",
        },
    }

    if not summary.get("total_columnas"):
        empty["exec"]["main_insight"] = (
            "Aún no hay un conjunto limpio asociado al último análisis. "
            "Ejecute la carga y el procesamiento."
        )
        return empty

    eda = build_eda_insights_payload(df, rid, meta_data, chart_files or [])
    period = _dash_humanize_period_label(eda.get("eda_summary_period") or "")
    rows_n = eda.get("eda_summary_rows")
    if rows_n is None and df is not None and not getattr(df, "empty", True):
        rows_n = len(df)
    records_display = _fmt_es_integer(rows_n) if rows_n is not None else "—"

    findings_src = list(eda.get("eda_findings") or [])
    for al in eda.get("eda_alerts") or []:
        if al and "data_limpia" not in str(al).lower():
            findings_src.append(al)
    findings_clean = [
        _dash_strip_pct_parentheticals(x) for x in findings_src if str(x).strip()
    ]
    findings_clean = [re.sub(r"\s+", " ", x).strip() for x in findings_clean]

    main_insight = ""
    if findings_clean:
        main_insight = " ".join(findings_clean[:2])
    elif period:
        main_insight = f"Los datos cubren el periodo {period}."
    else:
        main_insight = "Revise el análisis exploratorio para el detalle por variables."

    df_err = load_errores_modelos(run_path) if run_path else None
    mx = build_metrics_insight_context(df_err)
    meta_best = meta_data.get("best_model")
    best_model = (str(meta_best).strip() if meta_best else "") or mx.get(
        "mx_best_model", ""
    )

    df_fc_main = load_forecast_3m(run_path) if run_path else None
    df_fc_int = load_forecast_3m_int(run_path) if run_path else None
    fc_ctx = build_forecast_insight_context(
        df_fc_main, df_fc_int, run_path, rid or None
    )

    forecast_trend_line = ""
    if fc_ctx.get("fc_has_forecast"):
        tw = fc_ctx.get("fc_trend_label") or ""
        ch = fc_ctx.get("fc_change_display") or ""
        if tw and ch:
            forecast_trend_line = f"Tendencia {tw} ({ch})"
        elif tw:
            forecast_trend_line = f"Tendencia {tw}"

    prec = mx.get("mx_precision_level") or ""
    err_band = _dash_error_band_word(prec)
    if mx.get("mx_has_metrics"):
        reason = mx.get("mx_explanation") or (
            "Menor error de predicción frente a los demás modelos evaluados en este run."
        )
    else:
        reason = (
            "En este run no hay comparación automática de errores entre modelos exportada."
        )

    visuals = []
    f_top = _dash_pick_chart_file(
        chart_files, lambda lo: "01_top_delitos" in lo or lo.endswith("top_delitos.html")
    )
    f_ts = _dash_pick_chart_file(
        chart_files,
        lambda lo: "04_serie_media_movil" in lo
        or ("serie" in lo and "media" in lo and "movil" in lo),
    )
    if f_top and rid_ok:
        visuals.append(
            {
                "title": "Top delitos",
                "caption": "Frecuencias relativas de los tipos de incidente más observados.",
                "embed_token": _chart_html_embed_token(rid, f_top),
            }
        )
    if f_ts and rid_ok:
        visuals.append(
            {
                "title": "Tendencia en el tiempo",
                "caption": "Evolución agregada con suavizado para ver la dirección general.",
                "embed_token": _chart_html_embed_token(rid, f_ts),
            }
        )

    band_file = _forecast_pick_band_html(run_path) if run_path else None
    band_token = (
        _chart_html_embed_token(rid, band_file)
        if (band_file and rid_ok)
        else ""
    )

    fc_rows = []
    if fc_ctx.get("fc_has_forecast"):
        for row in (fc_ctx.get("fc_rows_summary") or [])[:3]:
            fc_rows.append(
                {"label": row.get("label", ""), "value": row.get("value", "")}
            )

    key_findings = []
    skip_exec = min(2, len(findings_clean))
    for line in findings_clean[skip_exec : skip_exec + 6]:
        if line and line not in key_findings:
            key_findings.append(line)
    if len(key_findings) < 3 and rel_payload.get("rel_patterns"):
        for p in rel_payload.get("rel_patterns")[:2]:
            ps = str(p).strip()
            if ps and ps not in key_findings:
                key_findings.append(ps)
    key_findings = key_findings[:5]

    return {
        "ready": True,
        "exec": {
            "dataset_label": period or "Periodo no indicado",
            "records_display": records_display,
            "main_insight": main_insight,
            "best_model": best_model or "No indicado",
            "forecast_trend_line": forecast_trend_line,
        },
        "findings": key_findings,
        "visuals": visuals,
        "model": {
            "name": best_model or "No indicado",
            "error_band": err_band,
            "precision_word": prec,
            "reason": reason,
            "has_block": bool(best_model or mx.get("mx_has_metrics")),
        },
        "forecast": {
            "has": bool(fc_ctx.get("fc_has_forecast")),
            "rows": fc_rows,
            "trend_word": fc_ctx.get("fc_trend_label") or "",
            "change_display": fc_ctx.get("fc_change_display") or "",
            "band_token": band_token,
            "band_caption": "Banda de pronóstico respecto a la historia reciente.",
        },
    }


def get_chart_paths(run_dir_path):
    if not run_dir_path or not os.path.isdir(run_dir_path):
        return []
    charts = []
    for file in os.listdir(run_dir_path):
        if file.endswith(".html"):
            charts.append(file)
    return charts


def _chart_html_embed_token(run_id, filename):
    """Token opaco para servir un HTML de gráfico sin exponer el nombre en la URL."""
    rid = str(run_id or "")
    fn = str(filename or "")
    secret = str(app.secret_key or "dev").encode("utf-8", errors="replace")
    msg = f"{rid}\x1e{fn}".encode("utf-8", errors="replace")
    return hmac.new(secret, msg, hashlib.sha256).hexdigest()[:26]


def build_charts_gallery_payload(chart_files, analytical_mode=None, run_id=None):
    """
    Agrupa gráficos HTML del run en secciones y tarjetas interpretativas.
    Los nombres de archivo no se muestran al usuario; el iframe usa ``charts_html_embed``.
    """
    files = [
        str(f)
        for f in (chart_files or [])
        if f and str(f).lower().endswith(".html")
    ]
    files_sorted = sorted(set(files))
    if not files_sorted:
        return {
            "sections": [],
            "cards_flat": [],
            "default_card_id": None,
        }

    lower = {f: f.lower() for f in files_sorted}
    used = set()

    def take_first(pred):
        for f in files_sorted:
            if f in used:
                continue
            if pred(lower[f]):
                used.add(f)
                return f
        return None

    slot_defs = [
        (
            "main",
            lambda lo: "01_top_delitos" in lo or lo.endswith("top_delitos.html"),
            "Top delitos",
            "Muestra los delitos más frecuentes en el conjunto de datos.",
        ),
        (
            "main",
            lambda lo: "02_boxplot_provincia" in lo or (
                "boxplot" in lo and "provincia" in lo
            ),
            "Distribución por provincia",
            "Permite identificar dónde ocurren más incidentes.",
        ),
        (
            "main",
            lambda lo: "03_pie_sexo" in lo or (
                "pie" in lo and "sexo" in lo
            ),
            "Distribución por sexo",
            "Muestra la proporción de registros según sexo.",
        ),
        (
            "time",
            lambda lo: "04_serie_media_movil" in lo or (
                "serie" in lo and "media" in lo and "movil" in lo
            ),
            "Tendencia en el tiempo",
            "Resume cómo cambia la actividad a lo largo del periodo observado.",
        ),
    ]

    rid_ok = run_id and is_safe_run_id(str(run_id))
    cards_flat = []
    for section_id, pred, title, desc in slot_defs:
        fname = take_first(pred)
        if fname:
            cid = f"g{len(cards_flat)}"
            cards_flat.append(
                {
                    "card_id": cid,
                    "section_id": section_id,
                    "title": title,
                    "description": desc,
                    "file": fname,
                    "embed_token": (
                        _chart_html_embed_token(run_id, fname) if rid_ok else ""
                    ),
                }
            )

    def pick_correlation_html():
        exp_name = "correlation_heatmap_experimental.html"
        norm_name = "correlation_heatmap.html"
        has_exp = exp_name in files_sorted and exp_name not in used
        has_norm = norm_name in files_sorted and norm_name not in used
        if has_norm:
            return norm_name
        if has_exp:
            return exp_name
        for f in files_sorted:
            if f in used:
                continue
            lo = lower[f]
            if "correlation" in lo and "heatmap" in lo:
                return f
        return None

    corr_f = pick_correlation_html()
    if corr_f:
        used.add(corr_f)
        cards_flat.append(
            {
                "card_id": f"g{len(cards_flat)}",
                "section_id": "relations",
                "title": "Relación entre variables",
                "description": "Comparación visual entre las mediciones numéricas disponibles.",
                "file": corr_f,
                "embed_token": (
                    _chart_html_embed_token(run_id, corr_f) if rid_ok else ""
                ),
            }
        )

    fc = take_first(lambda lo: lo == "forecast_band.html")
    if not fc:
        fc = take_first(lambda lo: "forecast_band" in lo and lo.endswith(".html"))
    if fc:
        cards_flat.append(
            {
                "card_id": f"g{len(cards_flat)}",
                "section_id": "forecast",
                "title": "Proyección futura",
                "description": "Escenario de evolución con bandas de incertidumbre del modelo.",
                "file": fc,
                "embed_token": (
                    _chart_html_embed_token(run_id, fc) if rid_ok else ""
                ),
            }
        )
    fc2 = take_first(lambda lo: lo == "forecast_plot.html")
    if fc2:
        cards_flat.append(
            {
                "card_id": f"g{len(cards_flat)}",
                "section_id": "forecast",
                "title": "Evolución del pronóstico",
                "description": "Serie estimada frente a la historia reciente exportada por el pipeline.",
                "file": fc2,
                "embed_token": (
                    _chart_html_embed_token(run_id, fc2) if rid_ok else ""
                ),
            }
        )

    for f in files_sorted:
        if f in used:
            continue
        lo = lower[f]
        section_id = "extra"
        title = "Vista complementaria"
        desc = "Otra salida gráfica de este análisis."
        if "forecast" in lo:
            section_id = "forecast"
            title = "Vista de pronóstico"
            desc = "Gráfico adicional del paso de proyección en este run."
        elif "correlation" in lo or "heatmap" in lo:
            section_id = "relations"
            title = "Relación entre variables"
            desc = "Comparación visual entre mediciones del conjunto."
        elif lo.startswith("heat_prov_pro") or "heat_prov" in lo:
            section_id = "extra"
            title = "Mapa de intensidad"
            desc = "Distribución geográfica agregada del pipeline."
        elif "provincia" in lo and ("map" in lo or "geo" in lo):
            section_id = "extra"
            title = "Mapa o geografía"
            desc = "Vista espacial de los datos disponibles en este run."
        used.add(f)
        cards_flat.append(
            {
                "card_id": f"g{len(cards_flat)}",
                "section_id": section_id,
                "title": title,
                "description": desc,
                "file": f,
                "embed_token": (
                    _chart_html_embed_token(run_id, f) if rid_ok else ""
                ),
            }
        )

    section_order = [
        ("main", "Gráficos principales", "charts-section--main"),
        ("time", "Tendencia temporal", "charts-section--time"),
        ("relations", "Relaciones", "charts-section--relations"),
        ("forecast", "Pronóstico", "charts-section--forecast"),
        ("extra", "Gráficos adicionales", "charts-section--extra"),
    ]
    by_section = {sid: [] for sid, _, _ in section_order}
    for c in cards_flat:
        sid = c["section_id"]
        if sid not in by_section:
            sid = "extra"
            c = dict(c)
            c["section_id"] = "extra"
        by_section[sid].append(c)

    sections = []
    for sid, title, css in section_order:
        bucket = by_section.get(sid) or []
        if not bucket:
            continue
        sections.append(
            {
                "section_id": sid,
                "title": title,
                "accent_class": css,
                "cards": bucket,
            }
        )

    default_card_id = None
    for c in cards_flat:
        lo = lower[c["file"]]
        if "01_top_delitos" in lo or lo.endswith("top_delitos.html"):
            default_card_id = c["card_id"]
            break
    if not default_card_id and cards_flat:
        default_card_id = cards_flat[0]["card_id"]

    return {
        "sections": sections,
        "cards_flat": cards_flat,
        "default_card_id": default_card_id,
    }


# Anclas del panel hub «Ver visualizaciones» → una sola sección de la galería.
_CHART_HUB_SCROLL_TO_SECTION = {
    "charts-sec-main": "main",
    "charts-sec-time": "time",
    "charts-sec-relations": "relations",
    "charts-sec-forecast": "forecast",
}


def _has_heatmap_in_relations(chart_gallery):
    """True si la sección ``relations`` del payload incluye al menos un HTML con heatmap."""
    if not chart_gallery:
        return False
    for section in chart_gallery.get("sections") or []:
        if section.get("section_id") != "relations":
            continue
        for card in section.get("cards") or []:
            file_lo = str(card.get("file") or "").lower()
            if "heatmap" in file_lo:
                return True
    return False


def _latest_run_chart_gallery_unfiltered():
    """Galería completa del último run (sin filtro hub); para flags en ``base``."""
    run_path = get_latest_run()
    if not run_path:
        return None
    run_id = os.path.basename(run_path)
    mode = get_analytical_mode()
    chart_files = []
    if run_path:
        chart_files = get_chart_paths(run_path)
        chart_files.sort()
    return build_charts_gallery_payload(chart_files, mode, run_id)


def _latest_run_has_relations_heatmap():
    gal = _latest_run_chart_gallery_unfiltered()
    if not gal:
        return False
    return _has_heatmap_in_relations(gal)


def _refine_chart_section_cards_for_hub_intent(scroll, want_sid, sections_list):
    """
    Tras el filtro por ``section_id``, recorta ``cards`` según la intención del
    botón del hub (sin tocar ``build_charts_gallery_payload``).

    Relaciones (hub): prioridad ``heatmap`` en nombre de archivo, luego
    ``correlation``, luego primera tarjeta de la sección; siempre como máximo
    una tarjeta.
    """
    if not scroll or not want_sid or not sections_list:
        return sections_list
    refined = []
    for sec in sections_list:
        sid = sec.get("section_id")
        cards = list(sec.get("cards") or [])
        if scroll == "charts-sec-main" and sid == "main":
            cards = [
                c
                for c in cards
                if "top_delitos" in str(c.get("file", "")).lower()
                or "top_delitos" in str(c.get("card_id", "")).lower()
            ]
        elif scroll == "charts-sec-relations" and sid == "relations":
            cards_all = cards
            fn_lo = lambda c: str(c.get("file", "") or "").lower()
            heatmaps = [c for c in cards_all if "heatmap" in fn_lo(c)]
            if not heatmaps:
                heatmaps = [c for c in cards_all if "correlation" in fn_lo(c)]
            if not heatmaps and cards_all:
                heatmaps = list(cards_all)
            cards = heatmaps[:1]
        elif scroll == "charts-sec-forecast" and sid == "forecast":
            cards = [
                c
                for c in cards
                if "forecast_band" in str(c.get("file", "")).lower()
            ]
        # charts-sec-time: sin cambios adicionales
        sec_copy = dict(sec)
        sec_copy["cards"] = cards
        refined.append(sec_copy)
    return refined


def _filter_chart_gallery_for_panel(chart_gallery, panel_scroll_to):
    """
    Deja solo la sección acorde al botón del hub; vacío ``charts-all`` o página
    completa conservan todas las secciones (incl. ``extra``).

    Para anclas del hub, aplica un segundo recorte por intención (una tarjeta
    o conjunto acotado) sin modificar el payload base.
    """
    if not chart_gallery:
        return chart_gallery
    scroll = (panel_scroll_to or "").strip()
    want_sid = _CHART_HUB_SCROLL_TO_SECTION.get(scroll)
    if not want_sid:
        out = dict(chart_gallery)
        out.pop("charts_hub_intent_empty_message", None)
        return out
    sections_in = list(chart_gallery.get("sections") or [])
    filtered_sections = [s for s in sections_in if s.get("section_id") == want_sid]
    refined_sections = _refine_chart_section_cards_for_hub_intent(
        scroll, want_sid, filtered_sections
    )
    cards_flat_f = []
    for s in refined_sections:
        for c in s.get("cards") or []:
            cards_flat_f.append(c)
    default_card_id = None
    for c in cards_flat_f:
        lo = str(c.get("file") or "").lower()
        if "01_top_delitos" in lo or lo.endswith("top_delitos.html"):
            default_card_id = c.get("card_id")
            break
    if not default_card_id and cards_flat_f:
        default_card_id = cards_flat_f[0].get("card_id")
    out = dict(chart_gallery)
    out["sections"] = refined_sections
    out["cards_flat"] = cards_flat_f
    out["default_card_id"] = default_card_id
    total_cards = sum(len(s.get("cards") or []) for s in refined_sections)
    if total_cards == 0:
        out["charts_hub_intent_empty_message"] = (
            "No hay visualizaciones disponibles para esta categoría."
        )
    else:
        out.pop("charts_hub_intent_empty_message", None)
    return out


def _eda_find_column(df, *candidates):
    """Primera columna cuyo nombre coincide o contiene alguno de ``candidates`` (insensible a mayúsculas)."""
    if df is None or getattr(df, "empty", True):
        return None
    lower_map = {str(c).lower(): c for c in df.columns}
    for cand in candidates:
        cl = cand.lower()
        if cl in lower_map:
            return lower_map[cl]
    for c in df.columns:
        cl = str(c).lower()
        for cand in candidates:
            if cand.lower() in cl:
                return c
    return None


def _eda_pick_chart(chart_paths, *needles):
    if not chart_paths:
        return None
    indexed = [(f, f.lower()) for f in chart_paths]
    for needle in needles:
        nl = needle.lower()
        for fname, lo in indexed:
            if nl in lo:
                return fname
    return None


def _eda_dominant_kind(df):
    if df is None or getattr(df, "empty", True):
        return None
    n_num = 0
    for c in df.columns:
        if pdt.is_bool_dtype(df[c]):
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            n_num += 1
    n_cat = len(df.columns) - n_num
    if n_cat > n_num * 1.15:
        return "Categórico"
    if n_num > n_cat * 1.15:
        return "Numérico"
    return "Mixto"


def _eda_unknown_mask(s):
    if s is None:
        return pd.Series([], dtype=bool)
    st = s.astype(str).str.strip()
    na_like = st.str.lower().isin(("", "nan", "none", "null", "-", "n/a", "na"))
    unk = st.str.contains(r"desconoc", case=False, na=False)
    return s.isna() | na_like | unk


def _eda_unknown_pct_series(s):
    if s is None or len(s) == 0:
        return 0.0
    m = _eda_unknown_mask(s)
    return round(100.0 * float(m.mean()), 1)


def _eda_day_night_counts(df):
    """Retorna (día, noche, total_horas_válidas) usando 06:00–17:59 como día."""
    if df is None or getattr(df, "empty", True):
        return None, None, 0
    hi = _eda_find_column(df, "Hora_Inicio", "hora_inicio", "hora")
    if hi and df[hi].notna().any():
        hnum = pd.to_numeric(df[hi], errors="coerce")
        valid = hnum.notna()
        if int(valid.sum()) == 0:
            return None, None, 0
        hh = (np.floor(hnum[valid]).astype(int) % 24).values
        day = int(((hh >= 6) & (hh < 18)).sum())
        night = int(len(hh) - day)
        return day, night, int(len(hh))
    hr = _eda_find_column(df, "Hora_Rango", "hora_rango")
    if not hr:
        return None, None, 0

    def _start_hour(val):
        m = re.match(r"^\s*(\d{1,2}):", str(val))
        return int(m.group(1)) % 24 if m else None

    hrs = df[hr].map(_start_hour)
    valid = hrs.notna()
    if int(valid.sum()) == 0:
        return None, None, 0
    hh = hrs[valid].astype(int).values
    day = int(((hh >= 6) & (hh < 18)).sum())
    night = int(len(hh) - day)
    return day, night, int(len(hh))


def _eda_fmt_period(dmin, dmax):
    try:
        a = pd.Timestamp(dmin)
        b = pd.Timestamp(dmax)
        if pd.isna(a) or pd.isna(b):
            return ""
        return f"{a.strftime('%Y-%m-%d')} – {b.strftime('%Y-%m-%d')}"
    except Exception:
        return f"{dmin} – {dmax}"


def build_eda_insights_payload(df, current_run_id, meta_data, chart_paths):
    """
    Contexto Jinja para la vista de análisis exploratorio (solo presentación).
    Deriva métricas desde ``data_limpia``; ``meta_data`` solo como respaldo de filas/fechas.
    """
    meta_data = meta_data or {}
    chart_paths = chart_paths or []
    empty = {
        "eda_ready": False,
        "eda_summary_rows": None,
        "eda_summary_period": "",
        "eda_summary_kind": "",
        "eda_findings": [],
        "eda_top_delitos": [],
        "eda_dist_delitos": {
            "title": "Top 5 delitos",
            "chart_file": None,
            "embed_token": None,
        },
        "eda_dist_provincia": {
            "title": "Distribución por provincia",
            "chart_file": None,
            "embed_token": None,
        },
        "eda_dist_sexo": {
            "title": "Distribución por sexo",
            "chart_file": None,
            "embed_token": None,
        },
        "eda_alerts": [],
        "eda_forecast_download_ok": False,
        "eda_meta_download_ok": False,
    }
    if not current_run_id or not is_safe_run_id(str(current_run_id)):
        return empty

    fpath = os.path.join(ARTIFACTS_DIR, current_run_id, "forecast_3m.csv")
    mpath = os.path.join(ARTIFACTS_DIR, current_run_id, "meta.json")
    empty["eda_forecast_download_ok"] = os.path.isfile(fpath)
    empty["eda_meta_download_ok"] = os.path.isfile(mpath)

    if df is None or getattr(df, "empty", True):
        rows = meta_data.get("rows")
        if rows is not None:
            empty["eda_summary_rows"] = int(rows)
        dmi, dma = meta_data.get("date_min"), meta_data.get("date_max")
        if dmi and dma:
            empty["eda_summary_period"] = _eda_fmt_period(dmi, dma)
        empty["eda_alerts"].append("No se encontró ``data_limpia`` para este run.")
        return empty

    out = {**empty, "eda_ready": True}
    n = len(df)
    out["eda_summary_rows"] = int(n)
    out["eda_summary_kind"] = _eda_dominant_kind(df) or ""

    fecha_col = _eda_find_column(df, "Fecha", "fecha", "FECHA")
    period = ""
    if fecha_col:
        fe = pd.to_datetime(df[fecha_col], errors="coerce")
        nat_pct = float(fe.isna().mean() * 100.0) if len(fe) else 0.0
        if nat_pct > 0.6:
            out["eda_alerts"].append(
                f"Datos temporales incompletos: ~{round(nat_pct, 1)}% de fechas no válidas o vacías."
            )
        if fe.notna().any():
            dmin, dmax = fe.min(), fe.max()
            period = _eda_fmt_period(dmin, dmax)
            try:
                span = (pd.Timestamp(dmax) - pd.Timestamp(dmin)).days
                if span is not None and span < 1 and n > 50:
                    out["eda_alerts"].append(
                        "Rango temporal muy corto en comparación con el volumen de datos."
                    )
            except Exception:
                pass
    if not period:
        dmi, dma = meta_data.get("date_min"), meta_data.get("date_max")
        if dmi and dma:
            period = _eda_fmt_period(dmi, dma)
    out["eda_summary_period"] = period

    findings = []
    delito_c = _eda_find_column(df, "Delito", "delito")
    if delito_c:
        vc = df[delito_c].astype(str).str.strip()
        vc = vc.replace("", pd.NA).dropna()
        counts = vc.value_counts()
        tot_d = int(counts.sum()) if len(counts) else 0
        if tot_d and len(counts):
            top = counts.index[0]
            pct = round(100.0 * float(counts.iloc[0]) / float(tot_d), 1)
            findings.append(f"El delito más frecuente es «{top}» ({pct}%).")
            out["eda_top_delitos"] = [
                {"name": str(name), "pct": round(100.0 * float(cnt) / float(tot_d), 1)}
                for name, cnt in counts.head(5).items()
            ]

    prov_c = _eda_find_column(df, "Provincia", "provincia")
    if prov_c:
        pv = df[prov_c].astype(str).str.strip()
        pv = pv.replace("", pd.NA).dropna()
        pc = pv.value_counts()
        if len(pc):
            findings.append(f"La provincia con más casos es «{pc.index[0]}».")

    sexo_c = _eda_find_column(df, "Sexo", "sexo")
    if sexo_c:
        unk = _eda_unknown_mask(df[sexo_c])
        p_unk = round(100.0 * float(unk.mean()), 1) if len(df) else 0.0
        if p_unk > 0.05:
            findings.append(f"{p_unk}% de los registros tienen sexo desconocido o vacío.")

    day, night, htot = _eda_day_night_counts(df)
    if htot and day is not None and night is not None:
        if day >= night:
            findings.append(
                f"La mayoría de los eventos con hora conocida ocurren en horario diurno ({round(100.0 * day / htot, 1)}% entre 06:00 y 17:59)."
            )
        else:
            findings.append(
                f"La mayoría de los eventos con hora conocida ocurren en horario nocturno ({round(100.0 * night / htot, 1)}% fuera de 06:00–17:59)."
            )

    out["eda_findings"] = findings[:6]

    bad_cols = []
    for c in df.columns:
        s = df[c]
        if pdt.is_datetime64_any_dtype(s):
            continue
        if pd.api.types.is_numeric_dtype(s) and not pdt.is_bool_dtype(s):
            continue
        pct = _eda_unknown_pct_series(s)
        if pct >= 12.0:
            bad_cols.append(f"{c} ({pct}%)")
    if bad_cols:
        tail = ", ".join(bad_cols[:6])
        if len(bad_cols) > 6:
            tail += "…"
        out["eda_alerts"].append(
            f"Alta proporción de valores vacíos o desconocidos en: {tail}."
        )

    out["eda_dist_delitos"]["chart_file"] = _eda_pick_chart(
        chart_paths, "top_delito", "01_top", "delitos"
    )
    out["eda_dist_provincia"]["chart_file"] = _eda_pick_chart(
        chart_paths, "boxplot_provincia", "02_", "provincia"
    )
    out["eda_dist_sexo"]["chart_file"] = _eda_pick_chart(
        chart_paths, "pie_sexo", "03_", "sexo"
    )

    rid = str(current_run_id)
    for key in ("eda_dist_delitos", "eda_dist_provincia", "eda_dist_sexo"):
        cf = out[key].get("chart_file")
        if cf:
            out[key]["embed_token"] = _chart_html_embed_token(rid, cf)
        else:
            out[key]["embed_token"] = None

    return out


def build_eda_exploratory_context(run_dir_path, current_run_id):
    """
    Contexto exclusivo del módulo EDA (``results.html`` y partial ``eda``):
    inspección de ``data_limpia`` y artefactos de distribución, sin pronóstico
    ni métricas de modelos (esos van al reporte / otras rutas).
    """
    if not current_run_id or not run_dir_path:
        return None
    meta_data = {}
    if run_dir_path and os.path.isdir(run_dir_path):
        meta_path = os.path.join(run_dir_path, "meta.json")
        if os.path.exists(meta_path):
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta_data = json.load(f)
            except (json.JSONDecodeError, OSError):
                meta_data = {}
    chart_paths = get_chart_paths(run_dir_path)
    eda_df = None
    if run_dir_path and os.path.isdir(run_dir_path):
        eda_df, _ = load_data_limpia(run_dir_path)
    eda_ctx = build_eda_insights_payload(
        eda_df, current_run_id, meta_data, chart_paths
    )
    return {
        "latest_run": run_dir_path,
        "current_run_id": current_run_id,
        "chart_paths": chart_paths,
        "latest_run_dir": current_run_id,
        **eda_ctx,
    }


def get_results_page_template_kwargs(run_dir_path, current_run_id):
    """Alias estable: mismo contrato que antes, solo contexto EDA."""
    return build_eda_exploratory_context(run_dir_path, current_run_id)


def build_results_page(run_dir_path, current_run_id):
    kw = get_results_page_template_kwargs(run_dir_path, current_run_id)
    if kw is None:
        return render_empty_runs_state(
            page_title="Análisis exploratorio",
            hint="Selecciona un dataset para comenzar el análisis.",
        )
    return fl.render_template("results.html", **kw)


def resolve_latest_results_run():
    """Primera entrada del historial con carpeta de artefactos existente."""
    for row in history_sidebar():
        rid = row.get("run_id")
        if rid and is_safe_run_id(rid):
            p = os.path.join(ARTIFACTS_DIR, rid)
            if os.path.isdir(p):
                return p, rid
    return None, None


@app.route("/", methods=["GET", "POST"])
def home():
    message = None
    status = None
    csv_files = list_csv_files()

    if fl.request.method == "POST":
        uploads = _upload_filestorage_list_from_request()
        if not uploads:
            message = "No se seleccionaron archivos."
            status = "danger"
        else:
            res = persist_multiple_csv_filestorage_list(uploads)
            message = res["message"]
            status = res["status"]
            if res["ok"]:
                csv_files = list_csv_files()
                fl.flash(res["message"], "success")
            else:
                fl.flash(res["message"], "danger")

    allowed_names = set(csv_files)
    sess_name = (fl.session.get(SESSION_ACTIVE_CSV_KEY) or "").strip()
    if sess_name and sess_name not in allowed_names:
        fl.session.pop(SESSION_ACTIVE_CSV_KEY, None)
    active_csv = (fl.session.get(SESSION_ACTIVE_CSV_KEY) or "").strip()
    if active_csv not in allowed_names:
        active_csv = ""

    return fl.render_template(
        "home.html",
        message=message,
        status=status,
        csv_files=csv_files,
        active_csv=active_csv,
    )


@app.route("/run", methods=["GET", "POST"])
def run():
    csv_files = list_csv_files()
    allowed_set = set(csv_files)

    if fl.request.method == "GET":
        ds_q = (fl.request.args.get("dataset") or "").strip()
        if ds_q and ds_q in allowed_set:
            fl.session[SESSION_ACTIVE_CSV_KEY] = ds_q
            print(f"[run] active dataset from query: {ds_q!r}")
        cm_q = (fl.request.args.get("csv_mode") or "").strip().lower()
        if cm_q in ("single", "all"):
            fl.session[SESSION_ML_CSV_STRATEGY] = cm_q
            print(f"[run] csv strategy from query: {cm_q!r}")

    if fl.request.method == "POST":
        try:
            cm_form = (fl.request.form.get("csv_mode") or "").strip().lower()
            if cm_form in ("single", "all"):
                fl.session[SESSION_ML_CSV_STRATEGY] = cm_form
            strategy = (fl.session.get(SESSION_ML_CSV_STRATEGY) or "single").strip().lower()
            if strategy not in ("single", "all"):
                strategy = "single"
                fl.session[SESSION_ML_CSV_STRATEGY] = strategy

            if strategy == "all":
                if not csv_files:
                    fl.flash(
                        "No hay archivos CSV en data/. Suba al menos uno.",
                        "danger",
                    )
                    return fl.redirect(fl.url_for("home"))
                source_mode = "all_csv"
                selected_file = f"(todos: {len(csv_files)} archivos)"
                psid_all = uuid.uuid4().hex
                ml_script = os.path.normpath(
                    os.path.join(os.path.dirname(__file__), "..", "ML1.py")
                )
                cmd = [sys.executable, ml_script, source_mode]
            else:
                active_sess = (fl.session.get(SESSION_ACTIVE_CSV_KEY) or "").strip()
                if not active_sess or active_sess not in allowed_set:
                    fl.flash(
                        "Seleccione un dataset válido desde Carga de datos.",
                        "danger",
                    )
                    return fl.redirect(fl.url_for("home"))

                psid = uuid.uuid4().hex
                out = _execute_ml1_for_csv_dataset(
                    active_sess, pipeline_session_id=psid
                )
                if out["ok"]:
                    _register_history_from_execute_output(out)
                    rid = (out.get("run_id") or "").strip()
                    if rid and is_safe_run_id(rid):
                        return fl.redirect(fl.url_for("results_run", run_id=rid))
                    return fl.redirect(fl.url_for("results"))
                fl.flash(out.get("error") or "Error de ejecución.", "danger")
                return fl.redirect(fl.url_for("run"))

            before_ids = _snapshot_artifact_run_ids()
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=os.path.dirname(ml_script),
                env={**os.environ, "PYTHONIOENCODING": "utf-8"},
            )
            if result.returncode == 0:
                latest_run = _resolve_run_dir_after_ml1(before_ids)
                if latest_run:
                    rid_new = os.path.basename(latest_run)
                    meta_path = os.path.join(latest_run, "meta.json")
                    best_model = "Unknown"
                    wrmse = None
                    meta_rid = rid_new
                    if os.path.exists(meta_path):
                        with open(meta_path, "r", encoding="utf-8") as f:
                            meta = json.load(f)
                        best_model = meta.get("best_model", "Unknown")
                        meta_rid = meta.get("run_id") or rid_new
                        errors_path = os.path.join(
                            latest_run, "errores_modelos.csv"
                        )
                        if os.path.exists(errors_path):
                            errors_df = pd.read_csv(errors_path, index_col=0)
                            if best_model in errors_df.index:
                                wrmse = errors_df.loc[best_model, "WRMSE"]
                    add_to_history(
                        meta_rid if is_safe_run_id(str(meta_rid)) else rid_new,
                        best_model,
                        wrmse,
                        source_mode=source_mode,
                        source_file=selected_file,
                        pipeline_session_id=psid_all,
                    )
                    if is_safe_run_id(rid_new):
                        return fl.redirect(
                            fl.url_for("results_run", run_id=rid_new)
                        )
                return fl.redirect(fl.url_for("results"))
            err = (result.stderr or "").strip() or (result.stdout or "").strip()
            fl.flash(err or "Error de ejecución.", "danger")
        except Exception as e:
            fl.flash(str(e), "danger")
        return fl.redirect(fl.url_for("run"))

    active_name = (fl.session.get(SESSION_ACTIVE_CSV_KEY) or "").strip()
    if active_name not in allowed_set:
        active_name = ""

    strategy = (fl.session.get(SESSION_ML_CSV_STRATEGY) or "single").strip().lower()
    if strategy not in ("single", "all"):
        strategy = "single"

    transform_bundle = build_transformation_preview_pair(None)
    if active_name:
        csv_path = os.path.join(DEFAULT_SAVE_DIR, active_name)
        if os.path.isfile(csv_path):
            try:
                raw_df = pd.read_csv(csv_path, nrows=_PREVIEW_ROWS_CAP)
                transform_bundle = build_transformation_preview_pair(raw_df)
            except Exception as exc:
                print(f"[run] preview read failed for {active_name!r}: {exc}")

    return fl.render_template(
        "run.html",
        csv_files=csv_files,
        active_dataset=active_name,
        has_csv=bool(csv_files),
        ml_csv_strategy=strategy,
        transform_raw_columns=transform_bundle["raw_columns"],
        transform_raw_records=transform_bundle["raw_records"],
        transform_clean_columns=transform_bundle["clean_columns"],
        transform_clean_records=transform_bundle["clean_records"],
        transform_raw_cell_classes=transform_bundle["raw_cell_classes"],
        transform_clean_cell_classes=transform_bundle["clean_cell_classes"],
    )


@app.route("/results")
def results():
    _p, rid = resolve_latest_results_run()
    if rid:
        return fl.redirect(fl.url_for("results_run", run_id=rid))
    return render_empty_runs_state(
        page_title="Análisis exploratorio",
        hint="Subí CSV en Carga de datos y ejecutá Procesamiento → Procesar para generar una ejecución.",
    )


@app.route("/results/<run_id>")
def results_run(run_id):
    if not is_safe_run_id(run_id):
        fl.abort(404)
    if not run_id_in_history(run_id):
        fl.abort(404)
    run_path = os.path.join(ARTIFACTS_DIR, run_id)
    if not os.path.isdir(run_path):
        fl.abort(404)
    return build_results_page(run_path, run_id)


@app.route("/api/history/clear", methods=["POST"])
def api_history_clear():
    """
    Vacía ``run_history.json`` (lista vacía). No elimina carpetas en ``artifacts/``.
    """
    try:
        save_history([])
    except OSError as e:
        return (
            fl.jsonify(
                {
                    "ok": False,
                    "error": f"No se pudo escribir el historial: {e}",
                }
            ),
            500,
        )
    except (TypeError, ValueError) as e:
        return (
            fl.jsonify({"ok": False, "error": str(e) or "Error al guardar el historial."}),
            500,
        )
    return fl.jsonify(
        {
            "ok": True,
            "message": "Historial limpiado correctamente",
        }
    )


@app.route("/runs", methods=["GET"])
def runs_list():
    active = (fl.request.args.get("active") or "").strip()
    if active and not is_safe_run_id(active):
        active = ""
    return fl.jsonify({"runs": runs_api_payload(active)})


@app.route("/runs", methods=["POST"])
def runs_create():
    data = fl.request.get_json(silent=True) or {}
    rid = (data.get("run_id") or data.get("id") or "").strip()
    if not rid:
        lr = get_latest_run()
        rid = os.path.basename(lr) if lr else ""
    if not is_safe_run_id(rid):
        return fl.jsonify({"ok": False, "error": "Identificador de ejecución inválido."}), 400
    run_path = os.path.join(ARTIFACTS_DIR, rid)
    if not os.path.isdir(run_path):
        return fl.jsonify({"ok": False, "error": "No existe esa carpeta de artefactos."}), 404
    with _history_file_lock:
        history = _load_history_raw()
        if any(
            isinstance(r, dict) and str(r.get("run_id")) == rid for r in history
        ):
            return fl.jsonify(
                {"ok": False, "error": "La ejecución ya está en el historial."}
            ), 409
        ts = datetime.now().isoformat()
        nombre = (data.get("nombre") or "").strip() or _default_run_nombre(ts, rid)
        meta_best, wrmse = "Unknown", None
        mp = os.path.join(run_path, "meta.json")
        if os.path.isfile(mp):
            try:
                with open(mp, "r", encoding="utf-8") as mf:
                    meta = json.load(mf)
                meta_best = meta.get("best_model") or meta_best
            except (json.JSONDecodeError, OSError):
                pass
        errp = os.path.join(run_path, "errores_modelos.csv")
        if os.path.isfile(errp) and meta_best and meta_best != "Unknown":
            try:
                edf = pd.read_csv(errp, index_col=0)
                if meta_best in edf.index and "WRMSE" in edf.columns:
                    wrmse = float(edf.loc[meta_best, "WRMSE"])
            except Exception:
                pass
        history.append(
            {
                "run_id": rid,
                "nombre": nombre,
                "timestamp": ts,
                "best_model": meta_best,
                "wrmse": wrmse,
                "artifacts_dir": rid,
                "source_mode": (data.get("source_mode") or "auto").strip(),
                "source_file": (data.get("source_file") or "").strip(),
            }
        )
        history = history[-HISTORY_MAX:]
        _save_history_raw(history)
    active = (data.get("active") or fl.request.args.get("active") or "").strip()
    if active and not is_safe_run_id(active):
        active = ""
    return fl.jsonify({"ok": True, "runs": runs_api_payload(active)})


@app.route("/runs/<run_id>", methods=["PUT"])
def runs_update(run_id):
    if not is_safe_run_id(run_id):
        return fl.jsonify({"ok": False, "error": "Identificador inválido."}), 400
    data = fl.request.get_json(silent=True) or {}
    nombre = (data.get("nombre") or "").strip()
    if not nombre or len(nombre) > 160:
        return fl.jsonify(
            {"ok": False, "error": "El nombre no puede estar vacío ni superar 160 caracteres."}
        ), 400
    with _history_file_lock:
        history = _load_history_raw()
        found = False
        for row in history:
            if isinstance(row, dict) and str(row.get("run_id")) == run_id:
                row["nombre"] = nombre
                found = True
                break
        if not found:
            return fl.jsonify({"ok": False, "error": "Ejecución no encontrada en el historial."}), 404
        _save_history_raw(history)
    return fl.jsonify({"ok": True})


@app.route("/runs/<run_id>", methods=["DELETE"])
def runs_delete(run_id):
    if not is_safe_run_id(run_id):
        return fl.jsonify({"ok": False, "error": "Identificador inválido."}), 400
    paths_before = get_valid_history_run_paths_ordered()
    head_before = os.path.basename(paths_before[0]) if paths_before else None
    with _history_file_lock:
        history = _load_history_raw()
        n_before = len(history)
        history = [
            r
            for r in history
            if not (isinstance(r, dict) and str(r.get("run_id")) == run_id)
        ]
        if len(history) == n_before:
            return fl.jsonify({"ok": False, "error": "Ejecución no encontrada."}), 404
        _save_history_raw(history)
    run_dir = os.path.join(ARTIFACTS_DIR, run_id)
    if os.path.isdir(run_dir):
        try:
            shutil.rmtree(run_dir)
        except OSError as exc:
            print(f"[runs] rmtree failed: {exc}")
            return fl.jsonify(
                {
                    "ok": False,
                    "error": "Se quitó del historial pero no se pudo borrar la carpeta en disco.",
                }
            ), 500
    if head_before == run_id or not get_valid_history_run_paths_ordered():
        fl.session.pop(SESSION_ACTIVE_CSV_KEY, None)
    fl.session.pop("last_run_id", None)
    return fl.jsonify({"ok": True})


@app.route("/api/datasets", methods=["GET"])
def api_datasets_list():
    files = sorted(list_csv_files())
    active = (fl.session.get(SESSION_ACTIVE_CSV_KEY) or "").strip()
    items = []
    for f in files:
        fp = os.path.join(DEFAULT_SAVE_DIR, f)
        try:
            st = os.stat(fp)
            items.append(
                {
                    "name": f,
                    "size": st.st_size,
                    "mtime": int(st.st_mtime),
                    "active": f == active,
                }
            )
        except OSError:
            items.append(
                {"name": f, "size": None, "mtime": None, "active": f == active}
            )
    return fl.jsonify({"ok": True, "datasets": items})


@app.route("/api/run/execute", methods=["POST"])
def api_run_execute():
    """
    Ejecuta ML1 para un CSV en ``data/`` (modo ``single``), o fusiona todos los CSV
    válidos y ejecuta **una** vez sobre ``merged_dataset.csv`` si ``merge_all: true``.

    El historial de ejecuciones se registra **una sola vez** al finalizar la corrida
    (no dentro de ``_execute_ml1_for_csv_dataset``).
    """
    pipeline_session_id = uuid.uuid4().hex
    print(f"[run] pipeline_session_id={pipeline_session_id}")
    data = fl.request.get_json(silent=True) or {}
    if data.get("merge_all"):
        names = sorted(
            f
            for f in list_csv_files()
            if f.lower() != MERGED_DATASET_BASENAME.lower()
        )
        print("[run] datasets recibidos (merge_all):", names)
        if not names:
            return (
                fl.jsonify(
                    {
                        "ok": False,
                        "error": "No hay CSV en data/ para fusionar (solo consolidado o carpeta vacía).",
                    }
                ),
                400,
            )
        try:
            merged_name = merge_csv_datasets(names)
        except Exception as e:
            print(f"[merge] fallo la fusión: {e}")
            return fl.jsonify({"ok": False, "error": str(e)}), 500
        print("[run] usando dataset unificado:", merged_name)
        sources_join = ", ".join(names)
        if len(sources_join) > 700:
            sources_join = sources_join[:700] + "…"
        merged_label = f"{MERGED_DATASET_BASENAME} ({len(names)} CSV: {sources_join})"
        out = _execute_ml1_for_csv_dataset(
            merged_name,
            history_source_file=merged_label,
            pipeline_session_id=pipeline_session_id,
        )
        if out["ok"]:
            _register_history_from_execute_output(out)
            return fl.jsonify(
                {
                    "ok": True,
                    "run_id": out["run_id"],
                    "dataset": out["dataset"],
                    "merged_from": names,
                    "pipeline_session_id": pipeline_session_id,
                }
            )
        err = out.get("error") or "Error"
        if err == "Dataset no encontrado.":
            return fl.jsonify({"ok": False, "error": err}), 404
        return fl.jsonify({"ok": False, "error": err}), 500

    name = (data.get("dataset") or data.get("name") or "").strip()
    if not name:
        return (
            fl.jsonify({"ok": False, "error": "Falta el nombre del dataset (JSON: dataset)."}),
            400,
        )
    out = _execute_ml1_for_csv_dataset(
        name, pipeline_session_id=pipeline_session_id
    )
    if out["ok"]:
        _register_history_from_execute_output(out)
        return fl.jsonify(
            {
                "ok": True,
                "run_id": out["run_id"],
                "dataset": out["dataset"],
                "pipeline_session_id": pipeline_session_id,
            }
        )
    err = out.get("error") or "Error"
    if err == "Dataset no encontrado.":
        return fl.jsonify({"ok": False, "error": err}), 404
    return fl.jsonify({"ok": False, "error": err}), 500


@app.route("/api/datasets/preview", methods=["GET"])
def api_datasets_preview():
    name = (fl.request.args.get("name") or "").strip()
    fn = _csv_basename_on_disk(name)
    if not fn:
        return fl.jsonify({"ok": False, "error": "Dataset no encontrado."}), 404
    path = os.path.join(DEFAULT_SAVE_DIR, fn)
    try:
        df = pd.read_csv(path, nrows=_PREVIEW_ROWS_CAP)
        cols, rows = dataframe_to_preview_records(df, _PREVIEW_ROWS_CAP)
        return fl.jsonify({"ok": True, "name": fn, "columns": cols, "rows": rows})
    except Exception as exc:
        return fl.jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/datasets/active", methods=["POST"])
def api_datasets_set_active():
    data = fl.request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    fn = _csv_basename_on_disk(name)
    if not fn:
        return fl.jsonify({"ok": False, "error": "Dataset inválido."}), 400
    fl.session[SESSION_ACTIVE_CSV_KEY] = fn
    return fl.jsonify({"ok": True, "active": fn})


@app.route("/api/datasets/rename", methods=["POST"])
def api_datasets_rename():
    data = fl.request.get_json(silent=True) or {}
    old = (data.get("from") or "").strip()
    new_raw = (data.get("to") or "").strip()
    old_fn = _csv_basename_on_disk(old)
    if not old_fn:
        return fl.jsonify({"ok": False, "error": "Origen no encontrado."}), 404
    new_fn = secure_filename(new_raw)
    if not new_fn.lower().endswith(".csv"):
        new_fn = secure_filename((new_raw or "") + ".csv")
    if not new_fn or not new_fn.lower().endswith(".csv"):
        return fl.jsonify({"ok": False, "error": "Nombre destino inválido."}), 400
    if new_fn == old_fn:
        return fl.jsonify({"ok": True, "name": new_fn})
    names = set(list_csv_files())
    if new_fn in names:
        return fl.jsonify(
            {"ok": False, "error": "Ya existe un archivo con ese nombre."}
        ), 409
    op = os.path.join(DEFAULT_SAVE_DIR, old_fn)
    np = os.path.join(DEFAULT_SAVE_DIR, new_fn)
    try:
        os.rename(op, np)
    except OSError as exc:
        return fl.jsonify({"ok": False, "error": str(exc)}), 500
    sess = (fl.session.get(SESSION_ACTIVE_CSV_KEY) or "").strip()
    if sess == old_fn:
        fl.session[SESSION_ACTIVE_CSV_KEY] = new_fn
    return fl.jsonify({"ok": True, "name": new_fn})


@app.route("/api/datasets/delete", methods=["POST"])
def api_datasets_delete():
    data = fl.request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    fn = _csv_basename_on_disk(name)
    if not fn:
        return fl.jsonify({"ok": False, "error": "No encontrado."}), 404
    path = os.path.join(DEFAULT_SAVE_DIR, fn)
    try:
        os.remove(path)
    except OSError as exc:
        return fl.jsonify({"ok": False, "error": str(exc)}), 500
    sess = (fl.session.get(SESSION_ACTIVE_CSV_KEY) or "").strip()
    if sess == fn:
        fl.session.pop(SESSION_ACTIVE_CSV_KEY, None)
    return fl.jsonify({"ok": True})


@app.route("/api/datasets/upload", methods=["POST"])
def api_datasets_upload():
    """Siempre responde JSON (evita que el cliente falle al parsear HTML de error)."""
    try:
        os.makedirs(DEFAULT_SAVE_DIR, exist_ok=True)
        uploads = _upload_filestorage_list_from_request()
        print("[upload] FILES RECEIVED:", len(uploads), flush=True)
        if not uploads:
            return fl.jsonify({"ok": False, "error": "No se enviaron archivos."}), 400
        res = persist_multiple_csv_filestorage_list(uploads)
        if not res["ok"]:
            return fl.jsonify({"ok": False, "error": res["message"]}), 400
        return fl.jsonify(
            {
                "ok": True,
                "name": res["last_filename"],
                "names": res["filenames"],
                "count": len(res["filenames"]),
                "message": res["message"],
                "failed": [
                    {"file": a, "error": b} for a, b in res.get("failed", [])
                ],
            }
        )
    except Exception as exc:
        print("[upload] UPLOAD ERROR:", str(exc), flush=True)
        return fl.jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/preview/<filename>")
def preview_csv(filename):
    file_path = os.path.join(DEFAULT_SAVE_DIR, filename)
    if not os.path.exists(file_path) or not filename.endswith(".csv"):
        fl.abort(404)
    try:
        df = pd.read_csv(file_path, nrows=10)
        preview = df.to_html(index=False)
        pv_cols, pv_rows = dataframe_to_preview_records(df, _PREVIEW_ROWS_CAP)
        return fl.render_template(
            "preview.html",
            filename=filename,
            preview=preview,
            preview_columns=pv_cols,
            preview_records=pv_rows,
        )
    except Exception as e:
        return str(e), 500


@app.route("/download/<run_id>/<filename>")
def download_run_file(run_id, filename):
    if not is_safe_run_id(run_id):
        fl.abort(404)
    base = secure_filename(filename)
    if not base or base != filename:
        fl.abort(404)
    folder = os.path.join(ARTIFACTS_DIR, run_id)
    path = os.path.normpath(os.path.join(folder, base))
    root = os.path.normpath(ARTIFACTS_DIR)
    try:
        if os.path.commonpath([path, root]) != root:
            fl.abort(404)
    except ValueError:
        fl.abort(404)
    if not os.path.isfile(path):
        fl.abort(404)
    return fl.send_file(path, as_attachment=True)


@app.route("/download/<filename>")
def download_file(filename):
    lr = get_latest_run()
    if not lr:
        fl.abort(404)
    rid = os.path.basename(lr)
    return fl.redirect(
        fl.url_for("download_run_file", run_id=rid, filename=filename)
    )


@app.route("/artifacts/<path:filepath>")
def serve_artifacts(filepath):
    return fl.send_from_directory(ARTIFACTS_DIR, filepath)


@app.route("/charts/embed/<run_id>/<token>")
def charts_html_embed(run_id, token):
    """Sirve un gráfico HTML del run sin exponer el nombre de archivo en la URL."""
    if not is_safe_run_id(str(run_id)) or not token or len(str(token)) != 26:
        fl.abort(404)
    if not run_id_in_history(run_id):
        fl.abort(404)
    run_path = os.path.join(ARTIFACTS_DIR, run_id)
    if not os.path.isdir(run_path):
        fl.abort(404)
    want = str(token).lower()
    for fname in get_chart_paths(run_path):
        if _chart_html_embed_token(run_id, fname).lower() == want:
            full = os.path.join(run_path, fname)
            if not os.path.isfile(full):
                fl.abort(404)
            return fl.send_file(full, mimetype="text/html")
    fl.abort(404)


@app.route("/forecast")
def show_forecast():
    run_path = get_latest_run()
    if not run_path:
        return render_empty_runs_state(page_title="Pronóstico")
    run_id = os.path.basename(run_path) if run_path else None
    mode = get_analytical_mode()
    df_main = load_forecast_3m(run_path) if run_path else None
    df_int = load_forecast_3m_int(run_path) if run_path else None
    df_any = df_main if df_main is not None else df_int
    tip_ctx = tipos_from_run_cache(run_id, mode)
    act = get_contextual_actions(
        "forecast", df_any, tip_ctx, None, analytical_mode=mode
    )
    msg = guidance_message_for_module(
        "forecast", df_any, tip_ctx or {}, None, analytical_mode=mode
    )
    fc_ctx = build_forecast_insight_context(df_main, df_int, run_path, run_id)
    df_main_disp = _forecast_table_for_details(df_main)
    df_int_disp = _forecast_table_for_details(df_int)
    # Ruta canónica (evita desajustes run_path vs ARTIFACTS_DIR/run_id)
    run_dir_canon = (
        os.path.normpath(os.path.join(ARTIFACTS_DIR, str(run_id)))
        if run_id and is_safe_run_id(str(run_id))
        else None
    )
    if run_path and run_dir_canon and os.path.normpath(run_path) != run_dir_canon:
        print(
            "[forecast] warn: run_path != ARTIFACTS_DIR/run_id",
            os.path.normpath(run_path),
            run_dir_canon,
        )
    base_dir = run_dir_canon if run_dir_canon and os.path.isdir(run_dir_canon) else run_path
    forecast_60m_path = (
        os.path.normpath(os.path.join(base_dir, "forecast_60m.csv")) if base_dir else ""
    )
    forecast_60m_int_path = (
        os.path.normpath(os.path.join(base_dir, "forecast_60m_int.csv")) if base_dir else ""
    )
    has_forecast_60m_csv = bool(forecast_60m_path and os.path.isfile(forecast_60m_path))
    has_forecast_60m_int = bool(forecast_60m_int_path and os.path.isfile(forecast_60m_int_path))
    has_forecast_60m_file = has_forecast_60m_csv or has_forecast_60m_int
    print("[forecast] Forecast 60m path:", forecast_60m_path)
    print("[forecast] Forecast 60m exists:", has_forecast_60m_csv)
    print("[forecast] Forecast 60m_int path:", forecast_60m_int_path)
    print("[forecast] Forecast 60m_int exists:", has_forecast_60m_int)
    return fl.render_template(
        "forecast_view.html",
        run_id=run_id,
        analytical_mode=mode,
        forecast_table_html=dataframe_to_html_table(df_main_disp),
        forecast_int_table_html=dataframe_to_html_table(df_int_disp),
        has_forecast_file=bool(df_main is not None),
        has_forecast_int_file=bool(df_int is not None),
        has_forecast_60m_file=has_forecast_60m_file,
        has_forecast_60m_csv=has_forecast_60m_csv,
        has_forecast_60m_int=has_forecast_60m_int,
        actions=act,
        guidance_message=msg,
        **fc_ctx,
    )


def _metrics_view_kwargs():
    run_path = get_latest_run()
    if not run_path:
        return None
    run_id = os.path.basename(run_path) if run_path else None
    mode = get_analytical_mode()
    df_err = load_errores_modelos(run_path) if run_path else None
    act = get_contextual_actions(
        "metrics", df_err, None, None, analytical_mode=mode
    )
    msg = guidance_message_for_module(
        "metrics", df_err, None, None, analytical_mode=mode
    )
    mx_ctx = build_metrics_insight_context(df_err)
    return {
        "run_id": run_id,
        "analytical_mode": mode,
        "metrics_full_table_html": dataframe_to_html_table(df_err),
        "has_file": bool(df_err is not None),
        "actions": act,
        "guidance_message": msg,
        **mx_ctx,
    }


@app.route("/metrics")
def show_metrics():
    ctx = _metrics_view_kwargs()
    if ctx is None:
        return render_empty_runs_state(page_title="Evaluación de modelos")
    return fl.render_template("metrics_view.html", **ctx)


def _clustering_view_kwargs():
    run_path = get_latest_run()
    if not run_path:
        return None
    run_id = os.path.basename(run_path) if run_path else None
    mode = get_analytical_mode()
    df_cl = load_clustering_provincia(run_path) if run_path else None
    tip_ctx = tipos_from_run_cache(run_id, mode) or {}
    act = get_contextual_actions(
        "clustering", df_cl, tip_ctx, None, analytical_mode=mode
    )
    msg = guidance_message_for_module(
        "clustering", df_cl, tip_ctx, None, analytical_mode=mode
    )
    cl_ctx = build_clustering_insight_context(df_cl)
    clustering_full_table_html = None
    if df_cl is not None and cl_ctx.get("cl_has_data") and cl_ctx.get("cl_cluster_col"):
        dfd = df_cl.copy()
        cid_series = dfd[cl_ctx["cl_cluster_col"]].map(_cl_normalize_cluster_id)
        dfd["Nivel interpretado"] = cid_series.map(cl_ctx["cl_tier_map_int"])
        clustering_full_table_html = dataframe_to_html_table(dfd)
    elif df_cl is not None:
        clustering_full_table_html = dataframe_to_html_table(df_cl)
    return {
        "run_id": run_id,
        "analytical_mode": mode,
        "clustering_full_table_html": clustering_full_table_html,
        "has_file": bool(df_cl is not None),
        "actions": act,
        "guidance_message": msg,
        **cl_ctx,
    }


@app.route("/clustering")
def show_clustering():
    ctx = _clustering_view_kwargs()
    if ctx is None:
        return render_empty_runs_state(page_title="Agrupamientos (Clustering)")
    return fl.render_template("clustering_view.html", **ctx)


def _charts_view_kwargs(panel_scroll_to=""):
    run_path = get_latest_run()
    if not run_path:
        return None
    run_id = os.path.basename(run_path) if run_path else None
    mode = get_analytical_mode()
    chart_files = []
    if run_path:
        chart_files = get_chart_paths(run_path)
        chart_files.sort()
        print(
            "[artifacts] show_charts:",
            f"run={run_id}",
            f"html_count={len(chart_files)}",
            f"files={chart_files}",
        )
    act = get_contextual_actions(
        "charts",
        None,
        None,
        None,
        extra={"no_charts": len(chart_files) == 0},
        analytical_mode=mode,
    )
    msg = guidance_message_for_module(
        "charts",
        None,
        None,
        None,
        extra={"no_charts": len(chart_files) == 0},
        analytical_mode=mode,
    )
    ch_pc, ch_pr = [], []
    if run_path:
        cdf, _ = load_analysis_dataset(run_path, mode)
        ch_pc, ch_pr = dataframe_to_preview_records(cdf, _PREVIEW_ROWS_CAP)
    chart_gallery_full = build_charts_gallery_payload(chart_files, mode, run_id)
    has_relations_heatmap = _has_heatmap_in_relations(chart_gallery_full)
    chart_gallery = _filter_chart_gallery_for_panel(
        chart_gallery_full, panel_scroll_to or ""
    )
    scroll = (panel_scroll_to or "").strip()
    fc_hub = (
        (_hub_forecast_context(run_path, run_id) if run_path else {})
        if scroll == "charts-sec-forecast"
        else {}
    )
    return {
        "run_id": run_id,
        "analytical_mode": mode,
        "chart_gallery": chart_gallery,
        "has_relations_heatmap": has_relations_heatmap,
        "charts_preview_columns": ch_pc,
        "charts_preview_records": ch_pr,
        "actions": act,
        "guidance_message": msg,
        "panel_scroll_to": panel_scroll_to or "",
        **fc_hub,
    }


@app.route("/charts")
def show_charts():
    ctx = _charts_view_kwargs("")
    if ctx is None:
        return render_empty_runs_state(page_title="Visualizaciones")
    return fl.render_template("charts_view.html", **ctx)


def _types_column_type_label(col, tipos):
    """Etiqueta legible de clasificación para una columna (vista Tipos)."""
    c = str(col)
    pairs = (
        ("numericas", "numérica"),
        ("categoricas", "categórica"),
        ("temporales", "temporal"),
        ("booleanas", "booleana"),
        ("ids", "id (alta cardinalidad)"),
    )
    for key, lab in pairs:
        if c in (tipos.get(key) or []):
            return lab
    return "—"


def _build_types_null_digest(df, tipos):
    """Por columna: nombre, tipo inferido, porcentaje de nulos (solo pandas NA)."""
    if df is None or getattr(df, "empty", True) or not tipos:
        return []
    n = len(df)
    rows = []
    for col in df.columns:
        nn = int(df[col].isna().sum())
        pct = round(100.0 * nn / n, 1) if n else 0.0
        rows.append(
            {
                "column": str(col),
                "type_label": _types_column_type_label(col, tipos),
                "null_pct": pct,
            }
        )
    return rows


def _types_view_kwargs():
    run_path = get_latest_run()
    if not run_path:
        return None
    run_id = os.path.basename(run_path) if run_path else None
    mode = get_analytical_mode()
    ck = analysis_cache_key(run_id, mode) if run_id else None
    if ck and ck in _types_cache:
        cached = _types_cache[ck]
        if "tipos" in cached and "summary" in cached:
            print(f"[artifacts] show_types: page cache HIT key={ck}")
            _nds = not bool(cached.get("data_source"))
            _act = get_contextual_actions(
                "types",
                None,
                cached["tipos"],
                None,
                extra={"no_data_source": _nds},
                analytical_mode=mode,
            )
            _msg = guidance_message_for_module(
                "types",
                None,
                cached["tipos"],
                None,
                extra={"no_data_source": _nds},
                analytical_mode=mode,
            )
            pv_cols = cached.get("preview_columns")
            pv_recs = cached.get("preview_records")
            if (pv_recs is None or pv_cols is None) and run_path:
                df_pv, _ = load_analysis_dataset(run_path, mode)
                pv_cols, pv_recs = dataframe_to_preview_records(
                    df_pv, _PREVIEW_ROWS_CAP
                )
                cached["preview_columns"] = pv_cols
                cached["preview_records"] = pv_recs
                _types_cache[ck] = cached
            digest = cached.get("types_null_digest")
            if digest is None and run_path:
                df_d, _ = load_analysis_dataset(run_path, mode)
                if df_d is not None:
                    digest = _build_types_null_digest(df_d, cached["tipos"])
                    cached["types_null_digest"] = digest
                    _types_cache[ck] = cached
                else:
                    digest = []
            if digest is None:
                digest = []
            return {
                "tipos": cached["tipos"],
                "summary": cached["summary"],
                "run_id": run_id,
                "data_source": cached.get("data_source") or "",
                "analytical_mode": mode,
                "types_preview_columns": pv_cols or [],
                "types_preview_records": pv_recs or [],
                "types_null_digest": digest,
                "actions": _act,
                "guidance_message": _msg,
            }

    df, source_name = (
        load_analysis_dataset(run_path, mode) if run_path else (None, None)
    )
    tipos = get_types_cached(run_id, df, mode)
    total_columnas = len(df.columns) if df is not None else 0
    summary = {
        "total_columnas": total_columnas,
        "num_numericas": len(tipos["numericas"]),
        "num_categoricas": len(tipos["categoricas"]),
        "num_temporales": len(tipos["temporales"]),
        "num_booleanas": len(tipos["booleanas"]),
        "num_ids": len(tipos["ids"]),
    }
    tpv_cols, tpv_recs = dataframe_to_preview_records(df, _PREVIEW_ROWS_CAP)
    null_digest = _build_types_null_digest(df, tipos)
    if df is not None and ck and source_name:
        _types_cache[ck] = {
            "tipos": tipos,
            "summary": summary,
            "data_source": source_name,
            "preview_columns": tpv_cols,
            "preview_records": tpv_recs,
            "types_null_digest": null_digest,
        }
        print(f"[artifacts] show_types: stored types cache key={ck}")
    _nds = not bool(source_name)
    _act = get_contextual_actions(
        "types",
        df,
        tipos,
        None,
        extra={"no_data_source": _nds},
        analytical_mode=mode,
    )
    _msg = guidance_message_for_module(
        "types",
        df,
        tipos,
        None,
        extra={"no_data_source": _nds},
        analytical_mode=mode,
    )
    return {
        "tipos": tipos,
        "summary": summary,
        "run_id": run_id,
        "data_source": source_name,
        "analytical_mode": mode,
        "types_preview_columns": tpv_cols,
        "types_preview_records": tpv_recs,
        "types_null_digest": null_digest,
        "actions": _act,
        "guidance_message": _msg,
    }


@app.route("/types")
def show_types():
    ctx = _types_view_kwargs()
    if ctx is None:
        return render_empty_runs_state(page_title="Tipos de variables")
    return fl.render_template("types_view.html", **ctx)


def _relations_view_kwargs():
    run_path = get_latest_run()
    if not run_path:
        return None
    run_id = os.path.basename(run_path) if run_path else None
    mode = get_analytical_mode()
    ck = analysis_cache_key(run_id, mode) if run_id else None

    if ck and ck in _relations_cache:
        cached = _relations_cache[ck]
        if cached.get("payload"):
            print(f"[relations] show_relations: page cache HIT key={ck}")
            pl = dict(cached["payload"])
            _relations_payload_hit_enrich(run_path, mode, pl)
            _tip = tipos_from_run_cache(run_id, mode) or {}
            pl["actions"] = get_contextual_actions(
                "relations", None, _tip, pl, analytical_mode=mode
            )
            pl["guidance_message"] = guidance_message_for_module(
                "relations", None, _tip, pl, analytical_mode=mode
            )
            pl["analytical_mode"] = mode
            return pl

    df, source_name = (
        load_analysis_dataset(run_path, mode) if run_path else (None, None)
    )
    if df is None or not source_name:
        payload = _empty_relations_payload(run_id)
        payload["data_source"] = source_name or ""
        payload["actions"] = get_contextual_actions(
            "relations", None, None, payload, analytical_mode=mode
        )
        payload["guidance_message"] = guidance_message_for_module(
            "relations", None, None, payload, analytical_mode=mode
        )
        payload["analytical_mode"] = mode
        return payload

    payload = _core_relations_payload_from_df(run_id, df, source_name, mode)

    if ck:
        _relations_cache[ck] = {"payload": dict(payload)}
        print(f"[relations] show_relations: stored relations cache key={ck}")

    _tip = tipos_from_run_cache(run_id, mode)
    if _tip is None and df is not None:
        _tip = detect_variable_types(df)
    payload["actions"] = get_contextual_actions(
        "relations", df, _tip or {}, payload, analytical_mode=mode
    )
    payload["guidance_message"] = guidance_message_for_module(
        "relations", df, _tip or {}, payload, analytical_mode=mode
    )
    payload["analytical_mode"] = mode
    return payload


@app.route("/relations")
def show_relations():
    ctx = _relations_view_kwargs()
    if ctx is None:
        return render_empty_runs_state(page_title="Relaciones entre variables")
    return fl.render_template("relations_view.html", **ctx)


def _dashboard_view_kwargs():
    """Contexto para dashboard.html; None si no hay run."""
    print("[dashboard] assembling unified view")
    run_path = get_latest_run()
    if not run_path:
        return None
    run_id = os.path.basename(run_path) if run_path else None
    mode = get_analytical_mode()
    ck_t = analysis_cache_key(run_id, mode) if run_id else None
    ck_r = analysis_cache_key(run_id, mode) if run_id else None

    df = None
    source_name = ""
    tipos = None
    summary = None
    rel_payload = None

    if ck_t:
        t_entry = _types_cache.get(ck_t)
        if t_entry and "tipos" in t_entry and "summary" in t_entry:
            tipos = t_entry["tipos"]
            summary = t_entry["summary"]
            source_name = t_entry.get("data_source") or ""
    if ck_r:
        r_entry = _relations_cache.get(ck_r)
        if r_entry and r_entry.get("payload"):
            rel_payload = r_entry["payload"]

    need_data = tipos is None or summary is None or rel_payload is None
    if not need_data and ck_t:
        print(
            f"[dashboard] cache warm key={ck_t}: "
            "skip dataset load (tipos + relaciones)"
        )
    if need_data and run_path:
        df, src = load_analysis_dataset(run_path, mode)
        if src:
            source_name = source_name or src
        if tipos is None and df is not None:
            tipos = get_types_cached(run_id, df, mode)
            summary = _summary_from_tipos_df(df, tipos)
            if ck_t and source_name:
                d_pv_cols, d_pv_recs = dataframe_to_preview_records(
                    df, _PREVIEW_ROWS_CAP
                )
                _types_cache[ck_t] = {
                    "tipos": tipos,
                    "summary": summary,
                    "data_source": source_name,
                    "preview_columns": d_pv_cols,
                    "preview_records": d_pv_recs,
                }
                print(f"[dashboard] stored _types_cache key={ck_t}")
        if rel_payload is None and df is not None:
            rel_payload = _core_relations_payload_from_df(
                run_id, df, source_name or "", mode
            )
            if ck_r:
                _relations_cache[ck_r] = {"payload": dict(rel_payload)}
                print(f"[dashboard] stored _relations_cache key={ck_r}")

    if tipos is None:
        tipos = detect_variable_types(df)
    if summary is None:
        summary = _summary_from_tipos_df(df, tipos)
    if rel_payload is None:
        rel_payload = _empty_relations_payload(run_id)

    report = generate_final_report(df, tipos, rel_payload)
    chart_files = []
    if run_path:
        chart_files = get_chart_paths(run_path)
        chart_files.sort()
    forecast_summary = _load_forecast_summary(run_path)
    meta_data = {}
    if run_path:
        meta_path = os.path.join(run_path, "meta.json")
        if os.path.isfile(meta_path):
            try:
                with open(meta_path, "r", encoding="utf-8") as mf:
                    meta_data = json.load(mf)
            except (json.JSONDecodeError, OSError):
                meta_data = {}
    dash_story = build_dashboard_story_context(
        df,
        run_id,
        run_path,
        summary,
        rel_payload,
        chart_files,
        meta_data,
        analytical_mode=mode,
    )

    _no_ds = not (source_name or "").strip() or (
        summary.get("total_columnas", 0) == 0
    )
    _act = get_contextual_actions(
        "dashboard",
        df,
        tipos,
        rel_payload,
        extra={"no_dataset": bool(_no_ds)},
        analytical_mode=mode,
    )
    _msg = guidance_message_for_module(
        "dashboard",
        df,
        tipos,
        rel_payload,
        extra={"no_dataset": bool(_no_ds)},
        analytical_mode=mode,
    )
    dash_pc, dash_pr = [], []
    _dash_df = df
    if _dash_df is None and run_path and summary.get("total_columnas", 0):
        _dash_df, _ = load_analysis_dataset(run_path, mode)
    if _dash_df is not None and summary.get("total_columnas", 0):
        dash_pc, dash_pr = dataframe_to_preview_records(_dash_df, 6)
    fc_hub = _hub_forecast_context(run_path, run_id)
    return {
        "run_id": run_id,
        "analytical_mode": mode,
        "data_source": source_name,
        "summary": summary,
        "report": report,
        "rel": rel_payload,
        "chart_files": chart_files,
        "forecast_summary": forecast_summary,
        "dash_story": dash_story,
        "meta_data": meta_data,
        "dashboard_preview_columns": dash_pc,
        "dashboard_preview_records": dash_pr,
        "actions": _act,
        "guidance_message": _msg,
        **fc_hub,
    }


@app.route("/dashboard")
def dashboard():
    """Reporte final: narrativa en una pantalla (resumen, hallazgos, 1–2 gráficos, modelo, pronóstico); detalle técnico colapsable."""
    ctx = _dashboard_view_kwargs()
    if ctx is None:
        return render_empty_runs_state(page_title="Reporte final")
    return fl.render_template("dashboard.html", **ctx)


@app.route("/partial/<path:slug>")
def partial_module(slug):
    """HTML fragment para panel lateral (hubs de análisis / visualizaciones)."""
    slug = (slug or "").strip().lower()
    scroll_map = {
        "charts-main": "charts-sec-main",
        "charts-time": "charts-sec-time",
        "charts-relations": "charts-sec-relations",
        "charts-forecast": "charts-sec-forecast",
    }
    if slug == "types":
        ctx = _types_view_kwargs()
        if ctx is None:
            return fl.render_template("partials/no_run.html", partial_title="Tipos de variables")
        return fl.render_template("partials/types_panel.html", **{**ctx, "hub_panel": True})
    if slug == "eda":
        run_path, rid = resolve_latest_results_run()
        ctx = build_eda_exploratory_context(run_path, rid)
        if ctx is None:
            return fl.render_template(
                "partials/no_run.html", partial_title="Análisis exploratorio"
            )
        return fl.render_template("partials/eda_panel.html", **{**ctx, "hub_panel": True})
    if slug == "metrics":
        ctx = _metrics_view_kwargs()
        if ctx is None:
            return fl.render_template(
                "partials/no_run.html", partial_title="Evaluación de modelos"
            )
        return fl.render_template("partials/metrics_panel.html", **{**ctx, "hub_panel": True})
    if slug == "clustering":
        ctx = _clustering_view_kwargs()
        if ctx is None:
            return fl.render_template(
                "partials/no_run.html", partial_title="Agrupamientos (Clustering)"
            )
        return fl.render_template("partials/clustering_panel.html", **{**ctx, "hub_panel": True})
    if slug == "relations":
        ctx = _relations_view_kwargs()
        if ctx is None:
            return fl.render_template(
                "partials/no_run.html", partial_title="Relaciones entre variables"
            )
        return fl.render_template("partials/relations_panel.html", **{**ctx, "hub_panel": True})
    if slug == "dashboard":
        ctx = _dashboard_view_kwargs()
        if ctx is None:
            return fl.render_template("partials/no_run.html", partial_title="Reporte final")
        return fl.render_template("partials/dashboard_panel.html", **{**ctx, "hub_panel": True})
    if slug in scroll_map or slug in ("charts", "charts-all"):
        anchor = scroll_map.get(slug, "")
        ctx = _charts_view_kwargs(anchor)
        if ctx is None:
            return fl.render_template(
                "partials/no_run.html", partial_title="Visualizaciones"
            )
        return fl.render_template("partials/charts_panel.html", **{**ctx, "hub_panel": True})
    fl.abort(404)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
