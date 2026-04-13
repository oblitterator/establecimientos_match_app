"""
georef_normalizer.py — Normalización de domicilios vía API Georef (datos.gob.ar)

Entrada por fila:
  - provincia (nombre)
  - departamento (nombre, opcional)
  - domicilio combinado  O  calle + número
  - localidad (opcional, ayuda al geocodificador)

Salida por fila (columnas nuevas agregadas):
  - provincia_normalizada, id_provincia_indec (2 dígitos)
  - departamento_normalizado, id_departamento_indec (5 dígitos INDEC)
  - domicilio_normalizado, latitud, longitud
  - provincia_error, departamento_error, domicilio_error

API: https://apis.datos.gob.ar/georef/api
"""

import os
import re
import time
import threading
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional

import requests

# ─────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────

BASE_URL = "https://apis.datos.gob.ar/georef/api"
TIMEOUT  = 25
MAX_RETRIES  = 4
BACKOFF_BASE = 2.0

GEOREF_API_KEY    = os.getenv("GEOREF_API_KEY",    "UTiFpVpdbzQTozOJmigfhvSvySOWhiXZ")
GEOREF_API_SECRET = os.getenv("GEOREF_API_SECRET", "SsfYSGLWeGcd2asYhRIpQfFDk5U1ExFZ")

# ─────────────────────────────────────────────────────────────────────
# ALIASES DE PROVINCIAS
# ─────────────────────────────────────────────────────────────────────

ALIAS_PROVINCIAS: Dict[str, str] = {
    "CABA":                                "CIUDAD AUTONOMA DE BUENOS AIRES",
    "CAPITAL FEDERAL":                     "CIUDAD AUTONOMA DE BUENOS AIRES",
    "CIUDAD AUTONOMA DE BS AS":            "CIUDAD AUTONOMA DE BUENOS AIRES",
    "CIUDAD AUTONOMA BS AS":               "CIUDAD AUTONOMA DE BUENOS AIRES",
    "CIUDAD DE BUENOS AIRES":              "CIUDAD AUTONOMA DE BUENOS AIRES",
    "CIUDAD AUTONOMA":                     "CIUDAD AUTONOMA DE BUENOS AIRES",
    "BS AS":                               "BUENOS AIRES",
    "PCIA DE BUENOS AIRES":               "BUENOS AIRES",
    "PCIA BUENOS AIRES":                  "BUENOS AIRES",
    "ENTRE RIOS":                          "ENTRE RIOS",
    "TIERRA DEL FUEGO ANTARTIDA E ISLAS DEL ATLANTICO SUR": "TIERRA DEL FUEGO",
    "TIERRA DEL FUEGO ANTARTIDA":         "TIERRA DEL FUEGO",
}

# ─────────────────────────────────────────────────────────────────────
# UTILIDADES DE TEXTO
# ─────────────────────────────────────────────────────────────────────

def _quitar_acentos(texto: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", str(texto))
        if not unicodedata.combining(c)
    )


def _limpiar_texto(texto: Optional[str]) -> str:
    if texto is None:
        return ""
    texto = str(texto).strip().upper()
    texto = _quitar_acentos(texto)
    texto = re.sub(r"[^\w\s/.-]", " ", texto)
    return re.sub(r"\s+", " ", texto).strip()


def _limpiar_domicilio(domicilio: Optional[str]) -> str:
    """Limpieza suave: no destruir demasiado para que Georef interprete bien."""
    if domicilio is None:
        return ""
    d = _quitar_acentos(str(domicilio).strip())
    return re.sub(r"\s+", " ", d).strip()


def _es_vacio(x: Any) -> bool:
    if x is None:
        return True
    s = str(x).strip()
    return s in ("", "nan", "None", "NaN", "none")


def _zfill(valor: Any, largo: int) -> Optional[str]:
    if _es_vacio(valor):
        return None
    try:
        return str(int(float(str(valor)))).zfill(largo)
    except Exception:
        v = str(valor).strip()
        return v.zfill(largo) if v else None


def _simplificar_domicilio(domicilio: Optional[str]) -> str:
    """
    Elimina sufijos internos (piso, depto, torre, etc.) que rompen el geocodificador.
    """
    if domicilio is None:
        return ""
    d = _limpiar_domicilio(domicilio).upper()
    d = re.sub(r"[:;,_]", " ", d)

    reemplazos = {
        "AVENIDA ": "AV ", "AV. ": "AV ",
        "N° ": "", "N.° ": "", "NUMERO ": "", "NRO. ": "", "NRO ": "",
        "S/N": "SN",
    }
    for k, v in reemplazos.items():
        d = d.replace(k, v)

    d = re.sub(r'["""\'´`]', " ", d)
    d = re.sub(r"\s+", " ", d).strip()

    patrones_cola = [
        r"\bDEPTO\b[:.]?\s*[A-Z0-9\-\/]*$",
        r"\bDPTO\b[:.]?\s*[A-Z0-9\-\/]*$",
        r"\bDTO\b[:.]?\s*[A-Z0-9\-\/]*$",
        r"\bPISO\b[:.]?\s*[A-Z0-9\-\/]*$",
        r"\bPI\s*SO\b[:.]?\s*[A-Z0-9\-\/]*$",
        r"\bOFICINA\b[:.]?\s*[A-Z0-9\-\/]*$",
        r"\bOF\b\.?\s*[A-Z0-9\-\/]*$",
        r"\bTORRE\b[:.]?\s*[A-Z0-9\-\/ ]*$",
        r"\bBLOQUE\b[:.]?\s*[A-Z0-9\-\/ ]*$",
        r"\bMONOBLOCK\b[:.]?\s*[A-Z0-9\-\/ ]*$",
        r"\bCASA\b[:.]?\s*[A-Z0-9\-\/ ]*$",
        r"\bMZA\b[:.]?\s*[A-Z0-9\-\/ ]*$",
        r"\bMANZANA\b[:.]?\s*[A-Z0-9\-\/ ]*$",
        r"\bLOTE\b[:.]?\s*[A-Z0-9\-\/ ]*$",
        r"\bUF\b[:.]?\s*[A-Z0-9\-\/ ]*$",
        r"\bP(B|ISO)?\b\.?\s*[A-Z0-9\-\/]*$",
    ]

    previo = None
    while previo != d:
        previo = d
        for patron in patrones_cola:
            d = re.sub(patron, "", d).strip()
        d = re.sub(r"\b0+(\d+)\b", r"\1", d)
        d = re.sub(r"\s+", " ", d).strip()

    return d


def _variantes_domicilio(domicilio: Optional[str]) -> List[str]:
    """
    Genera variantes del domicilio para intentar contra Georef.
    Más corto = más probable de ser encontrado cuando hay complementos.
    """
    candidatos: List[str] = []

    def agregar(v: Optional[str]):
        if not v:
            return
        v = re.sub(r"\s+", " ", str(v).strip())
        if v and v not in candidatos:
            candidatos.append(v)

    agregar(_limpiar_domicilio(domicilio))
    agregar(_simplificar_domicilio(domicilio))

    # Calle numérica + altura + complemento: "51 1120 4" → "51 1120"
    d = _quitar_acentos(str(domicilio or "")).upper()
    d = re.sub(r"[,:;]", " ", d)
    m = re.match(r"^(?P<calle>\d{1,3}(?:\s*BIS)?)\s+(?P<altura>\d{2,5})(?:\s+.+)$", d)
    if m:
        agregar(f"{m.group('calle').strip()} {m.group('altura')}")

    return [c for c in candidatos if c]


# ─────────────────────────────────────────────────────────────────────
# HTTP  (con retry y session por thread)
# ─────────────────────────────────────────────────────────────────────

_thread_local = threading.local()


def _make_headers() -> Dict[str, str]:
    h = {"User-Agent": "match-app-georef/1.0"}
    if GEOREF_API_KEY and GEOREF_API_SECRET:
        h["Authorization"] = f"Bearer {GEOREF_API_KEY}:{GEOREF_API_SECRET}"
    elif GEOREF_API_KEY:
        h["Authorization"] = f"Bearer {GEOREF_API_KEY}"
    return h


def _get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update(_make_headers())
        _thread_local.session = s
    return _thread_local.session


def _api_get(endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{BASE_URL}/{endpoint.lstrip('/')}"
    last_err = None
    for intento in range(MAX_RETRIES + 1):
        try:
            r = _get_session().get(url, params=params, timeout=TIMEOUT)
            if r.status_code in {429, 500, 502, 503, 504}:
                last_err = requests.HTTPError(f"HTTP {r.status_code}", response=r)
                if intento < MAX_RETRIES:
                    espera = min(BACKOFF_BASE * (2 ** intento), 60.0)
                    time.sleep(espera)
                    continue
            r.raise_for_status()
            return r.json()
        except (requests.ConnectionError, requests.Timeout) as e:
            last_err = e
            if intento < MAX_RETRIES:
                espera = min(BACKOFF_BASE * (2 ** intento), 60.0)
                time.sleep(espera)
                continue
            raise
    raise last_err


# ─────────────────────────────────────────────────────────────────────
# CACHE  (compartido entre threads, protegido con lock)
# ─────────────────────────────────────────────────────────────────────

_cache_prov: Dict[str, Dict] = {}
_cache_dept: Dict[str, Dict] = {}
_cache_dir:  Dict[str, Dict] = {}
_cache_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────────────
# NORMALIZADORES
# ─────────────────────────────────────────────────────────────────────

def normalizar_provincia(provincia_raw: Optional[str]) -> Dict[str, Any]:
    if _es_vacio(provincia_raw):
        return {"provincia_normalizada": None, "id_provincia_indec": None, "provincia_error": None}

    key = _limpiar_texto(provincia_raw)
    with _cache_lock:
        if key in _cache_prov:
            return _cache_prov[key]

    p = ALIAS_PROVINCIAS.get(key, key)

    try:
        data = _api_get("provincias", {"nombre": p, "max": 1, "campos": "id,nombre"})
        res  = data.get("provincias", [])
        if res:
            out = {
                "provincia_normalizada": _limpiar_texto(res[0]["nombre"]),
                "id_provincia_indec":    _zfill(res[0]["id"], 2),
                "provincia_error":       None,
            }
        else:
            out = {"provincia_normalizada": None, "id_provincia_indec": None,
                   "provincia_error": "sin match en API"}
    except Exception as e:
        out = {"provincia_normalizada": None, "id_provincia_indec": None,
               "provincia_error": f"{type(e).__name__}: {e}"}

    with _cache_lock:
        _cache_prov[key] = out
    return out


def normalizar_departamento(dept_raw: Optional[str],
                             id_provincia_indec: Optional[str]) -> Dict[str, Any]:
    if _es_vacio(dept_raw):
        return {"departamento_normalizado": None, "id_departamento_indec": None,
                "departamento_error": None}

    key = (_limpiar_texto(dept_raw), str(id_provincia_indec or ""))
    with _cache_lock:
        if key in _cache_dept:
            return _cache_dept[key]

    d      = _limpiar_texto(dept_raw)
    params = {"nombre": d, "max": 1, "campos": "id,nombre"}
    if id_provincia_indec:
        params["provincia"] = id_provincia_indec

    out = None
    try:
        data = _api_get("departamentos", params)
        res  = data.get("departamentos", [])
        if res:
            out = {
                "departamento_normalizado": _limpiar_texto(res[0]["nombre"]),
                "id_departamento_indec":    str(res[0]["id"]),
                "departamento_error":       None,
            }
    except Exception as e:
        out = {"departamento_normalizado": None, "id_departamento_indec": None,
               "departamento_error": f"{type(e).__name__}: {e}"}

    # Fallback: buscar como localidad y tomar el departamento al que pertenece.
    # Resuelve casos como "San Miguel de Tucumán" → departamento "Capital",
    # o cualquier dataset que cargue el nombre de la ciudad en lugar del dpto INDEC.
    if out is None or not out.get("id_departamento_indec"):
        try:
            loc_params = {"nombre": d, "max": 1}
            if id_provincia_indec:
                loc_params["provincia"] = id_provincia_indec
            data2 = _api_get("localidades", loc_params)
            locs  = data2.get("localidades", [])
            if locs:
                dept_info = locs[0].get("departamento") or {}
                if dept_info.get("id"):
                    out = {
                        "departamento_normalizado": _limpiar_texto(dept_info.get("nombre", "")),
                        "id_departamento_indec":    str(dept_info["id"]),
                        "departamento_error":       None,
                    }
        except Exception:
            pass  # si el fallback falla, queda el resultado del intento anterior

    if out is None:
        out = {"departamento_normalizado": None, "id_departamento_indec": None,
               "departamento_error": "sin match en API"}

    with _cache_lock:
        _cache_dept[key] = out
    return out


def normalizar_direccion(domicilio:         Optional[str],
                          id_provincia_indec: Optional[str]) -> Dict[str, Any]:
    empty = {"domicilio_normalizado": None, "latitud": None, "longitud": None,
             "domicilio_error": None}
    if _es_vacio(domicilio):
        return empty

    for variante in _variantes_domicilio(domicilio):
        key = (variante, str(id_provincia_indec or ""))
        with _cache_lock:
            if key in _cache_dir:
                cached = _cache_dir[key]
                if cached.get("latitud") is not None:
                    return cached
                # si estaba cacheado como error, igual intentar la próxima variante
                continue

        params: Dict[str, Any] = {
            "direccion": variante,
            "max":       1,
            # Sin 'campos': el response por defecto incluye ubicacion, departamento,
            # provincia, nomenclatura con la estructura correcta (dot-notation fields).
            # Pasarlo manualmente causaba 400 porque la API espera "ubicacion.lat"
            # no "ubicacion" — más simple usar el default.
        }
        if id_provincia_indec:
            params["provincia"] = id_provincia_indec

        try:
            data = _api_get("direcciones", params)
            res  = data.get("direcciones", [])
            if res:
                r        = res[0]
                ubicacion = r.get("ubicacion") or {}
                dept_info = r.get("departamento") or {}
                prov_info = r.get("provincia") or {}
                out = {
                    "domicilio_normalizado":       r.get("nomenclatura") or r.get("nombre"),
                    "latitud":                     ubicacion.get("lat"),
                    "longitud":                    ubicacion.get("lon"),
                    # internos: llenan prov/dept si los lookups directos fallaron
                    "_dir_id_provincia_indec":     _zfill(prov_info.get("id"), 2),
                    "_dir_id_departamento_indec":  str(dept_info.get("id") or ""),
                    "_dir_departamento_normalizado": _limpiar_texto(dept_info.get("nombre") or ""),
                    "domicilio_error":             None,
                }
                with _cache_lock:
                    _cache_dir[key] = out
                return out
            else:
                miss = {"domicilio_normalizado": None, "latitud": None, "longitud": None,
                        "domicilio_error": "sin match en direcciones"}
                with _cache_lock:
                    _cache_dir[key] = miss
        except Exception as e:
            return {"domicilio_normalizado": None, "latitud": None, "longitud": None,
                    "domicilio_error": f"{type(e).__name__}: {e}"}

    return {"domicilio_normalizado": None, "latitud": None, "longitud": None,
            "domicilio_error": "sin match en todas las variantes"}


# ─────────────────────────────────────────────────────────────────────
# NORMALIZACIÓN POR FILA
# ─────────────────────────────────────────────────────────────────────

def normalizar_fila(row:            Dict,
                    col_provincia:  Optional[str],
                    col_departamento: Optional[str],
                    col_domicilio:  Optional[str],
                    col_calle:      Optional[str],
                    col_numero:     Optional[str],
                    col_localidad:  Optional[str],
                    addr_type:      str = "combined") -> Dict[str, Any]:
    """
    Normaliza una fila y devuelve un dict con las columnas nuevas.
    """
    prov_raw      = row.get(col_provincia, "")   if col_provincia  else ""
    dept_raw      = row.get(col_departamento, "") if col_departamento else ""
    localidad_raw = row.get(col_localidad, "")   if col_localidad  else ""

    if addr_type == "split":
        calle  = str(row.get(col_calle,  "") or "").strip() if col_calle  else ""
        numero = str(row.get(col_numero, "") or "").strip() if col_numero else ""
        # Evitar duplicar el número cuando ya viene al final del campo calle
        # Ej: calle="Colon 59" + numero="59" → "Colon 59", no "Colon 59 59"
        if numero and calle.rstrip().endswith(numero):
            dom_raw = calle.strip()
        else:
            dom_raw = f"{calle} {numero}".strip()
    else:
        dom_raw = str(row.get(col_domicilio, "") or "").strip() if col_domicilio else ""

    # 1. Provincia
    prov_out = normalizar_provincia(prov_raw)
    id_prov  = prov_out.get("id_provincia_indec")

    # 2. Departamento
    dept_out = normalizar_departamento(dept_raw, id_prov)

    # 3. Dirección (localidad no es param de filtro válido en Georef /direcciones)
    dir_out  = normalizar_direccion(dom_raw, id_prov)

    # Si la dirección resolvió prov/dept y los lookups directos fallaron, usarlos
    if not prov_out.get("id_provincia_indec") and dir_out.get("_dir_id_provincia_indec"):
        prov_out["id_provincia_indec"] = dir_out["_dir_id_provincia_indec"]
    if not dept_out.get("id_departamento_indec") and dir_out.get("_dir_id_departamento_indec"):
        dept_out["id_departamento_indec"] = dir_out["_dir_id_departamento_indec"]
        if not dept_out.get("departamento_normalizado") and dir_out.get("_dir_departamento_normalizado"):
            dept_out["departamento_normalizado"] = dir_out["_dir_departamento_normalizado"]
        dept_out["departamento_error"] = None  # si vino del dpto, no es error

    # Limpiar claves internas antes de devolver
    dir_out.pop("_dir_id_provincia_indec", None)
    dir_out.pop("_dir_id_departamento_indec", None)
    dir_out.pop("_dir_departamento_normalizado", None)

    return {**prov_out, **dept_out, **dir_out}


# ─────────────────────────────────────────────────────────────────────
# PROCESAMIENTO MASIVO
# ─────────────────────────────────────────────────────────────────────

def procesar_dataframe(df,
                       col_provincia:    Optional[str],
                       col_departamento: Optional[str],
                       col_domicilio:    Optional[str],
                       col_calle:        Optional[str],
                       col_numero:       Optional[str],
                       col_localidad:    Optional[str],
                       addr_type:        str = "combined",
                       max_workers:      int = 5,
                       progress_cb:      Optional[Callable] = None) -> List[Dict]:
    """
    Procesa cada fila del DataFrame en paralelo.
    Devuelve lista de dicts (fila original + columnas georef nuevas).
    progress_cb(n_done, n_total) se llama después de cada fila.
    """
    rows    = df.to_dict("records")
    n_total = len(rows)
    results = [None] * n_total

    def process_one(idx: int, row: Dict):
        out = normalizar_fila(
            row, col_provincia, col_departamento,
            col_domicilio, col_calle, col_numero,
            col_localidad, addr_type,
        )
        return idx, {**row, **out}

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(process_one, i, row): i for i, row in enumerate(rows)}
        for f in as_completed(futures):
            try:
                idx, result = f.result()
                results[idx] = result
            except Exception as e:
                idx = futures[f]
                results[idx] = {**rows[idx],
                                "provincia_normalizada": None, "id_provincia_indec": None,
                                "departamento_normalizado": None, "id_departamento_indec": None,
                                "domicilio_normalizado": None, "latitud": None, "longitud": None,
                                "provincia_error": None, "departamento_error": None,
                                "domicilio_error": f"excepción: {e}"}
            if progress_cb:
                progress_cb(sum(1 for r in results if r is not None), n_total)

    return results
