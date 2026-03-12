from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from urllib.parse import urlencode
from dotenv import load_dotenv
from typing import Any
import asyncio
import math
import os
import uuid
import time
import json
import unicodedata

import pandas as pd
import openpyxl
import httpx


# =========================
# CARGA DE VARIABLES
# =========================
load_dotenv()

# =========================
# ARCHIVOS / DIRECTORIOS
# =========================
UPLOAD_DIR = "uploads"
TOKENS_FILE = "tokens.json"

os.makedirs(UPLOAD_DIR, exist_ok=True)

# =========================
# CONFIG MERCADO LIBRE
# =========================
ML_CLIENT_ID = os.getenv("ML_CLIENT_ID")
ML_CLIENT_SECRET = os.getenv("ML_CLIENT_SECRET")
ML_REDIRECT_URI = os.getenv("ML_REDIRECT_URI")
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:5173")

# Chile
ML_AUTH_URL = "https://auth.mercadolibre.cl/authorization"
ML_TOKEN_URL = "https://api.mercadolibre.com/oauth/token"
ML_ME_URL = "https://api.mercadolibre.com/users/me"
ML_API_BASE = "https://api.mercadolibre.com"
ML_DOMAIN_ID = "MLC-CARS_AND_VANS_FOR_COMPATIBILITIES"
ML_SITE_ID = "MLC"

# =========================
# COLUMNAS REQUERIDAS
# =========================
COMPAT_REQUIRED_COLUMNS = [
    "ASOCIACION ML",
    "Marca",
    "Modelo",
    "CILINDRADA ABREVIADA",
    "TRANSMISION",
    "Desde",
    "Hasta",
]


# =========================
# HELPERS TOKENS
# =========================
def load_tokens() -> dict:
    """
    Carga tokens desde tokens.json.
    Si no existe o está vacío/corrupto, devuelve {}.
    """
    try:
        if not os.path.exists(TOKENS_FILE):
            with open(TOKENS_FILE, "w", encoding="utf-8") as f:
                json.dump({}, f, ensure_ascii=False, indent=2)
            return {}

        with open(TOKENS_FILE, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return {}
            return json.loads(content)
    except Exception:
        return {}


def save_tokens(tokens: dict) -> None:
    """
    Guarda tokens en tokens.json.
    """
    with open(TOKENS_FILE, "w", encoding="utf-8") as f:
        json.dump(tokens, f, ensure_ascii=False, indent=2)


def get_token_data(user_id: int | str):
    """
    Busca token por user_id, considerando que en JSON
    las keys se guardan como string.
    """
    return TOKENS_BY_USER.get(str(user_id))


def remove_token_data(user_id: int | str) -> None:
    """
    Elimina los tokens de un usuario de memoria y disco.
    """
    TOKENS_BY_USER.pop(str(user_id), None)
    save_tokens(TOKENS_BY_USER)


def get_first_user_id() -> str | None:
    """
    Devuelve el primer user_id guardado o None.
    """
    if not TOKENS_BY_USER:
        return None
    return next(iter(TOKENS_BY_USER.keys()))


def build_token_payload(token_data: dict, user_id: int | str) -> dict:
    """
    Agrega expires_at para saber cuándo refrescar el token.
    Se deja un margen de 60 segundos.
    """
    expires_in = int(token_data.get("expires_in", 0))
    token_data["user_id"] = int(user_id)
    token_data["expires_at"] = int(time.time()) + expires_in - 60
    return token_data


def _require_ml_env():
    if not ML_CLIENT_ID:
        raise HTTPException(status_code=500, detail="Falta ML_CLIENT_ID en variables de entorno")
    if not ML_CLIENT_SECRET:
        raise HTTPException(status_code=500, detail="Falta ML_CLIENT_SECRET en variables de entorno")
    if not ML_REDIRECT_URI:
        raise HTTPException(status_code=500, detail="Falta ML_REDIRECT_URI en variables de entorno")


async def validate_ml_token(access_token: str) -> bool:
    """
    Valida si el access_token realmente sigue sirviendo contra Mercado Libre.
    """
    if not access_token:
        return False

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(
                ML_ME_URL,
                headers={"Authorization": f"Bearer {access_token}"}
            )
        return r.status_code == 200
    except Exception:
        return False


async def refresh_ml_token_internal(user_id: int | str) -> dict:
    """
    Refresca el token de un usuario y lo guarda en memoria y en tokens.json.
    Mercado Libre devuelve también refresh_token nuevo, y hay que guardarlo.
    """
    _require_ml_env()

    token_data = get_token_data(user_id)
    if not token_data:
        raise HTTPException(status_code=404, detail="No hay token guardado para ese user_id")

    refresh_token = token_data.get("refresh_token")
    if not refresh_token:
        remove_token_data(user_id)
        raise HTTPException(status_code=400, detail="No hay refresh_token guardado para ese user_id")

    payload = {
        "grant_type": "refresh_token",
        "client_id": ML_CLIENT_ID,
        "client_secret": ML_CLIENT_SECRET,
        "refresh_token": refresh_token,
    }

    headers = {
        "accept": "application/json",
        "content-type": "application/x-www-form-urlencoded",
    }

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(ML_TOKEN_URL, data=payload, headers=headers)

    if r.status_code >= 400:
        remove_token_data(user_id)
        raise HTTPException(status_code=r.status_code, detail=r.text)

    new_token_data = r.json()
    new_token_data = build_token_payload(new_token_data, user_id)

    TOKENS_BY_USER[str(user_id)] = new_token_data
    save_tokens(TOKENS_BY_USER)

    return new_token_data


async def get_valid_ml_token(user_id: int | str) -> str:
    """
    Devuelve un access_token válido.
    Si expiró o ML lo rechaza, intenta refrescar automáticamente.
    Si no puede recuperarlo, limpia la sesión del usuario.
    """
    token_data = get_token_data(user_id)
    if not token_data:
        raise HTTPException(status_code=404, detail="No hay token guardado para ese user_id")

    access_token = token_data.get("access_token")
    expires_at = int(token_data.get("expires_at", 0))
    now = int(time.time())

    if not access_token or now >= expires_at:
        try:
            token_data = await refresh_ml_token_internal(user_id)
            access_token = token_data.get("access_token")
        except HTTPException:
            remove_token_data(user_id)
            raise

    is_valid = await validate_ml_token(access_token)
    if is_valid:
        return access_token

    try:
        token_data = await refresh_ml_token_internal(user_id)
        access_token = token_data.get("access_token")
    except HTTPException:
        remove_token_data(user_id)
        raise HTTPException(status_code=401, detail="Token inválido y no se pudo refrescar")

    is_valid = await validate_ml_token(access_token)
    if not is_valid:
        remove_token_data(user_id)
        raise HTTPException(status_code=401, detail="Token inválido incluso después del refresh")

    return access_token


# =========================
# HELPERS GENERALES
# =========================
def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def normalize_for_compare(value: Any) -> str:
    text = normalize_text(value).lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("-", " ").replace("_", " ")
    text = " ".join(text.split())
    return text


def normalize_year(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
        year = int(float(value))
        if year <= 0:
            return None
        return year
    except Exception:
        return None


def normalize_engine(value: Any) -> str:
    """
    Convierte cilindrada a texto flexible para búsqueda.
    Ej: 2 -> '2.0', 1.6 -> '1.6'
    """
    if value is None:
        return ""
    try:
        if isinstance(value, float) and math.isnan(value):
            return ""
        num = float(value)
        if num.is_integer():
            return f"{int(num)}.0"
        return str(num).strip()
    except Exception:
        return normalize_text(value)


def normalize_transmission(value: Any) -> str:
    text = normalize_for_compare(value)
    mapping = {
        "manual": "Manual",
        "mecanico": "Manual",
        "mecanica": "Manual",
        "mecanico manual": "Manual",
        "mecanica manual": "Manual",
        "automatico": "Automática",
        "automatica": "Automática",
        "auto": "Automática",
        "cvt": "CVT",
    }
    return mapping.get(text, normalize_text(value))


def extract_item_id(value: Any) -> str:
    """
    Toma el ITEM_ID desde ASOCIACION ML.
    Ej: MLC3727328882
    """
    text = normalize_text(value).upper()
    return text


def debug_print_excel_first15(xlsx_path: str, sheet_name: str = "Hoja1", n_rows: int = 5):
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)

    if sheet_name not in wb.sheetnames:
        sheet_name = wb.sheetnames[0]

    ws = wb[sheet_name]

    headers = [ws.cell(row=1, column=c).value for c in range(1, 16)]
    print("\n==============================")
    print(f"DEBUG EXCEL: Archivo: {os.path.basename(xlsx_path)}")
    print(f"DEBUG EXCEL: Hoja: {sheet_name}")
    print("DEBUG EXCEL: Primeras 15 columnas (headers):")
    for i, h in enumerate(headers, start=1):
        print(f"  {i}. {h}")

    print(f"\nDEBUG EXCEL: Primeras {n_rows} filas (valores 15 primeras columnas):")
    for r in range(2, 2 + n_rows):
        row_vals = [ws.cell(row=r, column=c).value for c in range(1, 16)]
        print(f"FILA {r}: {row_vals}")

    print("==============================\n")


# =========================
# APP
# =========================
app = FastAPI(title="Compatibilidades API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        # "https://tu-frontend.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# ESTADO GLOBAL
# =========================
JOBS: dict[str, dict] = {}
TOKENS_BY_USER = load_tokens()


# =========================
# STATUS MERCADO LIBRE
# =========================
@app.get("/ml/status")
async def ml_status():
    """
    Devuelve si existe una cuenta ML conectada y con token válido.
    Si el token expiró, intenta refrescarlo.
    """
    user_id = get_first_user_id()
    if not user_id:
        return {"connected": False}

    token_data = get_token_data(user_id)
    if not token_data:
        return {"connected": False}

    try:
        await get_valid_ml_token(user_id)
        token_data = get_token_data(user_id)

        return {
            "connected": True,
            "user_id": user_id,
            "has_refresh_token": bool(token_data.get("refresh_token")),
            "expires_in": token_data.get("expires_in"),
            "expires_at": token_data.get("expires_at"),
        }
    except HTTPException:
        return {"connected": False}


@app.get("/ml/debug-token")
async def ml_debug_token(user_id: int):
    token_data = get_token_data(user_id)
    if not token_data:
        raise HTTPException(status_code=404, detail="No hay token guardado para ese user_id")
    return token_data


@app.get("/ml/access-token")
async def ml_access_token(user_id: int):
    access_token = await get_valid_ml_token(user_id)
    token_data = get_token_data(user_id)

    return {
        "ok": True,
        "user_id": int(user_id),
        "access_token": access_token,
        "expires_at": token_data.get("expires_at"),
        "expires_in": token_data.get("expires_in"),
    }


@app.get("/ml/me")
async def ml_me(user_id: int):
    access_token = await get_valid_ml_token(user_id)

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(
            ML_ME_URL,
            headers={"Authorization": f"Bearer {access_token}"}
        )

    if r.status_code == 401:
        remove_token_data(user_id)
        raise HTTPException(status_code=401, detail="Sesión inválida. Debes reconectar Mercado Libre.")

    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=r.text)

    return r.json()


# =========================
# OAUTH MERCADO LIBRE
# =========================
@app.get("/auth/login")
def ml_auth_login():
    _require_ml_env()

    params = {
        "response_type": "code",
        "client_id": ML_CLIENT_ID,
        "redirect_uri": ML_REDIRECT_URI,
    }

    url = f"{ML_AUTH_URL}?{urlencode(params)}"
    return RedirectResponse(url=url)


@app.get("/auth/callback")
async def ml_auth_callback(code: str = Query(...), state: str | None = None):
    _require_ml_env()

    payload = {
        "grant_type": "authorization_code",
        "client_id": ML_CLIENT_ID,
        "client_secret": ML_CLIENT_SECRET,
        "code": code,
        "redirect_uri": ML_REDIRECT_URI,
    }

    headers = {
        "accept": "application/json",
        "content-type": "application/x-www-form-urlencoded",
    }

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(ML_TOKEN_URL, data=payload, headers=headers)

    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=r.text)

    token_data = r.json()
    user_id = token_data.get("user_id")

    if not user_id:
        raise HTTPException(status_code=500, detail="No se recibió user_id desde Mercado Libre")

    token_data = build_token_payload(token_data, user_id)

    TOKENS_BY_USER[str(user_id)] = token_data
    save_tokens(TOKENS_BY_USER)

    return RedirectResponse(url=FRONTEND_URL)


@app.post("/auth/refresh")
async def ml_refresh_token(user_id: int):
    new_token_data = await refresh_ml_token_internal(user_id)

    return {
        "ok": True,
        "user_id": int(user_id),
        "access_token": new_token_data.get("access_token"),
        "refresh_token": new_token_data.get("refresh_token"),
        "expires_in": new_token_data.get("expires_in"),
        "expires_at": new_token_data.get("expires_at"),
        "message": "Token renovado correctamente",
    }


@app.post("/auth/logout")
async def ml_logout(user_id: int):
    token_data = get_token_data(user_id)
    if not token_data:
        return {
            "ok": True,
            "message": "No había sesión local para ese user_id"
        }

    remove_token_data(user_id)
    return {
        "ok": True,
        "message": "Sesión local eliminada correctamente"
    }


# =========================
# MODELOS DE RESPUESTA
# =========================
class JobResponse(BaseModel):
    job_id: str
    status: str
    message: str
    progress: int


# =========================
# HELPERS MERCADO LIBRE API
# =========================
async def ml_request(
    method: str,
    path: str,
    access_token: str,
    json_body: dict | None = None,
    params: dict | None = None,
) -> dict:
    url = f"{ML_API_BASE}{path}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    if json_body is not None:
        headers["Content-Type"] = "application/json"

    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.request(
            method=method,
            url=url,
            headers=headers,
            json=json_body,
            params=params,
        )

    if r.status_code >= 400:
        raise HTTPException(
            status_code=r.status_code,
            detail=f"ML API error {r.status_code}: {r.text}"
        )

    try:
        return r.json()
    except Exception:
        return {}


async def get_top_values(
    access_token: str,
    attribute_id: str,
    known_attributes: list[dict] | None = None,
    limit: int = 50,
) -> list[dict]:
    body = {"limit": limit}
    if known_attributes:
        body["known_attributes"] = known_attributes

    data = await ml_request(
        "POST",
        f"/catalog_domains/{ML_DOMAIN_ID}/attributes/{attribute_id}/top_values",
        access_token,
        json_body=body,
    )

    return data.get("values", []) or data.get("results", []) or []


def pick_value_id_by_name(values: list[dict], wanted_name: str) -> str | None:
    wanted = normalize_for_compare(wanted_name)
    if not wanted:
        return None

    # match exacto
    for item in values:
        name = normalize_for_compare(item.get("name"))
        if name == wanted:
            return str(item.get("id"))

    # match parcial wanted dentro de name
    for item in values:
        name = normalize_for_compare(item.get("name"))
        if wanted in name:
            return str(item.get("id"))

    # match parcial name dentro de wanted
    for item in values:
        name = normalize_for_compare(item.get("name"))
        if name and name in wanted:
            return str(item.get("id"))

    return None


async def resolve_brand_id(access_token: str, brand_name: str) -> str | None:
    values = await get_top_values(access_token, "BRAND")
    return pick_value_id_by_name(values, brand_name)


async def resolve_model_id(access_token: str, brand_id: str, model_name: str) -> str | None:
    values = await get_top_values(
        access_token,
        "CAR_AND_VAN_MODEL",
        known_attributes=[
            {"id": "BRAND", "value_ids": [brand_id]}
        ],
    )
    return pick_value_id_by_name(values, model_name)


async def resolve_year_id(access_token: str, brand_id: str, model_id: str, year: int) -> str | None:
    values = await get_top_values(
        access_token,
        "YEAR",
        known_attributes=[
            {"id": "BRAND", "value_ids": [brand_id]},
            {"id": "CAR_AND_VAN_MODEL", "value_ids": [model_id]},
        ],
    )
    return pick_value_id_by_name(values, str(year))


async def resolve_transmission_id(
    access_token: str,
    brand_id: str,
    model_id: str,
    year_id: str,
    transmission_name: str,
) -> str | None:
    if not transmission_name:
        return None

    values = await get_top_values(
        access_token,
        "TRANSMISSION_CONTROL_TYPE",
        known_attributes=[
            {"id": "BRAND", "value_ids": [brand_id]},
            {"id": "CAR_AND_VAN_MODEL", "value_ids": [model_id]},
            {"id": "YEAR", "value_ids": [year_id]},
        ],
    )
    return pick_value_id_by_name(values, transmission_name)


async def resolve_engine_id(
    access_token: str,
    brand_id: str,
    model_id: str,
    year_id: str,
    engine_name: str,
    transmission_id: str | None = None,
) -> str | None:
    if not engine_name:
        return None

    known_attributes = [
        {"id": "BRAND", "value_ids": [brand_id]},
        {"id": "CAR_AND_VAN_MODEL", "value_ids": [model_id]},
        {"id": "YEAR", "value_ids": [year_id]},
    ]

    if transmission_id:
        known_attributes.append({
            "id": "TRANSMISSION_CONTROL_TYPE",
            "value_ids": [transmission_id]
        })

    values = await get_top_values(
        access_token,
        "CAR_AND_VAN_ENGINE",
        known_attributes=known_attributes,
    )

    engine_id = pick_value_id_by_name(values, engine_name)
    if engine_id:
        return engine_id

    target = normalize_for_compare(engine_name)
    for item in values:
        name = normalize_for_compare(item.get("name"))
        if target and target in name:
            return str(item.get("id"))

    return None


async def search_vehicle_products(
    access_token: str,
    brand_id: str,
    model_id: str,
    year_id: str,
    transmission_id: str | None = None,
    engine_id: str | None = None,
) -> list[dict]:
    known_attributes = [
        {"id": "BRAND", "value_ids": [brand_id]},
        {"id": "CAR_AND_VAN_MODEL", "value_ids": [model_id]},
        {"id": "YEAR", "value_ids": [year_id]},
    ]

    if transmission_id:
        known_attributes.append({
            "id": "TRANSMISSION_CONTROL_TYPE",
            "value_ids": [transmission_id]
        })

    if engine_id:
        known_attributes.append({
            "id": "CAR_AND_VAN_ENGINE",
            "value_ids": [engine_id]
        })

    body = {
        "domain_id": ML_DOMAIN_ID,
        "site_id": ML_SITE_ID,
        "known_attributes": known_attributes,
        "limit": 10,
    }

    data = await ml_request(
        "POST",
        "/catalog_compatibilities/products_search/chunks",
        access_token,
        json_body=body,
    )

    return data.get("results", [])


async def search_vehicle_product_id(
    access_token: str,
    brand_id: str,
    model_id: str,
    year_id: str,
    transmission_id: str | None = None,
    engine_id: str | None = None,
) -> str | None:
    results = await search_vehicle_products(
        access_token=access_token,
        brand_id=brand_id,
        model_id=model_id,
        year_id=year_id,
        transmission_id=transmission_id,
        engine_id=engine_id,
    )
    if not results:
        return None
    first = results[0]
    if not first.get("id"):
        return None
    return str(first.get("id"))


async def add_item_compatibility(
    access_token: str,
    item_id: str,
    product_id: str,
    creation_source: str = "ITEM_SUGGESTIONS",
) -> dict:
    body = {
        "products": [
            {
                "id": product_id,
                "creation_source": creation_source,
            }
        ]
    }

    return await ml_request(
        "POST",
        f"/items/{item_id}/compatibilities",
        access_token,
        json_body=body,
    )


# =========================
# LÓGICA DE PROCESAMIENTO
# =========================
async def process_vehicle_row(access_token: str, row: dict) -> dict:
    item_id = extract_item_id(row.get("ASOCIACION ML"))
    brand_name = normalize_text(row.get("Marca"))
    model_name = normalize_text(row.get("Modelo"))
    engine_name = normalize_engine(row.get("CILINDRADA ABREVIADA"))
    transmission_name = normalize_transmission(row.get("TRANSMISION"))
    year_from = normalize_year(row.get("Desde"))
    year_to = normalize_year(row.get("Hasta"))

    if not item_id:
        return {"ok": False, "reason": "Fila sin ASOCIACION ML"}

    if not brand_name or not model_name or not year_from:
        return {
            "ok": False,
            "item_id": item_id,
            "reason": "Faltan datos mínimos: Marca / Modelo / Desde"
        }

    years = [year_from]
    if year_to and year_to >= year_from:
        years = list(range(year_from, year_to + 1))

    results = []

    brand_id = await resolve_brand_id(access_token, brand_name)
    if not brand_id:
        return {"ok": False, "item_id": item_id, "reason": f"No se encontró BRAND para '{brand_name}'"}

    model_id = await resolve_model_id(access_token, brand_id, model_name)
    if not model_id:
        return {"ok": False, "item_id": item_id, "reason": f"No se encontró MODEL para '{model_name}'"}

    for year in years:
        year_id = await resolve_year_id(access_token, brand_id, model_id, year)
        if not year_id:
            results.append({
                "ok": False,
                "item_id": item_id,
                "year": year,
                "reason": f"No se encontró YEAR={year}"
            })
            continue

        transmission_id = await resolve_transmission_id(
            access_token, brand_id, model_id, year_id, transmission_name
        )

        engine_id = await resolve_engine_id(
            access_token, brand_id, model_id, year_id, engine_name, transmission_id
        )

        product_id = await search_vehicle_product_id(
            access_token=access_token,
            brand_id=brand_id,
            model_id=model_id,
            year_id=year_id,
            transmission_id=transmission_id,
            engine_id=engine_id,
        )

        if not product_id:
            results.append({
                "ok": False,
                "item_id": item_id,
                "year": year,
                "reason": "No se encontró product_id exacto"
            })
            continue

        ml_response = await add_item_compatibility(
            access_token=access_token,
            item_id=item_id,
            product_id=product_id,
        )

        results.append({
            "ok": True,
            "item_id": item_id,
            "year": year,
            "product_id": product_id,
            "brand_id": brand_id,
            "model_id": model_id,
            "year_id": year_id,
            "transmission_id": transmission_id,
            "engine_id": engine_id,
            "ml_response": ml_response,
        })

    return {
        "ok": any(r["ok"] for r in results),
        "item_id": item_id,
        "results": results,
    }


async def process_excel_job_async(job_id: str, user_id: int | str):
    job = JOBS[job_id]
    xlsx_path = job["xlsx_path"]

    job["status"] = "processing"
    job["message"] = "Leyendo Excel y resolviendo compatibilidades..."
    job["progress"] = 0

    try:
        access_token = await get_valid_ml_token(user_id)

        df = pd.read_excel(xlsx_path, sheet_name="Hoja1", engine="openpyxl")
        df.columns = [str(c).strip() for c in df.columns]

        missing = [c for c in COMPAT_REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            job["status"] = "error"
            job["message"] = f"Faltan columnas requeridas: {missing}"
            job["progress"] = 0
            return

        total_rows = len(df.index)
        if total_rows <= 0:
            job["status"] = "error"
            job["message"] = "El Excel no tiene filas."
            job["progress"] = 0
            return

        output_rows = []
        success_count = 0
        error_count = 0

        for idx, (_, row) in enumerate(df.iterrows(), start=1):
            row_data = row.to_dict()

            try:
                result = await process_vehicle_row(access_token, row_data)
            except HTTPException as e:
                result = {
                    "ok": False,
                    "item_id": extract_item_id(row_data.get("ASOCIACION ML")),
                    "reason": f"HTTPException: {e.detail}"
                }
            except Exception as e:
                result = {
                    "ok": False,
                    "item_id": extract_item_id(row_data.get("ASOCIACION ML")),
                    "reason": f"Exception: {str(e)}"
                }

            output_rows.append(result)

            if result.get("ok"):
                success_count += 1
            else:
                error_count += 1

            job["progress"] = int(idx * 100 / max(total_rows, 1))
            job["message"] = (
                f"Procesando fila {idx}/{total_rows} | "
                f"ok={success_count} | error={error_count}"
            )

        result_path = os.path.join(UPLOAD_DIR, f"{job_id}_resultado.json")
        with open(result_path, "w", encoding="utf-8") as f:
            json.dump(output_rows, f, ensure_ascii=False, indent=2)

        job["status"] = "success"
        job["message"] = (
            f"Proceso finalizado. "
            f"Compatibilidades OK={success_count}, errores={error_count}"
        )
        job["progress"] = 100
        job["result_path"] = result_path
        job["summary"] = {
            "processed_rows": total_rows,
            "success_count": success_count,
            "error_count": error_count,
        }

    except Exception as e:
        job["status"] = "error"
        job["message"] = f"Error procesando Excel: {str(e)}"
        job["progress"] = 0


def process_excel_job(job_id: str, user_id: int | str):
    asyncio.run(process_excel_job_async(job_id, user_id))


# =========================
# ENDPOINTS IMPORT / JOBS
# =========================
@app.post("/imports-excel", response_model=JobResponse)
async def upload_excel(file: UploadFile = File(...)):
    filename = (file.filename or "").lower()
    if not filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Solo se aceptan archivos .xlsx")

    job_id = str(uuid.uuid4())
    xlsx_path = os.path.join(UPLOAD_DIR, f"{job_id}.xlsx")

    content = await file.read()
    with open(xlsx_path, "wb") as f:
        f.write(content)

    try:
        debug_print_excel_first15(xlsx_path, sheet_name="Hoja1", n_rows=5)
    except Exception as e:
        print("WARNING: no se pudo imprimir debug del Excel:", str(e))

    try:
        df = pd.read_excel(xlsx_path, sheet_name="Hoja1", engine="openpyxl")
        df.columns = [str(c).strip() for c in df.columns]
        missing = [c for c in COMPAT_REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"Faltan columnas requeridas: {missing}")
    except Exception as e:
        JOBS[job_id] = {
            "status": "error",
            "message": f"Error leyendo Excel: {str(e)}",
            "progress": 0,
            "xlsx_path": xlsx_path,
            "created_at": int(time.time()),
        }
        return JobResponse(
            job_id=job_id,
            status="error",
            message=JOBS[job_id]["message"],
            progress=0,
        )

    JOBS[job_id] = {
        "status": "ready",
        "message": "Excel subido correctamente. Listo para procesar compatibilidades.",
        "progress": 0,
        "xlsx_path": xlsx_path,
        "created_at": int(time.time()),
    }

    return JobResponse(
        job_id=job_id,
        status="ready",
        message=JOBS[job_id]["message"],
        progress=0,
    )


@app.post("/imports/{job_id}/start", response_model=JobResponse)
async def start_processing(job_id: str, background_tasks: BackgroundTasks):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job no existe")

    if job["status"] in ("processing", "success"):
        return JobResponse(
            job_id=job_id,
            status=job["status"],
            message=job["message"],
            progress=job["progress"],
        )

    if job["status"] == "error":
        return JobResponse(
            job_id=job_id,
            status="error",
            message=job["message"],
            progress=job["progress"],
        )

    if not job.get("xlsx_path"):
        return JobResponse(
            job_id=job_id,
            status="error",
            message="Excel no disponible para procesar.",
            progress=0,
        )

    user_id = get_first_user_id()
    if not user_id:
        raise HTTPException(status_code=401, detail="No hay cuenta de Mercado Libre conectada")

    background_tasks.add_task(process_excel_job, job_id, user_id)
    job["status"] = "processing"
    job["message"] = "Procesamiento en cola..."
    job["progress"] = 0

    return JobResponse(
        job_id=job_id,
        status="processing",
        message=job["message"],
        progress=0,
    )


@app.get("/imports/{job_id}", response_model=JobResponse)
async def get_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job no existe")

    return JobResponse(
        job_id=job_id,
        status=job["status"],
        message=job["message"],
        progress=job["progress"],
    )


@app.get("/imports/{job_id}/result")
async def get_job_result(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job no existe")

    if job.get("status") != "success":
        raise HTTPException(status_code=400, detail="El job aún no finaliza correctamente")

    result_path = job.get("result_path")
    if not result_path or not os.path.exists(result_path):
        raise HTTPException(status_code=404, detail="No se encontró archivo de resultado")

    with open(result_path, "r", encoding="utf-8") as f:
        result_data = json.load(f)

    return {
        "ok": True,
        "job_id": job_id,
        "summary": job.get("summary", {}),
        "results": result_data,
    }


# =========================
# ENDPOINT ÚTIL DE PRUEBA DIRECTA
# =========================
@app.post("/compatibilities/test-one-row")
async def compatibilities_test_one_row(user_id: int):
    """
    Procesa solo la primera fila válida del último Excel que tengas cargado,
    para probar el flujo completo sin recorrer todo el archivo.
    """
    access_token = await get_valid_ml_token(user_id)

    latest_job = None
    for _, job in JOBS.items():
        if job.get("xlsx_path"):
            latest_job = job

    if not latest_job:
        raise HTTPException(status_code=404, detail="No hay Excel cargado")

    xlsx_path = latest_job["xlsx_path"]
    df = pd.read_excel(xlsx_path, sheet_name="Hoja1", engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]

    missing = [c for c in COMPAT_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise HTTPException(status_code=400, detail=f"Faltan columnas: {missing}")

    if len(df.index) == 0:
        raise HTTPException(status_code=400, detail="El Excel no tiene filas")

    first_row = df.iloc[0].to_dict()
    result = await process_vehicle_row(access_token, first_row)

    return {
        "ok": True,
        "input_row": first_row,
        "result": result,
    }