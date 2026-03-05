from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from urllib.parse import urlencode
import os
import uuid
import time
import pandas as pd
import openpyxl
import httpx

# =========================
# Config general
# =========================

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

app = FastAPI(title="Compatibilidades API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        # "https://TU-FRONTEND.vercel.app",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

JOBS: dict[str, dict] = {}

EXPECTED = ["SKU", "Marca", "Modelo", "Desde", "Hasta"]

# =========================
# Mercado Libre OAuth Config
# =========================

ML_AUTH_URL = "https://auth.mercadolibre.com/authorization"
ML_TOKEN_URL = "https://api.mercadolibre.com/oauth/token"

# ✅ Usa variables de entorno (NO hardcode)
ML_CLIENT_ID = os.getenv("ML_CLIENT_ID")              # tu App ID
ML_CLIENT_SECRET = os.getenv("ML_CLIENT_SECRET")      # tu Client Secret
ML_REDIRECT_URI = os.getenv("ML_REDIRECT_URI")        # ej: https://xxxx.ngrok-free.dev/auth/callback

# Demo in-memory token store (en producción usa DB/Redis)
TOKENS_BY_USER: dict[int, dict] = {}


def _require_ml_env():
    if not ML_CLIENT_ID:
        raise HTTPException(status_code=500, detail="Falta ML_CLIENT_ID en variables de entorno")
    if not ML_CLIENT_SECRET:
        raise HTTPException(status_code=500, detail="Falta ML_CLIENT_SECRET en variables de entorno")
    if not ML_REDIRECT_URI:
        raise HTTPException(
            status_code=500,
            detail="Falta ML_REDIRECT_URI en variables de entorno (debe coincidir EXACTO con el Redirect URI configurado en ML)",
        )


@app.get("/auth/login")
def ml_auth_login(state: str | None = None):
    """
    Inicia el flujo OAuth: redirige al usuario a Mercado Libre para autorizar la app.
    Abre este endpoint en el navegador.
    """
    _require_ml_env()

    params = {
        "response_type": "code",
        "client_id": ML_CLIENT_ID,
        "redirect_uri": ML_REDIRECT_URI,
    }

    # state es opcional pero recomendable (anti-CSRF / correlación de sesión)
    if state:
        params["state"] = state

    url = f"{ML_AUTH_URL}?{urlencode(params)}"
    return RedirectResponse(url)


@app.get("/auth/callback")
async def ml_auth_callback(code: str = Query(...), state: str | None = None):
    """
    Mercado Libre redirige aquí con ?code=...
    Canjea el code por access_token y refresh_token.
    """
    _require_ml_env()

    payload = {
        "grant_type": "authorization_code",
        "client_id": ML_CLIENT_ID,
        "client_secret": ML_CLIENT_SECRET,
        "code": code,
        "redirect_uri": ML_REDIRECT_URI,
    }

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(ML_TOKEN_URL, data=payload)

    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=r.text)

    token_data = r.json()
    user_id = token_data.get("user_id")

    if user_id:
        TOKENS_BY_USER[int(user_id)] = token_data

    return {
        "ok": True,
        "user_id": user_id,
        "scope": token_data.get("scope"),
        "expires_in": token_data.get("expires_in"),
        "has_refresh_token": bool(token_data.get("refresh_token")),
        "state": state,
        "message": "OAuth OK. Ya tienes access_token/refresh_token guardado (demo en memoria).",
    }


@app.post("/auth/refresh")
async def ml_refresh_token(user_id: int):
    """
    Renueva access_token usando refresh_token (ideal para no cortar integración cada 6 horas).
    """
    _require_ml_env()

    token_data = TOKENS_BY_USER.get(int(user_id))
    if not token_data or not token_data.get("refresh_token"):
        raise HTTPException(status_code=404, detail="No hay refresh_token guardado para ese user_id")

    payload = {
        "grant_type": "refresh_token",
        "client_id": ML_CLIENT_ID,
        "client_secret": ML_CLIENT_SECRET,
        "refresh_token": token_data["refresh_token"],
    }

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(ML_TOKEN_URL, data=payload)

    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=r.text)

    new_token_data = r.json()
    TOKENS_BY_USER[int(user_id)] = new_token_data

    return {
        "ok": True,
        "user_id": int(user_id),
        "expires_in": new_token_data.get("expires_in"),
        "message": "Token renovado y guardado (demo en memoria).",
    }


# =========================
# Tu lógica Excel -> CSV
# =========================

class JobResponse(BaseModel):
    job_id: str
    status: str
    message: str
    progress: int


def debug_print_excel_first10(xlsx_path: str, sheet_name: str = "Hoja1", n_rows: int = 5):
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    if sheet_name not in wb.sheetnames:
        sheet_name = wb.sheetnames[0]
    ws = wb[sheet_name]

    headers = [ws.cell(row=1, column=c).value for c in range(1, 11)]
    print("\n==============================")
    print(f"DEBUG EXCEL: Archivo: {os.path.basename(xlsx_path)}")
    print(f"DEBUG EXCEL: Hoja: {sheet_name}")
    print("DEBUG EXCEL: Primeras 10 columnas (headers):")
    for i, h in enumerate(headers, start=1):
        print(f"  {i}. {h}")

    print(f"\nDEBUG EXCEL: Primeras {n_rows} filas (valores 10 primeras columnas):")
    for r in range(2, 2 + n_rows):
        row_vals = [ws.cell(row=r, column=c).value for c in range(1, 11)]
        print(f"FILA {r}: {row_vals}")

    print("==============================\n")


def excel_to_normalized_csv(xlsx_path: str, csv_path: str, sheet_name: str = "Hoja1"):
    df = pd.read_excel(
        xlsx_path,
        sheet_name=sheet_name,
        engine="openpyxl",
        usecols=EXPECTED,
    )

    df.columns = [str(c).strip() for c in df.columns]

    missing = [c for c in EXPECTED if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas en Excel: {missing}. Detectadas: {list(df.columns)}")

    df = df[EXPECTED].copy()

    df["SKU"] = df["SKU"].astype(str).str.strip()
    df["Marca"] = df["Marca"].astype(str).str.strip().str.upper()
    df["Modelo"] = df["Modelo"].astype(str).str.strip().str.upper()
    df["Desde"] = pd.to_numeric(df["Desde"], errors="coerce")
    df["Hasta"] = pd.to_numeric(df["Hasta"], errors="coerce")

    df.to_csv(csv_path, index=False, encoding="utf-8")


def process_csv_job(job_id: str):
    job = JOBS[job_id]
    csv_path = job["csv_path"]

    job["status"] = "processing"
    job["message"] = "Procesando CSV normalizado..."
    job["progress"] = 0

    try:
        total_rows = sum(1 for _ in open(csv_path, "rb")) - 1
        if total_rows <= 0:
            job["status"] = "error"
            job["message"] = "El CSV no tiene filas."
            job["progress"] = 0
            return

        processed = 0
        chunksize = 5000

        for chunk in pd.read_csv(csv_path, chunksize=chunksize, encoding="utf-8"):
            if processed == 0:
                print("\n=== DEBUG CSV NORMALIZADO ===")
                print("Columnas:", list(chunk.columns))
                print(chunk.head(5).to_string(index=False))
                print("=============================\n")

            # TODO: aquí va la lógica real de ML para compatibilidades
            time.sleep(0.02)

            processed += len(chunk)
            job["progress"] = int(processed * 100 / max(total_rows, 1))
            job["message"] = f"Procesando... {processed}/{total_rows}"

        job["status"] = "success"
        job["message"] = "Proceso finalizado."
        job["progress"] = 100

    except Exception as e:
        job["status"] = "error"
        job["message"] = f"Error procesando: {str(e)}"
        job["progress"] = 0


@app.post("/imports-excel", response_model=JobResponse)
async def upload_excel(file: UploadFile = File(...)):
    filename = (file.filename or "").lower()
    if not filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Solo se aceptan archivos .xlsx")

    job_id = str(uuid.uuid4())
    xlsx_path = os.path.join(UPLOAD_DIR, f"{job_id}.xlsx")
    csv_path = os.path.join(UPLOAD_DIR, f"{job_id}.csv")

    content = await file.read()
    with open(xlsx_path, "wb") as f:
        f.write(content)

    try:
        debug_print_excel_first10(xlsx_path, sheet_name="Hoja1", n_rows=5)
    except Exception as e:
        print("WARNING: no se pudo imprimir debug del Excel:", str(e))

    try:
        excel_to_normalized_csv(xlsx_path, csv_path, sheet_name="Hoja1")
    except Exception as e:
        JOBS[job_id] = {
            "status": "error",
            "message": f"Error leyendo Excel: {str(e)}",
            "progress": 0,
            "xlsx_path": xlsx_path,
            "csv_path": None,
            "created_at": int(time.time()),
        }
        return JobResponse(job_id=job_id, status="error", message=JOBS[job_id]["message"], progress=0)

    JOBS[job_id] = {
        "status": "ready",
        "message": "Excel subido y convertido a CSV normalizado. Listo para procesar.",
        "progress": 0,
        "xlsx_path": xlsx_path,
        "csv_path": csv_path,
        "created_at": int(time.time()),
    }

    return JobResponse(job_id=job_id, status="ready", message=JOBS[job_id]["message"], progress=0)


@app.post("/imports/{job_id}/start", response_model=JobResponse)
async def start_processing(job_id: str, background_tasks: BackgroundTasks):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job no existe")

    if job["status"] in ("processing", "success"):
        return JobResponse(job_id=job_id, status=job["status"], message=job["message"], progress=job["progress"])

    if job["status"] == "error":
        return JobResponse(job_id=job_id, status="error", message=job["message"], progress=job["progress"])

    if not job.get("csv_path"):
        return JobResponse(job_id=job_id, status="error", message="CSV no disponible para procesar.", progress=0)

    background_tasks.add_task(process_csv_job, job_id)
    job["status"] = "processing"
    job["message"] = "Procesamiento en cola..."
    job["progress"] = 0

    return JobResponse(job_id=job_id, status="processing", message=job["message"], progress=0)


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