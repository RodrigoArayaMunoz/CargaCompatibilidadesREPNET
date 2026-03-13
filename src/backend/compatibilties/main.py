import json
import os
import uuid
from contextlib import asynccontextmanager
from urllib.parse import urlencode

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

from config import settings
from schemas import JobResponse
from services.token_store import token_store, require_ml_env
from services.job_store import JobStore
from services.excel_service import save_upload_file, load_excel_rows
from services.ml_client import ml_client
from tasks.import_tasks import process_excel_job


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(settings.upload_dir, exist_ok=True)
    await ml_client.startup()
    yield
    await ml_client.shutdown()


app = FastAPI(title="Compatibilidades API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        settings.frontend_url,
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/ml/status")
async def ml_status():
    user_id = token_store.first_user_id()
    if not user_id:
        return {"connected": False}

    token_data = token_store.get(user_id)
    if not token_data:
        return {"connected": False}

    try:
        await ml_client.get_valid_token(user_id)
        token_data = token_store.get(user_id)
        return {
            "connected": True,
            "user_id": user_id,
            "has_refresh_token": bool(token_data.get("refresh_token")),
            "expires_in": token_data.get("expires_in"),
            "expires_at": token_data.get("expires_at"),
        }
    except HTTPException:
        return {"connected": False}


@app.get("/ml/me")
async def ml_me(user_id: int):
    access_token = await ml_client.get_valid_token(user_id)
    data = await ml_client.request("GET", "/users/me", access_token)
    return data


@app.get("/auth/login")
def ml_auth_login():
    require_ml_env()
    params = {
        "response_type": "code",
        "client_id": settings.ml_client_id,
        "redirect_uri": settings.ml_redirect_uri,
    }
    url = f"{settings.ml_auth_url}?{urlencode(params)}"
    return RedirectResponse(url=url)


@app.get("/auth/callback")
async def ml_auth_callback(code: str = Query(...), state: str | None = None):
    require_ml_env()

    payload = {
        "grant_type": "authorization_code",
        "client_id": settings.ml_client_id,
        "client_secret": settings.ml_client_secret,
        "code": code,
        "redirect_uri": settings.ml_redirect_uri,
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/x-www-form-urlencoded",
    }

    if not ml_client.client:
        raise HTTPException(status_code=500, detail="HTTP client no inicializado")

    r = await ml_client.client.post(settings.ml_token_url, data=payload, headers=headers)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=r.text)

    token_data = r.json()
    user_id = token_data.get("user_id")
    if not user_id:
        raise HTTPException(status_code=500, detail="No se recibió user_id")

    token_store.set(user_id, token_store.build_payload(token_data, user_id))
    return RedirectResponse(url=settings.frontend_url)


@app.post("/auth/refresh")
async def ml_refresh_token(user_id: int):
    new_token_data = await ml_client.refresh_token(user_id)
    return {
        "ok": True,
        "user_id": int(user_id),
        "expires_at": new_token_data.get("expires_at"),
        "expires_in": new_token_data.get("expires_in"),
        "message": "Token renovado correctamente",
    }


@app.post("/auth/logout")
async def ml_logout(user_id: int):
    token_store.remove(user_id)
    return {"ok": True, "message": "Sesión local eliminada"}


@app.post("/imports-excel", response_model=JobResponse)
async def upload_excel(file: UploadFile = File(...)):
    filename = (file.filename or "").lower()
    if not filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Solo se aceptan archivos .xlsx")

    job_id = str(uuid.uuid4())
    xlsx_path = os.path.join(settings.upload_dir, f"{job_id}.xlsx")

    save_upload_file(file, xlsx_path)

    try:
        load_excel_rows(xlsx_path)
    except Exception as exc:
        JobStore.create(job_id, {
            "status": "error",
            "message": f"Error leyendo Excel: {str(exc)}",
            "progress": 0,
            "xlsx_path": xlsx_path,
        })
        job = JobStore.get(job_id)
        return JobResponse(
            job_id=job_id,
            status=job["status"],
            message=job["message"],
            progress=job["progress"],
        )

    JobStore.create(job_id, {"xlsx_path": xlsx_path})

    job = JobStore.get(job_id)
    return JobResponse(
        job_id=job_id,
        status=job["status"],
        message=job["message"],
        progress=job["progress"],
    )


@app.post("/imports/{job_id}/start", response_model=JobResponse)
async def start_processing(job_id: str):
    job = JobStore.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job no existe")

    if job["status"] in ("processing", "success"):
        return JobResponse(job_id=job_id, status=job["status"], message=job["message"], progress=job["progress"])

    if not job.get("xlsx_path"):
        raise HTTPException(status_code=400, detail="Excel no disponible")

    user_id = token_store.first_user_id()
    if not user_id:
        raise HTTPException(status_code=401, detail="No hay cuenta de Mercado Libre conectada")

    JobStore.update(job_id, status="queued", message="Procesamiento en cola...", progress=0)
    process_excel_job.delay(job_id, str(user_id))

    job = JobStore.get(job_id)
    return JobResponse(job_id=job_id, status=job["status"], message=job["message"], progress=job["progress"])


@app.get("/imports/{job_id}", response_model=JobResponse)
async def get_job(job_id: str):
    job = JobStore.get(job_id)
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
    job = JobStore.get(job_id)
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