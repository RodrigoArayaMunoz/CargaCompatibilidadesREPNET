import json
import os
from contextlib import asynccontextmanager
from urllib.parse import urlencode

from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db, check_db_connection
from config import settings
from schemas import JobResponse
from services.ml_publicationswithout_service import ml_publications_service
from services.token_store import token_store, require_ml_env
from services.job_store import JobStore
from services.excel_service import save_upload_file, load_excel_rows
from services.ml_client import ml_client
from tasks.import_tasks import process_excel_job


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(settings.upload_dir, exist_ok=True)
    await check_db_connection()
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
async def ml_status(db: AsyncSession = Depends(get_db)):
    token_data = await token_store.get(db)
    if not token_data:
        return {
            "connected": False,
            "message": "No hay cuenta de Mercado Libre conectada",
        }

    try:
        access_token = await ml_client.get_valid_token(db)
        me = await ml_client.request("GET", "/users/me", access_token)

        return {
            "connected": True,
            "message": "Cuenta conectada correctamente",
            "account": {
                "user_id": me.get("id"),
                "nickname": me.get("nickname"),
                "email": me.get("email"),
            },
            "expires_at": token_data.get("expires_at"),
        }
    except HTTPException:
        return {
            "connected": False,
            "message": "La conexión con Mercado Libre no es válida. Debes reconectar.",
        }


@app.get("/ml/me")
async def ml_me(db: AsyncSession = Depends(get_db)):
    access_token = await ml_client.get_valid_token(db)
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
async def ml_auth_callback(
    code: str = Query(...),
    state: str | None = None,
    db: AsyncSession = Depends(get_db),
):
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
    if not token_data.get("access_token"):
        raise HTTPException(status_code=500, detail="No se recibió access_token")

    payload_to_save = token_store.build_payload(token_data)
    await token_store.set(db, payload_to_save)

    return RedirectResponse(url=f"{settings.frontend_url}/ml-connected")


@app.post("/auth/refresh")
async def ml_refresh_token(db: AsyncSession = Depends(get_db)):
    new_token_data = await ml_client.refresh_token(db)
    return {
        "ok": True,
        "expires_at": new_token_data.get("expires_at"),
        "expires_in": new_token_data.get("expires_in"),
        "message": "Token renovado correctamente",
    }


@app.post("/auth/logout")
async def ml_logout(db: AsyncSession = Depends(get_db)):
    await token_store.remove(db)
    return {"ok": True, "message": "Sesión global eliminada"}


@app.post("/imports-excel", response_model=JobResponse)
async def upload_excel(file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Archivo inválido")

    filename_lower = file.filename.lower()
    if not filename_lower.endswith((".xlsx", ".xls", ".csv")):
        raise HTTPException(
            status_code=400,
            detail="Formato no permitido. Solo se aceptan archivos .xlsx, .xls o .csv",
        )

    try:
        job = JobStore.create(file.filename)

        saved_path = await save_upload_file(file, settings.upload_dir)
        rows = load_excel_rows(saved_path)

        JobStore.update(
            job["id"],
            status="uploaded",
            message="Archivo cargado correctamente. Listo para procesar.",
            progress=0,
            xlsx_path=saved_path,
            total_rows=len(rows),
            processed_rows=0,
        )

        job = JobStore.get(job["id"])
        if not job:
            raise HTTPException(status_code=500, detail="No se pudo recuperar el job creado")

        return JobResponse(
            job_id=job["id"],
            status=job.get("status", "uploaded"),
            message=job.get("message", "Archivo cargado correctamente"),
            progress=job.get("progress", 0),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al cargar el archivo: {str(e)}")


@app.post("/imports/{job_id}/start", response_model=JobResponse)
async def start_processing(job_id: str, db: AsyncSession = Depends(get_db)):
    job = JobStore.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job no existe")

    if job.get("status") in ("processing", "success"):
        return JobResponse(
            job_id=job_id,
            status=job.get("status", "pending"),
            message=job.get("message", ""),
            progress=job.get("progress", 0),
        )

    if not job.get("xlsx_path"):
        raise HTTPException(status_code=400, detail="Excel no disponible")

    token_data = await token_store.get(db)
    if not token_data:
        raise HTTPException(status_code=401, detail="No hay cuenta de Mercado Libre conectada")


    JobStore.update(
        job_id,
        status="queued",
        message="Procesamiento en cola...",
        progress=0,
    )

    task = process_excel_job.delay(job_id)
    JobStore.update(job_id, task_id=task.id)

    job = JobStore.get(job_id)
    return JobResponse(
        job_id=job_id,
        status=job.get("status", "queued"),
        message=job.get("message", "Procesamiento en cola..."),
        progress=job.get("progress", 0),
    )


@app.get("/imports/{job_id}", response_model=JobResponse)
async def get_job(job_id: str):
    job = JobStore.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job no existe")

    return JobResponse(
        job_id=job_id,
        status=job.get("status", "pending"),
        message=job.get("message", ""),
        progress=job.get("progress", 0),
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


@app.get("/imports-excel/{job_id}", response_model=JobResponse)
async def get_import_status(job_id: str):
    job = JobStore.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job no encontrado")

    return JobResponse(
        job_id=job_id,
        status=job.get("status", "pending"),
        message=job.get("message", ""),
        progress=job.get("progress", 0),
    )


@app.get("/publications/without-compatibilities")
async def get_publications_without_compatibilities(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=20),
    q: str = Query(""),
    refresh: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    token_data = await token_store.get(db)
    if not token_data:
        raise HTTPException(status_code=401, detail="No hay cuenta de Mercado Libre conectada")

    return await ml_publications_service.get_publications_without_compatibilities(
    db=db,
    page=page,
    page_size=page_size,
    q=q,
    refresh=refresh,
)


@app.post("/publications/without-compatibilities/refresh")
async def refresh_publications_without_compatibilities(
    db: AsyncSession = Depends(get_db),
):
    token_data = await token_store.get(db)
    if not token_data:
        raise HTTPException(status_code=401, detail="No hay cuenta de Mercado Libre conectada")

    return await ml_publications_service.start_background_refresh(db=db)


@app.get("/publications/without-compatibilities/refresh-status")
async def get_publications_without_compatibilities_refresh_status(
    db: AsyncSession = Depends(get_db),
):
    token_data = await token_store.get(db)
    if not token_data:
        raise HTTPException(status_code=401, detail="No hay cuenta de Mercado Libre conectada")

    return await ml_publications_service.get_refresh_status(db=db)