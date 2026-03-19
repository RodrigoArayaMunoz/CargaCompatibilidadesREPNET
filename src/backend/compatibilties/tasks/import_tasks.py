import asyncio
import json
import os

from celery_app import celery_app
from config import settings
from database import AsyncSessionLocal
from services.compatibility_service import process_rows_for_job
from services.excel_service import load_excel_rows
from services.job_store import JobStore
from services.ml_client import ml_client


@celery_app.task(name="tasks.process_excel_job")
def process_excel_job(job_id: str) -> None:
    asyncio.run(_process_excel_job(job_id))


async def _process_excel_job(job_id: str) -> None:
    job = JobStore.get(job_id)
    if not job:
        return

    ml_started = False

    try:
        JobStore.update(
            job_id,
            status="processing",
            message="Preparando procesamiento...",
            progress=1,
        )

        xlsx_path = job.get("xlsx_path")
        if not xlsx_path:
            raise ValueError("El job no tiene xlsx_path asociado")

        if not os.path.exists(xlsx_path):
            raise FileNotFoundError(f"No existe el archivo Excel: {xlsx_path}")

        await ml_client.startup()
        ml_started = True

        async with AsyncSessionLocal() as db:
            access_token = await ml_client.get_valid_token(db)

        JobStore.update_progress(job_id, 5, "Leyendo archivo...")
        rows = load_excel_rows(xlsx_path)

        total_rows = len(rows)
        if total_rows == 0:
            raise ValueError("El archivo no contiene filas válidas para procesar")

        JobStore.update(
            job_id,
            total_rows=total_rows,
            processed_rows=0,
            progress=10,
            message=f"Iniciando procesamiento de {total_rows} filas...",
        )

        outcome = await process_rows_for_job(job_id, access_token, rows)

        os.makedirs(settings.upload_dir, exist_ok=True)
        result_path = os.path.join(settings.upload_dir, f"{job_id}_resultado.json")

        with open(result_path, "w", encoding="utf-8") as f:
            json.dump(outcome.get("results", []), f, ensure_ascii=False, indent=2)

        JobStore.update(
            job_id,
            status="success",
            message="Procesamiento finalizado correctamente",
            progress=100,
            result_path=result_path,
            summary=outcome.get("summary", {}),
            results_count=len(outcome.get("results", [])),
            processed_rows=total_rows,
        )

    except Exception as exc:
        JobStore.update(
            job_id,
            status="error",
            message=f"Error procesando Excel: {str(exc)}",
            progress=0,
        )
        raise

    finally:
        if ml_started:
            try:
                await ml_client.shutdown()
            except Exception:
                pass