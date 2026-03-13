import json
import os

from celery_app import celery_app
from config import settings
from services.job_store import JobStore
from services.excel_service import load_excel_rows
from services.ml_client import ml_client
from services.compatibility_service import process_rows_for_job


@celery_app.task(name="tasks.process_excel_job")
def process_excel_job(job_id: str, user_id: str) -> None:
    import asyncio
    asyncio.run(_process_excel_job(job_id, user_id))


async def _process_excel_job(job_id: str, user_id: str) -> None:
    job = JobStore.get(job_id)
    if not job:
        return

    try:
        JobStore.update(job_id, status="processing", message="Preparando procesamiento...", progress=1)

        await ml_client.startup()
        access_token = await ml_client.get_valid_token(user_id)

        xlsx_path = job["xlsx_path"]
        JobStore.update_progress(job_id, 5, "Leyendo Excel...")
        rows = load_excel_rows(xlsx_path)

        JobStore.update_progress(job_id, 10, f"Iniciando procesamiento de {len(rows)} filas...")
        outcome = await process_rows_for_job(job_id, access_token, rows)

        result_path = os.path.join(settings.upload_dir, f"{job_id}_resultado.json")
        with open(result_path, "w", encoding="utf-8") as f:
            json.dump(outcome["results"], f, ensure_ascii=False, indent=2)

        JobStore.update(
            job_id,
            status="success",
            message="Procesamiento finalizado correctamente",
            progress=100,
            result_path=result_path,
            summary=outcome["summary"],
        )

    except Exception as exc:
        JobStore.update(
            job_id,
            status="error",
            message=f"Error procesando Excel: {str(exc)}",
            progress=0,
        )
    finally:
        await ml_client.shutdown()