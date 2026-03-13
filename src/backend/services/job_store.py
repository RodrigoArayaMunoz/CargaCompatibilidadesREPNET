import json
import time
from typing import Any
from redis.client import redis_client


class JobStore:
    PREFIX = "job"

    @classmethod
    def key(cls, job_id: str) -> str:
        return f"{cls.PREFIX}:{job_id}"

    @classmethod
    def create(cls, job_id: str, payload: dict[str, Any]) -> None:
        data = {
            "status": "ready",
            "message": "Excel subido correctamente. Listo para procesar.",
            "progress": 0,
            "created_at": int(time.time()),
            **payload,
        }
        redis_client.set(cls.key(job_id), json.dumps(data, ensure_ascii=False))

    @classmethod
    def get(cls, job_id: str) -> dict[str, Any] | None:
        raw = redis_client.get(cls.key(job_id))
        if not raw:
            return None
        return json.loads(raw)

    @classmethod
    def update(cls, job_id: str, **updates: Any) -> dict[str, Any] | None:
        job = cls.get(job_id)
        if not job:
            return None
        job.update(updates)
        redis_client.set(cls.key(job_id), json.dumps(job, ensure_ascii=False))
        return job

    @classmethod
    def update_progress(cls, job_id: str, progress: int, message: str | None = None) -> None:
        job = cls.get(job_id)
        if not job:
            return
        job["progress"] = max(0, min(100, int(progress)))
        if message is not None:
            job["message"] = message
        redis_client.set(cls.key(job_id), json.dumps(job, ensure_ascii=False))