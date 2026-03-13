import json
import uuid
from typing import Optional

import redis
from config import settings


class JobStore:
    _client = redis.Redis.from_url(settings.redis_url, decode_responses=True)
    _ttl_seconds = 7 * 24 * 60 * 60  # 7 días

    @classmethod
    def _key(cls, job_id: str) -> str:
        return f"job:{job_id}"

    @classmethod
    def create(cls, filename: str) -> dict:
        job_id = str(uuid.uuid4())
        data = {
            "id": job_id,
            "filename": filename,
            "status": "uploaded",
            "message": "Archivo cargado correctamente",
            "progress": 0,
            "xlsx_path": None,
            "total_rows": 0,
            "processed_rows": 0,
            "result_path": None,
            "summary": {},
            "task_id": None,
        }

        cls._client.set(
            cls._key(job_id),
            json.dumps(data, ensure_ascii=False),
            ex=cls._ttl_seconds,
        )
        return data

    @classmethod
    def get(cls, job_id: str) -> Optional[dict]:
        raw = cls._client.get(cls._key(job_id))
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None

    @classmethod
    def update(cls, job_id: str, **kwargs) -> None:
        key = cls._key(job_id)
        raw = cls._client.get(key)
        if not raw:
            return

        try:
            job = json.loads(raw)
        except json.JSONDecodeError:
            return

        job.update(kwargs)

        cls._client.set(
            key,
            json.dumps(job, ensure_ascii=False),
            ex=cls._ttl_seconds,
        )

    @classmethod
    def update_progress(cls, job_id: str, progress: int, message: str) -> None:
        progress = max(0, min(100, int(progress)))
        cls.update(job_id, progress=progress, message=message)

    @classmethod
    def delete(cls, job_id: str) -> None:
        cls._client.delete(cls._key(job_id))