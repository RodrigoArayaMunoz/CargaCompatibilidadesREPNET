from celery import Celery
from config import settings

celery_app = Celery(
    "compat_worker",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["tasks.import_tasks"],
)

celery_app.conf.update(
    task_track_started=True,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    task_time_limit=60 * 60,
    task_soft_time_limit=55 * 60,
    timezone="America/Santiago",
    enable_utc=True,
    broker_connection_retry_on_startup=True,
    result_expires=24 * 60 * 60,
    task_reject_on_worker_lost=True,
)