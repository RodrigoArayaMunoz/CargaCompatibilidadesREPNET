from pydantic import BaseModel


class JobResponse(BaseModel):
    job_id: str
    status: str
    message: str
    progress: int