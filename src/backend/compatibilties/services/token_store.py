import time
from typing import Any

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from models import MLToken


class TokenStore:
    @staticmethod
    async def get(db: AsyncSession) -> dict[str, Any] | None:
        result = await db.execute(select(MLToken).where(MLToken.id == 1))
        row = result.scalar_one_or_none()
        if not row:
            return None

        return {
            "access_token": row.access_token,
            "refresh_token": row.refresh_token,
            "token_type": row.token_type,
            "scope": row.scope,
            "user_id": row.user_id,
            "expires_in": row.expires_in,
            "expires_at": row.expires_at,
        }

    @staticmethod
    async def set(db: AsyncSession, token_data: dict[str, Any]) -> None:
        result = await db.execute(select(MLToken).where(MLToken.id == 1))
        row = result.scalar_one_or_none()

        if row is None:
            row = MLToken(
                id=1,
                access_token=token_data["access_token"],
                refresh_token=token_data["refresh_token"],
                token_type=token_data.get("token_type"),
                scope=token_data.get("scope"),
                user_id=token_data.get("user_id"),
                expires_in=token_data.get("expires_in"),
                expires_at=token_data["expires_at"],
            )
            db.add(row)
        else:
            row.access_token = token_data["access_token"]
            row.refresh_token = token_data["refresh_token"]
            row.token_type = token_data.get("token_type")
            row.scope = token_data.get("scope")
            row.user_id = token_data.get("user_id")
            row.expires_in = token_data.get("expires_in")
            row.expires_at = token_data["expires_at"]

        await db.commit()

    @staticmethod
    async def remove(db: AsyncSession) -> None:
        result = await db.execute(select(MLToken).where(MLToken.id == 1))
        row = result.scalar_one_or_none()
        if row:
            await db.delete(row)
            await db.commit()

    @staticmethod
    def build_payload(token_data: dict[str, Any]) -> dict[str, Any]:
        expires_in = int(token_data.get("expires_in", 0))
        token_data["expires_at"] = int(time.time()) + expires_in - 60
        return token_data


token_store = TokenStore()


def require_ml_env() -> None:
    if not settings.ml_client_id:
        raise HTTPException(status_code=500, detail="Falta ML_CLIENT_ID")
    if not settings.ml_client_secret:
        raise HTTPException(status_code=500, detail="Falta ML_CLIENT_SECRET")
    if not settings.ml_redirect_uri:
        raise HTTPException(status_code=500, detail="Falta ML_REDIRECT_URI")