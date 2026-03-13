import json
import os
import time
from typing import Any
from fastapi import HTTPException
from config import settings


class TokenStore:
    def __init__(self, path: str):
        self.path = path

    def load(self) -> dict[str, Any]:
        try:
            if not os.path.exists(self.path):
                with open(self.path, "w", encoding="utf-8") as f:
                    json.dump({}, f, ensure_ascii=False, indent=2)
                return {}
            with open(self.path, "r", encoding="utf-8") as f:
                raw = f.read().strip()
                return json.loads(raw) if raw else {}
        except Exception:
            return {}

    def save(self, tokens: dict[str, Any]) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(tokens, f, ensure_ascii=False, indent=2)

    def get(self, user_id: int | str) -> dict[str, Any] | None:
        return self.load().get(str(user_id))

    def set(self, user_id: int | str, token_data: dict[str, Any]) -> None:
        all_tokens = self.load()
        all_tokens[str(user_id)] = token_data
        self.save(all_tokens)

    def remove(self, user_id: int | str) -> None:
        all_tokens = self.load()
        all_tokens.pop(str(user_id), None)
        self.save(all_tokens)

    def first_user_id(self) -> str | None:
        all_tokens = self.load()
        return next(iter(all_tokens.keys()), None)

    @staticmethod
    def build_payload(token_data: dict[str, Any], user_id: int | str) -> dict[str, Any]:
        expires_in = int(token_data.get("expires_in", 0))
        token_data["user_id"] = int(user_id)
        token_data["expires_at"] = int(time.time()) + expires_in - 60
        return token_data


token_store = TokenStore(settings.tokens_file)


def require_ml_env() -> None:
    if not settings.ml_client_id:
        raise HTTPException(status_code=500, detail="Falta ML_CLIENT_ID")
    if not settings.ml_client_secret:
        raise HTTPException(status_code=500, detail="Falta ML_CLIENT_SECRET")
    if not settings.ml_redirect_uri:
        raise HTTPException(status_code=500, detail="Falta ML_REDIRECT_URI")