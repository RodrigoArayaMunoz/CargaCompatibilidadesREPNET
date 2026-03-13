import os
import shutil
import unicodedata
import math
from typing import Any

import pandas as pd
from config import settings

os.makedirs(settings.upload_dir, exist_ok=True)

COLUMN_ALIASES = {
    "ASOCIACION ML": ["ASOCIACION ML"],
    "MARCA": ["MARCA", "Marca"],
    "MODELO": ["MODELO", "Modelo"],
    "CILINDRADA": ["CILINDRADA", "CILINDRADA ABREVIADA"],
    "TRANSMISION": ["TRANSMISION"],
    "DESDE": ["DESDE", "Desde"],
    "HASTA": ["HASTA", "Hasta"],
}

COMPAT_REQUIRED_LOGICAL_COLUMNS = [
    "ASOCIACION ML",
    "MARCA",
    "MODELO",
    "CILINDRADA",
    "TRANSMISION",
    "DESDE",
    "HASTA",
]


def save_upload_file(upload_file, destination: str) -> None:
    with open(destination, "wb") as buffer:
        shutil.copyfileobj(upload_file.file, buffer)


def load_excel_rows(xlsx_path: str, sheet_name: str = "Hoja1") -> list[dict]:
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    missing = validate_dataframe_columns(df)
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")
    if len(df.index) == 0:
        raise ValueError("El Excel no tiene filas")
    return df.to_dict(orient="records")


def validate_dataframe_columns(df: pd.DataFrame) -> list[str]:
    missing = []
    cols = set(str(c).strip() for c in df.columns)
    for logical_col in COMPAT_REQUIRED_LOGICAL_COLUMNS:
        aliases = COLUMN_ALIASES.get(logical_col, [logical_col])
        if not any(alias in cols for alias in aliases):
            missing.append(f"{logical_col} (aliases: {aliases})")
    return missing


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def normalize_for_compare(value: Any) -> str:
    text = normalize_text(value).lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("-", " ").replace("_", " ")
    text = " ".join(text.split())
    return text


def normalize_year(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
        year = int(float(value))
        return year if year > 0 else None
    except Exception:
        return None


def build_years_list(desde: Any, hasta: Any) -> list[int]:
    year_from = normalize_year(desde)
    year_to = normalize_year(hasta)

    if not year_from:
        return []
    if not year_to or year_to == 0 or year_to < year_from:
        return [year_from]
    return list(range(year_from, year_to + 1))


def normalize_engine(value: Any) -> str:
    return normalize_text(value)


def normalize_transmission(value: Any) -> str:
    text = normalize_for_compare(value)
    mapping = {
        "manual": "Manual",
        "mecanico": "Manual",
        "mecanica": "Manual",
        "mecanico manual": "Manual",
        "mecanica manual": "Manual",
        "automatico": "Automática",
        "automatica": "Automática",
        "auto": "Automática",
        "cvt": "CVT",
    }
    return mapping.get(text, normalize_text(value))


def extract_item_id(value: Any) -> str:
    return normalize_text(value).upper()


def get_row_value(row: dict, logical_name: str) -> Any:
    aliases = COLUMN_ALIASES.get(logical_name, [logical_name])
    for alias in aliases:
        if alias in row:
            return row.get(alias)
    return None