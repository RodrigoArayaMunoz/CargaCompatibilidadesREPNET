import asyncio
from dataclasses import dataclass, field
from typing import Any

from fastapi import HTTPException

from config import settings
from services.excel_service import (
    build_years_list,
    extract_item_id,
    get_row_value,
    normalize_engine,
    normalize_text,
    normalize_transmission,
    normalize_for_compare,
)
from services.job_store import JobStore
from services.ml_client import ml_client, pick_value_id_by_name


@dataclass
class JobCaches:
    item_detail: dict[str, dict] = field(default_factory=dict)
    brand: dict[str, str | None] = field(default_factory=dict)
    model: dict[tuple[str, str], str | None] = field(default_factory=dict)
    year: dict[tuple[str, str, int], str | None] = field(default_factory=dict)
    engine: dict[tuple[str, str, str, str], str | None] = field(default_factory=dict)
    transmission: dict[tuple[str, str, str, str], str | None] = field(default_factory=dict)
    product: dict[tuple[str, str, str, str | None, str | None], str | None] = field(default_factory=dict)


async def resolve_brand_id(access_token: str, brand_name: str, caches: JobCaches) -> str | None:
    key = normalize_for_compare(brand_name)
    if key in caches.brand:
        return caches.brand[key]
    values = await ml_client.get_top_values(access_token, "BRAND")
    value = pick_value_id_by_name(values, brand_name)
    caches.brand[key] = value
    return value


async def resolve_model_id(access_token: str, brand_id: str, model_name: str, caches: JobCaches) -> str | None:
    key = (brand_id, normalize_for_compare(model_name))
    if key in caches.model:
        return caches.model[key]
    values = await ml_client.get_top_values(
        access_token,
        "CAR_AND_VAN_MODEL",
        known_attributes=[{"id": "BRAND", "value_id": brand_id}],
    )
    value = pick_value_id_by_name(values, model_name)
    caches.model[key] = value
    return value


async def resolve_year_id(access_token: str, brand_id: str, model_id: str, year: int, caches: JobCaches) -> str | None:
    key = (brand_id, model_id, year)
    if key in caches.year:
        return caches.year[key]
    values = await ml_client.get_top_values(
        access_token,
        "YEAR",
        known_attributes=[
            {"id": "BRAND", "value_id": brand_id},
            {"id": "CAR_AND_VAN_MODEL", "value_id": model_id},
        ],
    )
    value = pick_value_id_by_name(values, str(year))
    caches.year[key] = value
    return value


async def resolve_engine_id(
    access_token: str,
    brand_id: str,
    model_id: str,
    year_id: str,
    engine_name: str,
    caches: JobCaches,
) -> str | None:
    if not engine_name:
        return None

    key = (brand_id, model_id, year_id, normalize_for_compare(engine_name))
    if key in caches.engine:
        return caches.engine[key]

    values = await ml_client.get_top_values(
        access_token,
        "CAR_AND_VAN_ENGINE",
        known_attributes=[
            {"id": "BRAND", "value_id": brand_id},
            {"id": "CAR_AND_VAN_MODEL", "value_id": model_id},
            {"id": "YEAR", "value_id": year_id},
        ],
    )
    value = pick_value_id_by_name(values, engine_name)
    caches.engine[key] = value
    return value


async def resolve_transmission_id(
    access_token: str,
    brand_id: str,
    model_id: str,
    year_id: str,
    transmission_name: str,
    caches: JobCaches,
) -> str | None:
    if not transmission_name:
        return None

    key = (brand_id, model_id, year_id, normalize_for_compare(transmission_name))
    if key in caches.transmission:
        return caches.transmission[key]

    values = await ml_client.get_top_values(
        access_token,
        "TRANSMISSION_CONTROL_TYPE",
        known_attributes=[
            {"id": "BRAND", "value_id": brand_id},
            {"id": "CAR_AND_VAN_MODEL", "value_id": model_id},
            {"id": "YEAR", "value_id": year_id},
        ],
    )
    value = pick_value_id_by_name(values, transmission_name)
    caches.transmission[key] = value
    return value


async def search_vehicle_product_id(
    access_token: str,
    brand_id: str,
    model_id: str,
    year_id: str,
    transmission_id: str | None,
    engine_id: str | None,
    caches: JobCaches,
) -> str | None:
    key = (brand_id, model_id, year_id, transmission_id, engine_id)
    if key in caches.product:
        return caches.product[key]

    results = await ml_client.search_vehicle_products(
        access_token=access_token,
        brand_id=brand_id,
        model_id=model_id,
        year_id=year_id,
        transmission_id=transmission_id,
        engine_id=engine_id,
    )
    value = str(results[0]["id"]) if results and results[0].get("id") else None
    caches.product[key] = value
    return value


async def get_item_detail_cached(access_token: str, item_id: str, caches: JobCaches) -> dict:
    if item_id in caches.item_detail:
        return caches.item_detail[item_id]
    data = await ml_client.get_item_detail(access_token, item_id)
    caches.item_detail[item_id] = data
    return data


def dedup_key(row: dict) -> tuple:
    item_id = extract_item_id(get_row_value(row, "ASOCIACION ML"))
    brand_name = normalize_for_compare(get_row_value(row, "MARCA"))
    model_name = normalize_for_compare(get_row_value(row, "MODELO"))
    engine_name = normalize_for_compare(get_row_value(row, "CILINDRADA"))
    transmission_name = normalize_for_compare(get_row_value(row, "TRANSMISION"))
    years = tuple(build_years_list(get_row_value(row, "DESDE"), get_row_value(row, "HASTA")))
    return (item_id, brand_name, model_name, engine_name, transmission_name, years)


async def process_vehicle_row(
    access_token: str,
    row: dict,
    caches: JobCaches,
) -> dict:
    item_id = extract_item_id(get_row_value(row, "ASOCIACION ML"))
    brand_name = normalize_text(get_row_value(row, "MARCA"))
    model_name = normalize_text(get_row_value(row, "MODELO"))
    engine_name = normalize_engine(get_row_value(row, "CILINDRADA"))
    transmission_name = normalize_transmission(get_row_value(row, "TRANSMISION"))
    years = build_years_list(get_row_value(row, "DESDE"), get_row_value(row, "HASTA"))

    if not item_id:
        return {"ok": False, "reason": "Fila sin ASOCIACION ML", "results": []}

    if not brand_name or not model_name or not years:
        return {
            "ok": False,
            "item_id": item_id,
            "reason": "Faltan datos mínimos: MARCA / MODELO / DESDE",
            "results": [],
        }

    item_detail = await get_item_detail_cached(access_token, item_id, caches)
    category_id = item_detail.get("category_id")
    user_product_id = item_detail.get("user_product_id")

    if not category_id:
        return {"ok": False, "item_id": item_id, "reason": "El item no devolvió category_id", "results": []}
    if not user_product_id:
        return {"ok": False, "item_id": item_id, "reason": "El item no devolvió user_product_id", "results": []}

    brand_id = await resolve_brand_id(access_token, brand_name, caches)
    if not brand_id:
        return {
            "ok": False,
            "item_id": item_id,
            "user_product_id": user_product_id,
            "category_id": category_id,
            "reason": f"No se encontró BRAND para '{brand_name}'",
            "results": [],
        }

    model_id = await resolve_model_id(access_token, brand_id, model_name, caches)
    if not model_id:
        return {
            "ok": False,
            "item_id": item_id,
            "user_product_id": user_product_id,
            "category_id": category_id,
            "reason": f"No se encontró MODEL para '{model_name}'",
            "results": [],
        }

    results = []

    for year in years:
        try:
            year_id = await resolve_year_id(access_token, brand_id, model_id, year, caches)
            if not year_id:
                results.append({"ok": False, "year": year, "reason": f"No se encontró YEAR para '{year}'"})
                continue

            engine_id = await resolve_engine_id(access_token, brand_id, model_id, year_id, engine_name, caches)
            if engine_name and not engine_id:
                results.append({"ok": False, "year": year, "reason": f"No se encontró ENGINE para '{engine_name}'"})
                continue

            transmission_id = await resolve_transmission_id(
                access_token,
                brand_id,
                model_id,
                year_id,
                transmission_name,
                caches,
            )
            if transmission_name and not transmission_id:
                results.append({"ok": False, "year": year, "reason": f"No se encontró TRANSMISSION para '{transmission_name}'"})
                continue

            product_id = await search_vehicle_product_id(
                access_token,
                brand_id,
                model_id,
                year_id,
                transmission_id,
                engine_id,
                caches,
            )
            if not product_id:
                results.append({"ok": False, "year": year, "reason": "No se encontró product_id"})
                continue

            ml_response = await ml_client.add_user_product_compatibility(
                access_token=access_token,
                user_product_id=str(user_product_id),
                category_id=str(category_id),
                product_id=product_id,
                creation_source="DEFAULT",
            )
            results.append({
                "ok": True,
                "year": year,
                "product_id": product_id,
                "ml_response": ml_response,
            })

        except HTTPException as exc:
            results.append({"ok": False, "year": year, "reason": f"HTTPException: {exc.detail}"})
        except Exception as exc:
            results.append({"ok": False, "year": year, "reason": f"Exception: {str(exc)}"})

    success_results = [r for r in results if r.get("ok")]
    error_results = [r for r in results if not r.get("ok")]

    return {
        "ok": len(success_results) > 0,
        "item_id": item_id,
        "brand_name": brand_name,
        "model_name": model_name,
        "engine_name": engine_name,
        "transmission_name": transmission_name,
        "user_product_id": user_product_id,
        "category_id": category_id,
        "years_requested": years,
        "years_processed": len(results),
        "success_count": len(success_results),
        "error_count": len(error_results),
        "results": results,
    }


async def process_rows_for_job(
    job_id: str,
    access_token: str,
    rows: list[dict],
) -> dict:
    caches = JobCaches()
    semaphore = asyncio.Semaphore(settings.max_row_concurrency)

    unique_map: dict[tuple, list[int]] = {}
    unique_rows: list[dict] = []

    for idx, row in enumerate(rows):
        key = dedup_key(row)
        if key not in unique_map:
            unique_map[key] = []
            unique_rows.append(row)
        unique_map[key].append(idx)

    completed = 0
    row_results_unique: list[dict] = [None] * len(unique_rows)  # type: ignore

    async def worker(pos: int, row: dict):
        nonlocal completed
        async with semaphore:
            result = await process_vehicle_row(access_token, row, caches)
            row_results_unique[pos] = result
            completed += 1
            progress = 10 + int((completed / max(len(unique_rows), 1)) * 85)
            JobStore.update_progress(
                job_id,
                progress,
                f"Procesadas {completed}/{len(unique_rows)} filas únicas",
            )

    await asyncio.gather(*(worker(i, row) for i, row in enumerate(unique_rows)))

    final_results = [None] * len(rows)
    for unique_pos, row in enumerate(unique_rows):
        key = dedup_key(row)
        for original_idx in unique_map[key]:
            final_results[original_idx] = row_results_unique[unique_pos]

    rows_ok = sum(1 for r in final_results if r.get("ok"))
    rows_error = len(final_results) - rows_ok

    compat_total = 0
    compat_ok = 0
    compat_error = 0
    for r in final_results:
        details = r.get("results", [])
        compat_total += len(details)
        compat_ok += sum(1 for d in details if d.get("ok"))
        compat_error += sum(1 for d in details if not d.get("ok"))

    return {
        "results": final_results,
        "summary": {
            "processed_rows": len(rows),
            "unique_rows": len(unique_rows),
            "success_count": rows_ok,
            "error_count": rows_error,
            "compatibilities_total": compat_total,
            "compatibilities_ok": compat_ok,
            "compatibilities_error": compat_error,
        },
    }