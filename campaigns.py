"""
Daraz Campaign Manager blueprint for tb_track.

Workflow:
  1. Seller downloads the campaign Excel template from Daraz Seller Center.
  2. Uploads it here -> parsed, original xlsx kept on disk untouched.
  3. UI shows a friendly grid: English product name + image + color, current
     campaign price, recommended-price range, min-price (cost) floor, stock.
  4. Seller edits campaign prices (with validation + bulk tools).
  5. Downloads -> we mutate only the 'Campaign Price' column in the original
     workbook and stream it back. Daraz accepts it because every other cell is
     byte-for-byte preserved.
"""
from __future__ import annotations

import json
import os
import re
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import shopify
from flask import Blueprint, abort, jsonify, render_template, request, send_file
from openpyxl import load_workbook

BASE_DIR = Path(os.getenv("CAMPAIGN_DIR", "campaign_uploads")).resolve()


def init_campaign_dirs() -> None:
    BASE_DIR.mkdir(parents=True, exist_ok=True)


COL_SELLER_SKU = 0
COL_SKU_ID = 1
COL_SHOP_SKU = 2
COL_PRODUCT_NAME = 3
COL_PRODUCT_URL = 4
COL_SALES_PRICE = 5
COL_CAMPAIGN_PRICE = 6
COL_RECOMMENDED = 9
COL_STOCK = 10
COL_IS_HERO = 11
COL_CATEGORY = 16

DATA_START_ROW = 2
RECOMMENDED_PATTERN = re.compile(r"<\s*=?\s*([\d.]+)")


def parse_recommended_max(cell_value: Any) -> float | None:
    if cell_value is None:
        return None
    match = RECOMMENDED_PATTERN.search(str(cell_value))
    return float(match.group(1)) if match else None


_shopify_cache: dict[str, dict[str, Any]] = {}
_shopify_lookup_lock_seconds = 0.6


def _split_seller_sku(seller_sku: str) -> tuple[str, int | None]:
    if not seller_sku:
        return "", None
    if "-" in seller_sku:
        base, _, tail = seller_sku.rpartition("-")
        if tail.isdigit():
            return base, int(tail)
    return seller_sku, None


def enrich_from_shopify(seller_sku: str) -> dict[str, Any]:
    if not seller_sku:
        return {}

    base, variant_idx = _split_seller_sku(seller_sku)
    if seller_sku in _shopify_cache:
        return _shopify_cache[seller_sku]

    fallback = {"title": None, "image": None, "color": None, "variant_title": None}
    try:
        time.sleep(_shopify_lookup_lock_seconds)
        products = shopify.Product.find(limit=5, sku=seller_sku)
        product = None
        matched_variant = None

        if products:
            product = products[0]
            for variant in product.variants or []:
                if str(variant.sku) == seller_sku:
                    matched_variant = variant
                    break

        if product is None:
            time.sleep(_shopify_lookup_lock_seconds)
            products = shopify.Product.find(limit=5, sku=base)
            if products:
                product = products[0]
                if variant_idx is not None and product.variants:
                    if 0 <= variant_idx < len(product.variants):
                        matched_variant = product.variants[variant_idx]
                if matched_variant is None and product.variants:
                    matched_variant = product.variants[0]

        if product is None:
            _shopify_cache[seller_sku] = fallback
            return fallback

        image_src = None
        if matched_variant and matched_variant.image_id:
            try:
                time.sleep(_shopify_lookup_lock_seconds)
                images = shopify.Image.find(image_id=matched_variant.image_id, product_id=product.id)
                for image in images:
                    if image.id == matched_variant.image_id:
                        image_src = image.src
                        break
            except Exception as e:
                print(f"[campaigns] image fetch failed for {seller_sku}: {e}")

        if image_src is None and product.image:
            image_src = product.image.src

        variant_title = getattr(matched_variant, "title", None) if matched_variant else None
        color = None
        if variant_title and variant_title != "Default Title":
            color = variant_title.split(" / ")[0]

        result = {
            "title": product.title,
            "image": image_src,
            "color": color,
            "variant_title": variant_title,
        }
        _shopify_cache[seller_sku] = result
        return result
    except Exception as e:
        print(f"[campaigns] Shopify lookup failed for {seller_sku}: {e}")
        _shopify_cache[seller_sku] = fallback
        return fallback


def _campaign_paths(campaign_id: str) -> tuple[Path, Path]:
    safe = re.sub(r"[^a-zA-Z0-9_\-]", "", campaign_id)
    if not safe:
        abort(400, "Invalid campaign id")
    return BASE_DIR / f"{safe}.xlsx", BASE_DIR / f"{safe}.meta.json"


def _load_meta(campaign_id: str) -> dict[str, Any]:
    _, meta_path = _campaign_paths(campaign_id)
    if not meta_path.exists():
        abort(404, "Campaign not found")
    with open(meta_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _save_meta(campaign_id: str, meta: dict[str, Any]) -> None:
    _, meta_path = _campaign_paths(campaign_id)
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(meta, handle, ensure_ascii=False, indent=2)


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _to_int(value: Any) -> int | None:
    numeric = _to_float(value)
    return int(numeric) if numeric is not None else None


def _read_template_rows(xlsx_path: Path) -> list[dict[str, Any]]:
    workbook = load_workbook(xlsx_path, data_only=True)
    worksheet = workbook[workbook.sheetnames[0]]
    rows: list[dict[str, Any]] = []

    for row_index, row in enumerate(worksheet.iter_rows(values_only=True)):
        if row_index < DATA_START_ROW:
            continue
        if not row or row[COL_SELLER_SKU] in (None, ""):
            continue

        seller_sku = str(row[COL_SELLER_SKU]).strip()
        sales_price = _to_float(row[COL_SALES_PRICE])
        campaign_price = _to_float(row[COL_CAMPAIGN_PRICE])
        rec_max = parse_recommended_max(row[COL_RECOMMENDED])

        rows.append(
            {
                "row_index": row_index,
                "seller_sku": seller_sku,
                "sku_id": str(row[COL_SKU_ID]) if row[COL_SKU_ID] else "",
                "shop_sku": str(row[COL_SHOP_SKU]) if row[COL_SHOP_SKU] else "",
                "daraz_name": row[COL_PRODUCT_NAME] or "",
                "product_url": row[COL_PRODUCT_URL] or "",
                "sales_price": sales_price,
                "campaign_price": campaign_price,
                "campaign_price_original": campaign_price,
                "recommended_max": rec_max,
                "recommended_raw": str(row[COL_RECOMMENDED] or ""),
                "stock": _to_int(row[COL_STOCK]),
                "is_hero": (str(row[COL_IS_HERO] or "N").upper() == "Y"),
                "category": row[COL_CATEGORY] or "",
            }
        )

    return rows


campaigns_bp = Blueprint(
    "campaigns",
    __name__,
    template_folder="templates",
    static_folder="static",
    url_prefix="/campaigns",
)


@campaigns_bp.route("/")
def list_campaigns():
    items = []
    for meta_file in sorted(BASE_DIR.glob("*.meta.json"), key=os.path.getmtime, reverse=True):
        try:
            with open(meta_file, "r", encoding="utf-8") as handle:
                meta = json.load(handle)
            items.append(
                {
                    "id": meta_file.stem.replace(".meta", ""),
                    "name": meta.get("name", "Untitled"),
                    "uploaded_at": meta.get("uploaded_at", ""),
                    "original_filename": meta.get("original_filename", ""),
                    "row_count": len(meta.get("rows", [])),
                    "edited_count": sum(
                        1
                        for row in meta.get("rows", [])
                        if row.get("campaign_price") != row.get("campaign_price_original")
                    ),
                }
            )
        except Exception as e:
            print(f"[campaigns] could not load {meta_file}: {e}")

    return render_template("campaigns.html", view="list", campaigns=items)


@campaigns_bp.route("/upload", methods=["POST"])
def upload_campaign():
    uploaded_file = request.files.get("file")
    name = (request.form.get("name") or "").strip() or "Untitled Campaign"
    if not uploaded_file or not uploaded_file.filename.lower().endswith(".xlsx"):
        return jsonify({"error": "Please upload a .xlsx Daraz campaign template"}), 400

    campaign_id = uuid.uuid4().hex[:12]
    xlsx_path, _ = _campaign_paths(campaign_id)
    uploaded_file.save(xlsx_path)

    try:
        rows = _read_template_rows(xlsx_path)
    except Exception as e:
        xlsx_path.unlink(missing_ok=True)
        return jsonify({"error": f"Could not parse template: {e}"}), 400

    state_rows = []
    for row in rows:
        state_rows.append(
            {
                **row,
                "min_price": 0,
                "campaign_price": row["campaign_price"],
                "enrichment": None,
            }
        )

    meta = {
        "name": name,
        "original_filename": uploaded_file.filename,
        "uploaded_at": datetime.now().isoformat(timespec="seconds"),
        "rows": state_rows,
    }
    _save_meta(campaign_id, meta)
    return jsonify({"id": campaign_id, "redirect": f"/campaigns/{campaign_id}"})


@campaigns_bp.route("/<campaign_id>")
def edit_campaign(campaign_id: str):
    meta = _load_meta(campaign_id)
    return render_template("campaigns.html", view="edit", campaign_id=campaign_id, meta=meta)


@campaigns_bp.route("/<campaign_id>/data")
def campaign_data(campaign_id: str):
    meta = _load_meta(campaign_id)
    return jsonify(meta)


@campaigns_bp.route("/<campaign_id>/enrich", methods=["POST"])
def enrich_campaign(campaign_id: str):
    meta = _load_meta(campaign_id)
    body = request.get_json(silent=True) or {}
    requested = body.get("seller_skus")
    batch_size = int(body.get("batch_size", 10))

    enriched_now: dict[str, dict[str, Any]] = {}
    to_process: list[str] = []

    if requested:
        to_process = [sku for sku in requested if sku]
    else:
        for row in meta["rows"]:
            if row.get("enrichment") is None:
                to_process.append(row["seller_sku"])
                if len(to_process) >= batch_size:
                    break

    seen = set()
    to_process = [sku for sku in to_process if not (sku in seen or seen.add(sku))]

    for sku in to_process:
        enriched_now[sku] = enrich_from_shopify(sku)

    for row in meta["rows"]:
        if row["seller_sku"] in enriched_now:
            row["enrichment"] = enriched_now[row["seller_sku"]]

    _save_meta(campaign_id, meta)
    remaining = sum(1 for row in meta["rows"] if row.get("enrichment") is None)
    return jsonify({"enriched": enriched_now, "remaining": remaining})


@campaigns_bp.route("/<campaign_id>/update", methods=["POST"])
def update_prices(campaign_id: str):
    meta = _load_meta(campaign_id)
    body = request.get_json(silent=True) or {}
    updates = {u["seller_sku"]: u for u in body.get("updates", []) if u.get("seller_sku")}
    if not updates:
        return jsonify({"updated": 0})

    changed = 0
    for row in meta["rows"]:
        update = updates.get(row["seller_sku"])
        if not update:
            continue
        if "campaign_price" in update:
            value = _to_float(update["campaign_price"])
            if value is not None:
                row["campaign_price"] = value
                changed += 1
        if "min_price" in update:
            value = _to_float(update["min_price"])
            if value is not None:
                row["min_price"] = value

    _save_meta(campaign_id, meta)
    return jsonify({"updated": changed})


@campaigns_bp.route("/<campaign_id>/bulk", methods=["POST"])
def bulk_apply(campaign_id: str):
    meta = _load_meta(campaign_id)
    body = request.get_json(silent=True) or {}
    action = body.get("action")
    params = body.get("params") or {}
    target = set(body.get("seller_skus") or [])

    def applies(row):
        return not target or row["seller_sku"] in target

    changed = 0
    for row in meta["rows"]:
        if not applies(row):
            continue
        new_price = row["campaign_price"]
        if action == "set_to_recommended_max" and row.get("recommended_max"):
            new_price = row["recommended_max"]
        elif action == "percent_off_sales":
            pct = float(params.get("pct", 0))
            if row.get("sales_price"):
                new_price = round(row["sales_price"] * (1 - pct / 100.0), 2)
        elif action == "flat_amount_off_sales":
            amount = float(params.get("amount", 0))
            if row.get("sales_price"):
                new_price = round(row["sales_price"] - amount, 2)
        elif action == "set_min_to_percent_of_sales":
            pct = float(params.get("pct", 0))
            if row.get("sales_price"):
                row["min_price"] = round(row["sales_price"] * pct / 100.0, 2)
                changed += 1
            continue
        else:
            continue

        if row.get("recommended_max") and new_price > row["recommended_max"]:
            new_price = row["recommended_max"]
        if row.get("min_price") and new_price < row["min_price"]:
            new_price = row["min_price"]
        row["campaign_price"] = new_price
        changed += 1

    _save_meta(campaign_id, meta)
    return jsonify({"updated": changed})


@campaigns_bp.route("/<campaign_id>/download")
def download_campaign(campaign_id: str):
    import re as regex
    import shutil
    import zipfile

    meta = _load_meta(campaign_id)
    xlsx_path, _ = _campaign_paths(campaign_id)
    if not xlsx_path.exists():
        abort(404, "Source template missing")

    edits = {
        row["row_index"] + 1: row["campaign_price"]
        for row in meta["rows"]
        if row.get("campaign_price") is not None
    }

    out_path = BASE_DIR / f"{campaign_id}.download.xlsx"
    shutil.copy(xlsx_path, out_path)

    with zipfile.ZipFile(out_path) as zin:
      sheet_xml = zin.read("xl/worksheets/sheet1.xml").decode("utf-8")
      other_files = {name: zin.read(name) for name in zin.namelist() if name != "xl/worksheets/sheet1.xml"}

    def format_price(price):
        if isinstance(price, (int, float)):
            if float(price).is_integer():
                return str(int(price))
            return f"{price:.2f}".rstrip("0").rstrip(".")
        return str(price)

    for row_num, price in edits.items():
        price_str = format_price(price)
        price_xml = price_str.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        new_cell = f'<c r="G{row_num}" t="inlineStr"><is><t>{price_xml}</t></is></c>'
        pattern = regex.compile(rf'<c r="G{row_num}"[^>]*>.*?</c>', regex.DOTALL)
        sheet_xml, n_subs = pattern.subn(new_cell, sheet_xml, count=1)
        if n_subs == 0:
            print(f"[campaigns] warning: could not find G{row_num} cell to replace")

    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as zout:
        for name, data in other_files.items():
            zout.writestr(name, data)
        zout.writestr("xl/worksheets/sheet1.xml", sheet_xml)

    download_name = f"{meta.get('name', 'campaign').replace(' ', '_')}_filled.xlsx"
    return send_file(
        out_path,
        as_attachment=True,
        download_name=download_name,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@campaigns_bp.route("/<campaign_id>", methods=["DELETE"])
def delete_campaign(campaign_id: str):
    xlsx_path, meta_path = _campaign_paths(campaign_id)
    xlsx_path.unlink(missing_ok=True)
    meta_path.unlink(missing_ok=True)
    (BASE_DIR / f"{campaign_id}.download.xlsx").unlink(missing_ok=True)
    return jsonify({"deleted": True})
