"""
SBI Statement Parser API

FastAPI server that accepts SBI statement PDFs and returns parsed transactions.

Endpoints:
    POST /parse          - Upload PDF, get transactions as JSON array
    POST /parse-and-save - Upload PDF, parse, merge into master CSV, return new transactions
    GET  /transactions   - Get all transactions from master CSV

Usage:
    uvicorn api:app --host 0.0.0.0 --port 8000
"""

import logging
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, File, UploadFile, HTTPException, Query
from fastapi.responses import JSONResponse

from parse_sbi_statement import (
    parse_pdf,
    compute_hash,
    load_existing_csv,
    write_master_csv,
    load_password,
    MASTER_CSV,
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="SBI Statement Parser",
    description="Parse SBI bank statement PDFs and extract transactions",
    version="1.0.0",
)

# Load password once at startup
PASSWORD = load_password()

# Lock for CSV read-modify-write in /parse-and-save
_csv_lock = threading.Lock()

MAX_PDF_SIZE = 50 * 1024 * 1024  # 50 MB


def _validate_pdf(file_bytes: bytes, filename: str):
    """Validate file extension and PDF magic bytes."""
    if not filename.lower().endswith(".pdf"):
        raise HTTPException(400, "File must be a PDF")
    if len(file_bytes) > MAX_PDF_SIZE:
        raise HTTPException(400, f"File too large. Maximum size is {MAX_PDF_SIZE // (1024*1024)} MB")
    if not file_bytes[:5].startswith(b"%PDF-"):
        raise HTTPException(400, "File does not appear to be a valid PDF")


def _parse_uploaded_pdf(file_bytes: bytes, filename: str):
    """Save uploaded bytes to a temp file and parse it."""
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name

    try:
        transactions, stmt_from, stmt_to, page_count = parse_pdf(tmp_path, PASSWORD)
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    return transactions, stmt_from, stmt_to, page_count


def _txn_to_dict(txn):
    """Convert transaction to the API response format."""
    h = txn.get("hash", "")
    return {
        "txn_id": h[:16],
        "value_date": txn.get("value_date", ""),
        "post_date": txn.get("post_date", ""),
        "details": txn.get("details", ""),
        "ref_no": txn.get("ref_no", ""),
        "debit": txn.get("debit", ""),
        "credit": txn.get("credit", ""),
        "balance": txn.get("balance", ""),
        "txn_type": txn.get("txn_type", ""),
        "account_source": txn.get("account_source", "sbi_email"),
        "imported_at": txn.get("imported_at", ""),
        "hash": h,
    }


@app.post("/parse")
async def parse_statement(file: UploadFile = File(...)):
    """Upload a PDF, get parsed transactions back as a JSON array."""
    file_bytes = await file.read()
    _validate_pdf(file_bytes, file.filename)

    try:
        transactions, stmt_from, stmt_to, page_count = _parse_uploaded_pdf(
            file_bytes, file.filename
        )
    except RuntimeError as e:
        raise HTTPException(422, str(e))
    except Exception:
        logger.exception("Unexpected parse error for file: %s", file.filename)
        raise HTTPException(500, "Internal server error during parsing")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    for txn in transactions:
        txn["hash"] = compute_hash(txn)
        txn["imported_at"] = now

    return JSONResponse(content=[_txn_to_dict(t) for t in transactions])


@app.post("/parse-and-save")
async def parse_and_save(file: UploadFile = File(...)):
    """Upload a PDF, parse it, merge new transactions into master CSV.

    Returns only the NEW transactions that were added.
    """
    file_bytes = await file.read()
    _validate_pdf(file_bytes, file.filename)

    try:
        transactions, stmt_from, stmt_to, page_count = _parse_uploaded_pdf(
            file_bytes, file.filename
        )
    except RuntimeError as e:
        raise HTTPException(422, str(e))
    except Exception:
        logger.exception("Unexpected parse error for file: %s", file.filename)
        raise HTTPException(500, "Internal server error during parsing")

    with _csv_lock:
        existing, existing_hashes = load_existing_csv(MASTER_CSV)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        seq_offset = len(existing)

        new_txns = []
        dup_count = 0
        for txn in transactions:
            h = compute_hash(txn)
            if h in existing_hashes:
                dup_count += 1
            else:
                txn["hash"] = h
                txn["imported_at"] = now
                txn["txn_id"] = 0
                txn["_parse_seq"] = txn["_parse_seq"] + seq_offset
                new_txns.append(txn)
                existing_hashes.add(h)

        if new_txns:
            merged = existing + new_txns
            write_master_csv(merged, MASTER_CSV)
            total = len(merged)
        else:
            total = len(existing)

    return {
        "filename": file.filename,
        "pages": page_count,
        "period": {"from": stmt_from, "to": stmt_to},
        "parsed": len(transactions),
        "new": len(new_txns),
        "duplicates_skipped": dup_count,
        "total_in_csv": total,
        "new_transactions": [_txn_to_dict(t) for t in new_txns],
    }


def _safe_parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%d/%m/%Y")
    except (ValueError, TypeError):
        return None


@app.get("/transactions")
async def get_transactions(
    from_date: str = Query(None, description="Filter from date (DD/MM/YYYY)"),
    to_date: str = Query(None, description="Filter to date (DD/MM/YYYY)"),
    txn_type: str = Query(None, description="Filter by debit or credit"),
    limit: int = Query(None, ge=1, description="Limit number of results"),
    offset: int = Query(0, ge=0, description="Skip first N results"),
):
    """Get transactions from master CSV with optional filters."""
    if from_date:
        try:
            fd = datetime.strptime(from_date, "%d/%m/%Y")
        except ValueError:
            raise HTTPException(400, "from_date must be DD/MM/YYYY")

    if to_date:
        try:
            td = datetime.strptime(to_date, "%d/%m/%Y")
        except ValueError:
            raise HTTPException(400, "to_date must be DD/MM/YYYY")

    if not MASTER_CSV.exists():
        return JSONResponse(content=[])

    existing, _ = load_existing_csv(MASTER_CSV)

    filtered = existing
    if from_date:
        filtered = [
            r for r in filtered
            if (d := _safe_parse_date(r["post_date"])) is not None and d >= fd
        ]

    if to_date:
        filtered = [
            r for r in filtered
            if (d := _safe_parse_date(r["post_date"])) is not None and d <= td
        ]

    if txn_type:
        filtered = [r for r in filtered if r.get("txn_type", "").lower() == txn_type.lower()]

    filtered = filtered[offset:]
    if limit is not None:
        filtered = filtered[:limit]

    return JSONResponse(content=[_txn_to_dict(r) for r in filtered])


@app.get("/health")
async def health():
    csv_exists = MASTER_CSV.exists()
    count = 0
    if csv_exists:
        with open(MASTER_CSV, "r", encoding="utf-8-sig") as f:
            count = sum(1 for _ in f) - 1
            count = max(count, 0)
    return {
        "status": "ok",
        "csv_exists": csv_exists,
        "transaction_count": count,
    }
