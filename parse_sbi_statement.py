"""
SBI Bank Statement PDF to CSV Parser

Parses password-protected SBI savings account statements and maintains
a single cumulative CSV for Google Sheets.

- Extracts table rows exactly as they appear in the PDF.
- Existing data is never overwritten or duplicated (hash-based dedup).
- New transactions are merged and sorted by date ascending.

Usage:
    python parse_sbi_statement.py statement.pdf
    python parse_sbi_statement.py jan.pdf feb.pdf mar.pdf
    python parse_sbi_statement.py                (auto-finds PDFs in Downloads)

Output:  D:/finance/SBI_Transactions.csv  (single cumulative file)
"""

import sys
import csv
import hashlib
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
import os
import pdfplumber


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DOWNLOADS_DIR = Path.home() / "Downloads"
MASTER_CSV = Path(__file__).parent / "SBI_Transactions.csv"

COL_TXN_DATE = 0
COL_VALUE_DATE = 1
COL_DESCRIPTION = 2
COL_CHEQUE_NO = 3
COL_DEBIT = 4
COL_CREDIT = 5
COL_BALANCE = 6
MIN_COLS = 7

CSV_FIELDS = [
    "txn_id", "value_date", "post_date", "details", "ref_no",
    "debit", "credit", "balance", "txn_type", "account_source",
    "imported_at", "hash",
]


def load_password():
    env_path = Path(__file__).parent / ".env"
    load_dotenv(env_path)
    password = os.getenv("PDF_PASSWORD")
    if not password:
        raise RuntimeError("PDF_PASSWORD not set in .env")
    return password


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_amount(value):
    if not value or value.strip() == "-":
        return ""
    cleaned = value.strip().replace(",", "")
    try:
        float(cleaned)
    except ValueError:
        return ""
    return cleaned


def is_date(text):
    if not text:
        return False
    try:
        datetime.strptime(text.strip(), "%d/%m/%Y")
        return True
    except ValueError:
        return False


def is_transaction_row(row):
    if not row or len(row) < MIN_COLS:
        return False
    return is_date(row[COL_TXN_DATE])


def is_summary_row(row):
    if not row:
        return False
    first = str(row[0]) if row[0] else ""
    return "Statement Summary" in first or "Brought Forward" in first


def extract_statement_period(pdf):
    text = pdf.pages[0].extract_text() or ""
    match = re.search(
        r"Statement\s+From\s*:\s*(\d{2}-\d{2}-\d{4})\s+to\s+(\d{2}-\d{2}-\d{4})",
        text, re.IGNORECASE,
    )
    if match:
        return match.group(1), match.group(2)
    return None, None


# ---------------------------------------------------------------------------
# Description helpers
# ---------------------------------------------------------------------------

def extract_ref_number(desc):
    if not desc:
        return ""
    for line in desc.split("\n"):
        match = re.match(r"^(\d{10,13})\b", line.strip())
        if match:
            return match.group(1)
    return ""


def clean_description(desc):
    if not desc:
        return ""
    cleaned = desc.replace("\n", " | ")
    return re.sub(r"\s+", " ", cleaned).strip()


# ---------------------------------------------------------------------------
# PDF parsing
# ---------------------------------------------------------------------------

def parse_pdf(pdf_path, password):
    """Parse all transaction rows from an SBI statement PDF.

    Returns list of dicts, one per table row, exactly as extracted.
    """
    transactions = []

    try:
        pdf = pdfplumber.open(pdf_path, password=password)
    except Exception as e:
        err_str = str(e).lower()
        if "password" in err_str or "decrypt" in err_str or "encrypted" in err_str:
            raise RuntimeError(
                f"Wrong password or encrypted PDF: {pdf_path}\n"
                f"  Check PDF_PASSWORD in your .env file."
            ) from e
        raise

    with pdf:
        page_count = len(pdf.pages)
        if page_count == 0:
            raise RuntimeError(f"PDF has no pages: {pdf_path}")

        first_page_text = pdf.pages[0].extract_text() or ""
        if not re.search(r"State Bank|SBI|Account\s*Number", first_page_text, re.IGNORECASE):
            raise RuntimeError(
                f"This doesn't look like an SBI statement: {pdf_path}\n"
                f"  First page has no SBI/State Bank header."
            )

        stmt_from, stmt_to = extract_statement_period(pdf)
        seq = 0

        for page in pdf.pages:
            tables = page.extract_tables()
            if not tables:
                continue
            for table in tables:
                if not table:
                    continue
                for row in table:
                    if not row:
                        continue
                    if is_summary_row(row):
                        continue
                    if not is_transaction_row(row):
                        continue

                    desc_raw = row[COL_DESCRIPTION] or ""
                    post_date = (row[COL_TXN_DATE] or "").strip()
                    value_date = (row[COL_VALUE_DATE] or "").strip()
                    debit = parse_amount(row[COL_DEBIT])
                    credit = parse_amount(row[COL_CREDIT])
                    balance = parse_amount(row[COL_BALANCE])

                    if not debit and not credit and not balance:
                        continue

                    txn_type = "debit" if debit else "credit" if credit else ""

                    transactions.append({
                        "value_date": value_date,
                        "post_date": post_date,
                        "details": clean_description(desc_raw),
                        "ref_no": extract_ref_number(desc_raw),
                        "debit": debit,
                        "credit": credit,
                        "balance": balance,
                        "txn_type": txn_type,
                        "account_source": "sbi_email",
                        "_parse_seq": seq,
                    })
                    seq += 1

    return transactions, stmt_from, stmt_to, page_count


# ---------------------------------------------------------------------------
# Hash, dedup, sort, CSV I/O
# ---------------------------------------------------------------------------

def compute_hash(txn):
    """SHA-256 of 5 financial fields. Balance is a running total so
    even same-amount transactions on the same day produce unique hashes."""
    raw = "|".join([
        txn["post_date"], txn["value_date"],
        txn["debit"], txn["credit"], txn["balance"],
    ])
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def parse_date_for_sort(date_str):
    try:
        return datetime.strptime(date_str, "%d/%m/%Y")
    except (ValueError, TypeError):
        return datetime.max


def sort_key(txn):
    """Sort by date ascending, then by PDF row order within each day."""
    seq = txn.get("_parse_seq")
    if seq is None:
        try:
            seq = int(txn.get("txn_id", 0))
        except (ValueError, TypeError):
            seq = 0
    return (parse_date_for_sort(txn["post_date"]), seq)


def load_existing_csv(path):
    if not path.exists():
        return [], set()
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    if rows:
        missing = set(CSV_FIELDS) - set(rows[0].keys())
        if missing:
            raise RuntimeError(
                f"Master CSV is missing columns: {missing}\n"
                f"  Delete the CSV and re-run to regenerate."
            )
    hashes = {r["hash"] for r in rows if r.get("hash")}
    return rows, hashes


def write_master_csv(transactions, path):
    """Sort, assign sequential txn_id, write CSV atomically."""
    transactions.sort(key=sort_key)
    for i, txn in enumerate(transactions, start=1):
        txn["txn_id"] = i

    path = Path(path)
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".csv.tmp")
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(transactions)
        Path(tmp_path).replace(path)
    except BaseException:
        Path(tmp_path).unlink(missing_ok=True)
        raise


# ---------------------------------------------------------------------------
# Main (CLI)
# ---------------------------------------------------------------------------

def find_sbi_pdfs():
    return sorted(
        [p for p in DOWNLOADS_DIR.glob("*.pdf") if "statement" in p.name.lower()],
        key=lambda p: p.stat().st_mtime, reverse=True,
    )


def main():
    try:
        password = load_password()
    except RuntimeError as e:
        print(f"Error: {e}")
        sys.exit(1)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    if len(sys.argv) > 1:
        pdf_paths = [Path(p) for p in sys.argv[1:]]
    else:
        pdf_paths = find_sbi_pdfs()
        if not pdf_paths:
            print("No statement PDFs found in Downloads.")
            print("Usage: python parse_sbi_statement.py <pdf1> [pdf2] ...")
            sys.exit(1)
        print(f"Found {len(pdf_paths)} statement PDF(s) in {DOWNLOADS_DIR}:\n")

    existing, existing_hashes = load_existing_csv(MASTER_CSV)
    if existing:
        print(f"Existing master CSV: {len(existing)} transactions\n")

    all_new = []
    seq_offset = len(existing)

    for pdf_path in pdf_paths:
        if not pdf_path.exists():
            print(f"  SKIP: {pdf_path} (not found)")
            continue

        print(f"Parsing: {pdf_path.name}")
        try:
            transactions, stmt_from, stmt_to, pages = parse_pdf(str(pdf_path), password)
        except RuntimeError as e:
            print(f"  ERROR: {e}")
            continue
        except Exception as e:
            print(f"  ERROR parsing {pdf_path.name}: {type(e).__name__}: {e}")
            continue

        if stmt_from and stmt_to:
            print(f"  Period: {stmt_from} to {stmt_to}  ({pages} pages)")

        new_count = 0
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
                all_new.append(txn)
                existing_hashes.add(h)
                new_count += 1

        print(f"  Found: {len(transactions)} total,  {new_count} new,  {dup_count} duplicates skipped\n")

    if not all_new:
        print("No new transactions to add.")
        if existing:
            print(f"Master CSV unchanged: {MASTER_CSV}")
        return

    merged = existing + all_new
    write_master_csv(merged, MASTER_CSV)

    total_dr = sum(float(t["debit"]) for t in merged if t["debit"])
    total_cr = sum(float(t["credit"]) for t in merged if t["credit"])
    print(f"{'=' * 50}")
    print(f"Added {len(all_new)} new transactions")
    print(f"Master CSV: {MASTER_CSV}")
    print(f"  Total: {len(merged)}  |  Debits: {total_dr:,.2f}  |  Credits: {total_cr:,.2f}")

    if merged:
        print(f"  Date range: {merged[0]['post_date']} to {merged[-1]['post_date']}")


if __name__ == "__main__":
    main()
