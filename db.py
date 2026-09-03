"""
db.py — Postgres storage for daily scans.

Design note:
    Each row is stored as JSONB keyed by the CSV's own header names, alongside
    the header list for that scan (in scan_meta). This lets get_scan() rebuild
    the exact list-of-lists shape gspread returns ([headers, row, row, ...]),
    so existing commands that do headers.index("lord_id") keep working with no
    changes. A few hot fields (power/merits/units_dead) are also pulled out into
    real columns so you can query them directly without digging into the JSON.
"""

import csv
import io
import json
import os
import re
import openpyxl
from datetime import date, datetime

import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL")

_pool = None

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS scan_meta (
    season      TEXT        NOT NULL,
    scan_date   DATE        NOT NULL,
    headers     JSONB       NOT NULL,
    row_count   INTEGER     NOT NULL DEFAULT 0,
    source_file TEXT,
    ingested_by TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (season, scan_date)
);

CREATE TABLE IF NOT EXISTS scans (
    season      TEXT   NOT NULL,
    scan_date   DATE   NOT NULL,
    lord_id     TEXT   NOT NULL,
    row_idx     INTEGER NOT NULL,
    name        TEXT,
    alliance    TEXT,
    home_server TEXT,
    power       BIGINT,
    merits      BIGINT,
    units_dead  BIGINT,
    data        JSONB  NOT NULL,
    PRIMARY KEY (season, scan_date, lord_id)
);

CREATE INDEX IF NOT EXISTS scans_season_date_idx
    ON scans (season, scan_date);
CREATE INDEX IF NOT EXISTS scans_lord_history_idx
    ON scans (season, lord_id, scan_date DESC);
CREATE INDEX IF NOT EXISTS scans_server_idx
    ON scans (season, scan_date, home_server);
"""


async def _init_connection(conn):
    """
    JSONB codec in binary format. Binary is required because ingest uses
    COPY (copy_records_to_table), which cannot use text-format encoders.
    The leading \x01 byte is Postgres's jsonb format version marker.
    """
    await conn.set_type_codec(
        "jsonb",
        encoder=lambda value: b"\x01" + json.dumps(value).encode("utf-8"),
        decoder=lambda value: json.loads(value[1:].decode("utf-8")),
        schema="pg_catalog",
        format="binary",
    )


async def init_db():
    """Create the pool and ensure tables exist. Call once on bot startup."""
    global _pool
    if _pool is not None:
        return _pool

    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is not set. In Railway, add a PostgreSQL service and "
            "reference its DATABASE_URL variable from the bot service."
        )

    _pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=5,
        init=_init_connection,
        command_timeout=60,
    )

    async with _pool.acquire() as conn:
        await conn.execute(SCHEMA)

    return _pool


def pool():
    if _pool is None:
        raise RuntimeError("init_db() has not been called yet.")
    return _pool


async def close_db():
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def to_int(val):
    """Tolerant int parser. Handles '1,234', '1 234', '-', '', None."""
    if val is None:
        return 0
    s = str(val).replace("\u00A0", "").replace(",", "").replace(" ", "").strip()
    if s in ("", "-"):
        return 0
    try:
        return int(s)
    except ValueError:
        # Fall back to digits only (handles '21.734.811' style EU formatting)
        digits = "".join(ch for ch in s if ch.isdigit() or ch == "-")
        try:
            return int(digits) if digits not in ("", "-") else 0
        except ValueError:
            return 0


def parse_scan_csv(raw: bytes):
    """
    Parse raw CSV bytes into (headers, rows) where rows is a list of lists.

    Handles UTF-8 BOM, latin-1 fallback, and comma/semicolon/tab delimiters.
    Values are kept as strings — exactly like gspread's get_all_values() —
    so downstream parsing behaviour is identical.
    """
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError("Could not decode the file as text — is it really a CSV?")

    sample = text[:8192]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
    except csv.Error:
        dialect = csv.excel  # default to comma

    reader = csv.reader(io.StringIO(text), dialect)
    rows = [r for r in reader if any(str(c).strip() for c in r)]

    if not rows:
        raise ValueError("The CSV appears to be empty.")

    headers = [str(h).strip() for h in rows[0]]
    if not any(headers):
        raise ValueError("The first row of the CSV has no usable column headers.")

    return headers, rows[1:]


def find_header(headers, *candidates, contains=None):
    """
    Locate a column index by exact name (case-insensitive), then by substring.
    Returns None if nothing matches — callers decide whether that's fatal.
    """
    lowered = [h.strip().lower() for h in headers]
    for cand in candidates:
        c = cand.strip().lower()
        if c in lowered:
            return lowered.index(c)
    needle = (contains or candidates[0]).strip().lower()
    for i, h in enumerate(lowered):
        if needle in h:
            return i
    return None


def date_from_filename(filename: str):
    """Pull a YYYY-MM-DD (or YYYY_MM_DD / YYYYMMDD) date out of a filename."""
    if not filename:
        return None
    m = re.search(r"(20\d{2})[-_]?(\d{2})[-_]?(\d{2})", filename)
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------

async def ingest_scan(season, scan_date, headers, rows,
                      source_file=None, ingested_by=None, id_column=None):
    """
    Store one day's scan. Idempotent: re-ingesting the same (season, date)
    replaces that day cleanly, so a bad upload is fixable by just doing it again.

    Returns a dict summarising what happened.
    """
        if id_column:
        lowered = [h.strip().lower() for h in headers]
        target = id_column.strip().lower()
        id_idx = lowered.index(target) if target in lowered else None
    else:
        id_idx = find_header(headers, "lord_id", "id", contains="lord_id")

    if id_idx is None:
        raise ValueError(
            f"No '{id_column or 'lord_id'}' column found. "
            f"Headers were: {', '.join(headers[:12])}"
        )

    name_idx   = find_header(headers, "name", contains="name")
    alli_idx   = find_header(headers, "alliance", "alliance_tag", contains="alliance")
    server_idx = find_header(headers, "home_server", contains="server")
    power_idx  = find_header(headers, "highest_power", "power", contains="power")
    merits_idx = find_header(headers, "merits", contains="merits")
    dead_idx   = find_header(headers, "units_dead", contains="units_dead")

    def cell(row, idx):
        if idx is None or idx >= len(row):
            return ""
        return str(row[idx]).strip()

    records = {}
    skipped = 0

    for i, row in enumerate(rows):
        lid = cell(row, id_idx)
        if not lid:
            skipped += 1
            continue

        # Pad short rows so the JSON always has every header key
        payload = {h: (str(row[j]).strip() if j < len(row) else "")
                   for j, h in enumerate(headers)}

        # Dict keyed on lord_id => later duplicates win, matching the
        # "keep last occurrence" behaviour your prev_map already relies on.
        records[lid] = (
            season, scan_date, lid, i,
            cell(row, name_idx) or None,
            cell(row, alli_idx) or None,
            cell(row, server_idx) or None,
            to_int(cell(row, power_idx)),
            to_int(cell(row, merits_idx)),
            to_int(cell(row, dead_idx)),
            payload,
        )

    duplicates = len(
        [r for r in rows if cell(r, id_idx)]
    ) - len(records)

    if not records:
        raise ValueError("No rows with a valid lord_id were found in this file.")

    async with pool().acquire() as conn:
        async with conn.transaction():
            replaced = await conn.fetchval(
                "SELECT row_count FROM scan_meta WHERE season = $1 AND scan_date = $2",
                season, scan_date,
            )
            await conn.execute(
                "DELETE FROM scans WHERE season = $1 AND scan_date = $2",
                season, scan_date,
            )
            await conn.copy_records_to_table(
                "scans",
                records=list(records.values()),
                columns=[
                    "season", "scan_date", "lord_id", "row_idx", "name",
                    "alliance", "home_server", "power", "merits",
                    "units_dead", "data",
                ],
            )
            await conn.execute(
                """
                INSERT INTO scan_meta
                    (season, scan_date, headers, row_count, source_file, ingested_by)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (season, scan_date) DO UPDATE SET
                    headers     = EXCLUDED.headers,
                    row_count   = EXCLUDED.row_count,
                    source_file = EXCLUDED.source_file,
                    ingested_by = EXCLUDED.ingested_by,
                    ingested_at = now()
                """,
                season, scan_date, headers, len(records), source_file, ingested_by,
            )

    return {
        "rows": len(records),
        "skipped_no_id": skipped,
        "duplicate_ids": duplicates,
        "replaced": replaced,
        "columns": len(headers),
    }


# ---------------------------------------------------------------------------
# Retrieval
# ---------------------------------------------------------------------------

async def list_scan_dates(season, limit=None):
    """Most recent first."""
    q = """
        SELECT scan_date, row_count, ingested_at
        FROM scan_meta WHERE season = $1
        ORDER BY scan_date DESC
    """
    if limit:
        q += f" LIMIT {int(limit)}"
    async with pool().acquire() as conn:
        return await conn.fetch(q, season)


async def get_scan(season, scan_date):
    """
    Return one scan as [headers, row, row, ...] — the same shape gspread's
    get_all_values() returns, so existing command code works unchanged.
    """
    async with pool().acquire() as conn:
        headers = await conn.fetchval(
            "SELECT headers FROM scan_meta WHERE season = $1 AND scan_date = $2",
            season, scan_date,
        )
        if headers is None:
            return None
        rows = await conn.fetch(
            """
            SELECT data FROM scans
            WHERE season = $1 AND scan_date = $2
            ORDER BY row_idx
            """,
            season, scan_date,
        )

    out = [headers]
    for r in rows:
        d = r["data"]
        out.append([d.get(h, "") for h in headers])
    return out


async def get_latest_dates(season, n=2):
    """The n most recent scan dates for a season, newest first."""
    async with pool().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT scan_date FROM scan_meta
            WHERE season = $1 ORDER BY scan_date DESC LIMIT $2
            """,
            season, n,
        )
    return [r["scan_date"] for r in rows]


async def get_oldest_date(season):
    async with pool().acquire() as conn:
        return await conn.fetchval(
            "SELECT MIN(scan_date) FROM scan_meta WHERE season = $1", season
        )


async def nearest_date_on_or_before(season, target):
    """Useful for arbitrary windows: '7 days ago' may not be an exact scan day."""
    async with pool().acquire() as conn:
        return await conn.fetchval(
            """
            SELECT MAX(scan_date) FROM scan_meta
            WHERE season = $1 AND scan_date <= $2
            """,
            season, target,
        )


async def delete_scan(season, scan_date):
    async with pool().acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "DELETE FROM scans WHERE season = $1 AND scan_date = $2",
                season, scan_date,
            )
            return await conn.execute(
                "DELETE FROM scan_meta WHERE season = $1 AND scan_date = $2",
                season, scan_date,
            )


# ---------------------------------------------------------------------------
# Example of what the DB unlocks — direct SQL deltas, any window you like
# ---------------------------------------------------------------------------

async def dead_gains(season, from_date, to_date, min_power=0, server=None, limit=10):
    """
    Dead-unit gains between two arbitrary dates, for players present in both.
    This is the kind of query that's simply impossible with the overwrite-a-tab
    setup — it's why keeping every day is worth it.
    """
    async with pool().acquire() as conn:
        return await conn.fetch(
            """
            SELECT
                b.lord_id,
                b.name,
                b.alliance,
                b.power,
                GREATEST(b.units_dead - a.units_dead, 0) AS gain
            FROM scans a
            JOIN scans b
              ON a.season = b.season AND a.lord_id = b.lord_id
            WHERE a.season = $1
              AND a.scan_date = $2
              AND b.scan_date = $3
              AND b.power >= $4
              AND ($5::text IS NULL OR b.home_server = $5)
            ORDER BY gain DESC
            LIMIT $6
            """,
            season, from_date, to_date, min_power, server, limit,
        )

# =============================================================================
# db.py ADDITIONS — paste at the end of db.py
# =============================================================================
# The 375 export is a CUMULATIVE range (season start -> today), not a snapshot.
# So we record both ends of the period, and gains for a window are computed by
# subtracting an earlier file from the latest one.
#
# Also adds XLSX parsing, since the in-game tool exports .xlsx not .csv.
#
# Requires: openpyxl  (add to requirements.txt)
# =============================================================================

import openpyxl


# -----------------------------------------------------------------------------
# Schema upgrade — safe to run repeatedly
# -----------------------------------------------------------------------------

PERIOD_SCHEMA = """
ALTER TABLE scan_meta ADD COLUMN IF NOT EXISTS period_start DATE;
ALTER TABLE scan_meta ADD COLUMN IF NOT EXISTS period_end   DATE;
UPDATE scan_meta
   SET period_start = COALESCE(period_start, scan_date),
       period_end   = COALESCE(period_end,   scan_date);
"""


async def upgrade_schema():
    """Call once on startup, after init_db()."""
    async with pool().acquire() as conn:
        await conn.execute(PERIOD_SCHEMA)


# -----------------------------------------------------------------------------
# XLSX parsing
# -----------------------------------------------------------------------------

def parse_scan_xlsx(raw: bytes, sheet_name=None):
    """
    Parse raw .xlsx bytes into (headers, rows), all values as strings —
    matching parse_scan_csv so downstream handling is identical.
    """
    wb = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
    ws = wb[sheet_name] if sheet_name else wb.worksheets[0]

    rows_iter = ws.iter_rows(values_only=True)
    try:
        header_row = next(rows_iter)
    except StopIteration:
        raise ValueError("The spreadsheet is empty.")

    headers = [str(h).strip() if h is not None else "" for h in header_row]
    if not any(headers):
        raise ValueError("The first row has no usable column headers.")

    rows = []
    for r in rows_iter:
        if not any(c is not None and str(c).strip() for c in r):
            continue
        rows.append([str(c).strip() if c is not None else "" for c in r])

    wb.close()
    return headers, rows


def parse_scan_file(raw: bytes, filename: str):
    """Dispatch to the right parser based on file extension."""
    lower = (filename or "").lower()
    if lower.endswith((".xlsx", ".xlsm")):
        return parse_scan_xlsx(raw)
    return parse_scan_csv(raw)


def date_range_from_filename(filename: str):
    """
    Pull a start and end date out of a filename like
    '375_2026-08-28_2026-09-02.xlsx'. Returns (start, end), either may be None.
    """
    if not filename:
        return None, None
    found = re.findall(r"(20\d{2})[-_](\d{2})[-_](\d{2})", filename)
    parsed = []
    for y, m, d in found:
        try:
            parsed.append(date(int(y), int(m), int(d)))
        except ValueError:
            continue
    if len(parsed) >= 2:
        return parsed[0], parsed[1]
    if len(parsed) == 1:
        return None, parsed[0]
    return None, None


# -----------------------------------------------------------------------------
# Period-aware ingest
# -----------------------------------------------------------------------------

async def ingest_period(season, period_start, period_end, headers, rows,
                        id_column, source_file=None, ingested_by=None):
    """
    Store a cumulative period export. Keyed on period_end (stored as scan_date),
    so one file per day slots in naturally.
    """
    result = await ingest_scan(
        season=season,
        scan_date=period_end,
        headers=headers,
        rows=rows,
        source_file=source_file,
        ingested_by=ingested_by,
        id_column=id_column,
    )

    async with pool().acquire() as conn:
        await conn.execute(
            """
            UPDATE scan_meta SET period_start = $3, period_end = $2
            WHERE season = $1 AND scan_date = $2
            """,
            season, period_end, period_start,
        )

    result["period_start"] = period_start
    result["period_end"] = period_end
    return result


async def get_period_starts(season):
    """Distinct period_start values seen for a dataset — used to spot mistakes."""
    async with pool().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT period_start FROM scan_meta
            WHERE season = $1 AND period_start IS NOT NULL
            """,
            season,
        )
    return [r["period_start"] for r in rows]


# -----------------------------------------------------------------------------
# Materialising a window
# -----------------------------------------------------------------------------
# Columns that are absolute values rather than accumulating gains — these are
# never subtracted when computing a window.
# -----------------------------------------------------------------------------

ABSOLUTE_COLUMNS_375 = {
    "rank",
    "character id",
    "character name",
    "current power",
    "historical highest power",
}


async def materialize_period(season, id_column, base_date=None, end_date=None,
                             absolute_columns=None):
    """
    Return [headers, row, row, ...] for a dataset — the same shape gspread
    produces, so existing command code works unchanged.

    base_date=None  -> the raw latest file (i.e. season-to-date totals)
    base_date=<d>   -> latest MINUS the file from <d>, giving gains for the window

    Players present in the latest file but not the base are returned as-is
    (they joined mid-window, so their gains are their whole total).
    """
    absolute = absolute_columns if absolute_columns is not None else ABSOLUTE_COLUMNS_375

    if end_date is None:
        dates = await get_latest_dates(season, n=1)
        if not dates:
            return None
        end_date = dates[0]

    latest = await get_scan(season, end_date)
    if latest is None:
        return None
    if base_date is None or base_date == end_date:
        return latest

    base = await get_scan(season, base_date)
    if base is None:
        return latest

    headers = latest[0]
    lower = [h.strip().lower() for h in headers]

    try:
        id_idx = lower.index(id_column.strip().lower())
    except ValueError:
        return latest

    base_map = {str(r[id_idx]).strip(): r for r in base[1:]}

    out = [headers]
    for row in latest[1:]:
        rid = str(row[id_idx]).strip()
        prev = base_map.get(rid)
        if prev is None:
            out.append(list(row))
            continue

        new_row = []
        for i, val in enumerate(row):
            if lower[i] in absolute:
                new_row.append(val)
                continue
            gain = to_int(val) - to_int(prev[i] if i < len(prev) else 0)
            new_row.append(str(max(0, gain)))
        out.append(new_row)

    return out
