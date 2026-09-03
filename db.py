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
    """Make asyncpg hand us dicts for JSONB instead of raw strings."""
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
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
                      source_file=None, ingested_by=None):
    """
    Store one day's scan. Idempotent: re-ingesting the same (season, date)
    replaces that day cleanly, so a bad upload is fixable by just doing it again.

    Returns a dict summarising what happened.
    """
    id_idx = find_header(headers, "lord_id", "id", contains="lord_id")
    if id_idx is None:
        raise ValueError(
            f"No 'lord_id' column found. Headers were: {', '.join(headers[:12])}"
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
