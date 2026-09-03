import gspread
from oauth2client.service_account import ServiceAccountCredentials
from string import ascii_uppercase
import os
import json
import discord
import db
import lb
import re
from discord.ext import commands
from discord.ext import tasks
from datetime import datetime, timedelta, UTC, timezone, date
import asyncio
import unicodedata

# Google Sheets 
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_json = os.getenv("CREDENTIALS_JSON")
creds_dict = json.loads(creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

EVENT_SHEET_NAME = "Event Schedule"      # The spreadsheet name
EVENT_TAB_NAME = "events"               # The tab name
ANNOUNCE_CHANNEL_ID = 1383515877793595435  # 👈 set your daily-announcement channel

# Season sheet
SEASON_SHEETS = {
    "sos3": "NxW - SoS3",
    "sos4": "NxW - SoS4",
    "farms": "NVR Farms",
    "sos7": "EvG - SoS7",
    "sos2": "NVR - SoS2",
    "statue": "NVR - SoS2 Statue",
}

SERVER_375_SHEET = "Call of Dragons - Server 375 Stats"

DEFAULT_SEASON = "sos2"

# =============================================================================
# SEASON CONFIG — put this near the top of main.py, after SEASON_SHEETS
# =============================================================================
# Everything that changes between seasons lives here. Next season you add one
# entry and change DEFAULT_SEASON. You do not touch any command code.
# =============================================================================

# Every server you've ever encountered. Add new opponents here as you meet them.
# lb.py has its own copy for defaults; this merges into it.
ALL_SERVERS = {
    "375": "NVR",
    "357": "YSS",
    "756": "SAB",
    "341": "NW:E",
    "320": "EvG",
    "5":   "OMG",
    # Next season, add the new opponent here, e.g.:
    # "412": "XYZ",
}

SERVER_COLORS = {
    "375": 0xE74C3C,   # red — home
    "357": 0x3498DB,   # blue — main rival
    "756": 0xE67E22,
    "341": 0x9B59B6,
    "320": 0x2ECC71,
    "5":   0xF1C40F,
}

lb.SERVER_NAMES.update(ALL_SERVERS)
lb.SERVER_COLORS.update(SERVER_COLORS)


SEASON_CONFIG = {
    "sos2": {
        "label":   "SoS2",
        "start":   date(2026, 8, 28),
        "home":    "375",
        # War pairings. Each side is a list, so alliances of multiple servers work.
        "matchups": [
            (["375"], ["357"]),
            (["756"], ["341"]),
            (["5"],   ["320"]),
        ],
    },

    # ---- Next season: copy the block above, change the values ----------------
    # "sos8": {
    #     "label": "SoS8",
    #     "start": date(2026, 11, 15),
    #     "home":  "375",
    #     "matchups": [
    #         (["375"], ["412"]),
    #     ],
    # },
}


def season_cfg(season):
    """Config for a season, with safe fallbacks if it hasn't been set up."""
    cfg = SEASON_CONFIG.get(season, {})
    return {
        "label":    cfg.get("label", season.upper()),
        "start":    cfg.get("start", SEASON_START),
        "home":     cfg.get("home", lb.DEFAULT_SERVER),
        "matchups": cfg.get("matchups", []),
    }


def season_start(season=None):
    return season_cfg(season or DEFAULT_SEASON)["start"]


def home_server(season=None):
    return season_cfg(season or DEFAULT_SEASON)["home"]

# Global memory bank for background tasks
bot_cache = {
    "375_data": None,
    "seasons": {} 
}

# Now your bot setup
intents = discord.Intents.default()
intents.guilds = True
intents.messages = True
intents.members = True
intents.reactions = True
intents.guild_reactions = True
intents.message_content = True  

bot = commands.Bot(command_prefix="!", intents=intents)
bot.remove_command('help')  # Add it right here!

# Global flag
VACATION_MODE = False
VACATION_MSG = "🗣️ not updated 🗣️ old data 🗣️ update update"

# Simple check before every command
@bot.check
async def global_vacation_check(ctx):
    if VACATION_MODE:
        await ctx.send(VACATION_MSG)
        return False
    return True

# Config values
CONFIRM_CHANNEL_ID = 1527938722987900978  # ID of the channel with the message + reactions
WAR_CHANNEL_ID = 1369071691111600168  # ⬅️ replace with your war channel ID
REACTION_MESSAGE_ID = 1369072129068372008  # ⬅️ replace with your message ID
ALLOWED_COMMAND_CHANNEL_ID = [1378735765827358791, 1383515877793595435, 1236059889411952690]

# Emoji → new channel name mapping
WAR_CHANNEL_REACTIONS = {
    "🔴": "🔴｜𝐅𝐔𝐋𝐋𝐖𝐀𝐑",
    "🟢": "🟢｜𝐍𝐎-𝐅𝐈𝐆𝐇𝐓𝐈𝐍𝐆",
    "🟡": "🟡｜𝐒𝐊𝐈𝐑𝐌𝐈𝐒𝐇𝐄𝐒",
    "🧑‍🌾": "🧑‍🌾｜𝐆𝐎-𝐅𝐀𝐑𝐌",
    "⚠️": "⚠️｜𝐁𝐔𝐈𝐋𝐃-𝐁𝐔𝐅𝐅",
}

SCAN_INGEST_CHANNEL_ID = 1236059889411952690   # where !ingest is allowed
SCAN_ADMIN_ROLE_ID     = 1528211607556198482   # role permitted to ingest
 
 
def scan_admin():
    """Only allow ingest/delete from an admin role in the designated channel."""
    async def predicate(ctx):
        if ctx.channel.id != SCAN_INGEST_CHANNEL_ID:
            await ctx.send(f"❌ Use this in <#{SCAN_INGEST_CHANNEL_ID}>.")
            return False
        if not any(r.id == SCAN_ADMIN_ROLE_ID for r in getattr(ctx.author, "roles", [])):
            await ctx.send("❌ You don't have permission to manage scans.")
            return False
        return True
    return commands.check(predicate)


@bot.event
async def on_raw_reaction_add(payload):
    # This captures the member who reacted
    user = payload.member 
    
    print(f"Reaction detected: emoji={payload.emoji}, user={payload.user_id}, message={payload.message_id}")

    if payload.message_id != REACTION_MESSAGE_ID:
        return

    # Basic safety check to ensure 'user' exists (rarely an issue in guilds)
    if not user:
        return

    emoji = str(payload.emoji)
    new_name = WAR_CHANNEL_REACTIONS.get(emoji)
    if not new_name:
        return

    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return

    war_channel = guild.get_channel(WAR_CHANNEL_ID)
    confirm_channel = guild.get_channel(CONFIRM_CHANNEL_ID)
    
    if not war_channel or not confirm_channel:
        return

    try:
        await war_channel.edit(name=new_name)
        # Now 'user' is defined, so this line will work!
        await confirm_channel.send(f"✅ War channel renamed to `{new_name}` by **{user.display_name}** based on reaction {emoji}")
    except Exception as e:
        await confirm_channel.send(f"❌ Failed to rename war channel: {e}")
        
# ============================
# Parsers / formatters
# ============================

def to_int_eu(v):
    """Parse ints from EU/US formats: '21.734.811', '21,734,811', '21 734 811', '-', '' -> 0."""
    try:
        s = str(v).replace(".", "").replace(",", "").replace(" ", "").strip()
        if s in ("", "-"):
            return 0
        return int(s)
    except:
        return 0

def fmt_int_eu(n: int) -> str:
    """12345678 -> '12.345.678'."""
    return f"{n:,}".replace(",", ".")

def fmt_pct(n: float) -> str:
    return f"{n:.2f}%"

# ---------- card rendering ----------

def player_field_name(p):
    # "Name (last6id)  •  S375"
    lid = p.get("lid","")
    short = lid[-6:] if lid else ""
    srv = p.get("srv","")
    base = (p.get("name") or "—").strip()
    return f"{base} ({short})  •  S{srv}"

def player_field_value(p):
    # Clean, readable lines (no tables)
    power = fmt_int_eu(p["power"])
    meritsΔ = fmt_int_eu(p["merits_gain"])
    m_pct = fmt_pct(p["merit_ratio"])
    dead_abs = fmt_int_eu(p["dead_gain"])
    d_pct = fmt_pct(p["dead_ratio"])
    tags = []
    if p.get("flex"):   tags.append("flex")
    if p.get("abs_ok"): tags.append("abs")
    tag_str = f" — *{', '.join(tags)}*" if tags else ""
    return (
        f"**Power:** {power}\n"
        f"**MeritsΔ:** {meritsΔ} ({m_pct})\n"
        f"**Deads:** {dead_abs} ({d_pct}){tag_str}"
    )

async def send_section_cards(ctx, title: str, emoji: str, color: int, items: list):
    """
    Sends paginated embeds where each player is a field (max 25 per embed).
    """
    if not items:
        embed = discord.Embed(
            title=f"{emoji} {title} — 0",
            description="No entries.",
            color=color
        )
        await ctx.send(embed=embed)
        return

    MAX_FIELDS = 25
    total_pages = (len(items) - 1) // MAX_FIELDS + 1
    page = 1
    for i in range(0, len(items), MAX_FIELDS):
        chunk = items[i:i+MAX_FIELDS]
        embed = discord.Embed(
            title=f"{emoji} {title} — {len(items)} (page {page}/{total_pages})",
            color=color
        )
        for p in chunk:
            embed.add_field(
                name=player_field_name(p),
                value=player_field_value(p),
                inline=False
            )
        await ctx.send(embed=embed)
        page += 1


@bot.command(name="ingest")
@scan_admin()
async def ingest_scan_cmd(ctx, season: str = DEFAULT_SEASON, scan_date: str = None):
    """
    Upload a daily scan CSV.
 
        !ingest                     -> default season, date from filename or today
        !ingest sos2                -> explicit season
        !ingest sos2 2026-09-01     -> explicit season and date (backfilling)
 
    Re-running for the same date replaces that day — safe to redo a bad upload.
    """
    async with ctx.typing():
        season = season.lower()
        if season not in SEASON_SHEETS:
            await ctx.send(f"❌ Unknown season. Options: {', '.join(SEASON_SHEETS.keys())}")
            return
 
        if not ctx.message.attachments:
            await ctx.send("❌ Attach the scan CSV to the same message as the command.")
            return
 
        attachment = ctx.message.attachments[0]
        if not attachment.filename.lower().endswith((".csv", ".txt", ".tsv")):
            await ctx.send(f"❌ `{attachment.filename}` doesn't look like a CSV.")
            return
 
        # Work out which day this scan represents
        if scan_date:
            try:
                target = datetime.strptime(scan_date, "%Y-%m-%d").date()
            except ValueError:
                await ctx.send("❌ Date must be `YYYY-MM-DD`.")
                return
            date_source = "you specified it"
        else:
            target = db.date_from_filename(attachment.filename)
            if target:
                date_source = "read from the filename"
            else:
                target = datetime.now(UTC).date()
                date_source = "today's UTC date — no date in the filename"
 
        try:
            raw = await attachment.read()
            headers, rows = db.parse_scan_csv(raw)
            result = await db.ingest_scan(
                season=season,
                scan_date=target,
                headers=headers,
                rows=rows,
                source_file=attachment.filename,
                ingested_by=str(ctx.author),
            )
        except Exception as e:
            await ctx.send(f"❌ **Ingest failed:** {e}")
            return
 
        embed = discord.Embed(
            title="✅ Scan ingested",
            description=f"**{season.upper()}** — `{target}`\n*Date {date_source}.*",
            color=discord.Color.green(),
        )
        embed.add_field(name="Rows", value=f"{result['rows']:,}", inline=True)
        embed.add_field(name="Columns", value=str(result["columns"]), inline=True)
        embed.add_field(name="File", value=f"`{attachment.filename}`", inline=True)
 
        notes = []
        if result["replaced"] is not None:
            notes.append(f"↻ Replaced an existing scan for this date ({result['replaced']:,} rows).")
        if result["duplicate_ids"]:
            notes.append(f"⚠️ {result['duplicate_ids']} duplicate lord_id(s) — kept the last occurrence.")
        if result["skipped_no_id"]:
            notes.append(f"⚠️ Skipped {result['skipped_no_id']} row(s) with no lord_id.")
        if notes:
            embed.add_field(name="Notes", value="\n".join(notes), inline=False)
 
        embed.set_footer(text="Cache refreshes within 10 minutes, or run !resync now.")
        await ctx.send(embed=embed)
 
        # Refresh immediately so the new scan is live right away
        await refresh_season_cache()
 
 
# -----------------------------------------------------------------------------
# 3. ADMIN UTILITIES
# -----------------------------------------------------------------------------
 
@bot.command(name="scans")
async def list_scans_cmd(ctx, season: str = DEFAULT_SEASON, limit: int = 15):
    """Show which scan dates are stored for a season."""
    async with ctx.typing():
        if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
            mentions = ", ".join(f"<#{c}>" for c in ALLOWED_COMMAND_CHANNEL_ID)
            await ctx.send(f"❌ Commands are only allowed in {mentions}.")
            return
 
        season = season.lower()
        rows = await db.list_scan_dates(season, limit=min(max(limit, 1), 50))
        if not rows:
            await ctx.send(f"📭 No scans stored for **{season.upper()}** yet.")
            return
 
        lines = [
            f"`{r['scan_date']}` — {r['row_count']:,} rows"
            for r in rows
        ]
        embed = discord.Embed(
            title=f"🗂️ Stored scans — {season.upper()}",
            description="\n".join(lines),
            color=discord.Color.blurple(),
        )
        embed.set_footer(text=f"Showing the {len(rows)} most recent.")
        await ctx.send(embed=embed)
 
 
@bot.command(name="unscan")
@scan_admin()
async def delete_scan_cmd(ctx, season: str, scan_date: str):
    """Delete one stored scan: !unscan sos2 2026-09-01"""
    try:
        target = datetime.strptime(scan_date, "%Y-%m-%d").date()
    except ValueError:
        await ctx.send("❌ Date must be `YYYY-MM-DD`.")
        return
 
    await db.delete_scan(season.lower(), target)
    await ctx.send(f"🗑️ Deleted scan `{target}` for **{season.upper()}**.")
    await refresh_season_cache()
 
 
@bot.command(name="resync")
@scan_admin()
async def resync_cmd(ctx):
    """Force an immediate cache refresh."""
    async with ctx.typing():
        await refresh_season_cache()
        await ctx.send("✅ Cache refreshed from the database.")

async def _load_season_from_sheets(season_key):
    """The old behaviour — used as a fallback until the DB has data."""
    sheet_name = SEASON_SHEETS[season_key]
    tabs = await asyncio.to_thread(client.open(sheet_name).worksheets)
    scan_tabs = [t for t in tabs if t.title.lower() != "roster"]
    if len(scan_tabs) < 2:
        return None

    latest_data = await asyncio.to_thread(scan_tabs[-1].get_all_values)
    prev_data   = await asyncio.to_thread(scan_tabs[-2].get_all_values)
    oldest_data = await asyncio.to_thread(scan_tabs[0].get_all_values)

    return {
        "latest":       latest_data,
        "prev":         prev_data,
        "oldest":       oldest_data,
        "latest_title": scan_tabs[-1].title,
        "prev_title":   scan_tabs[-2].title,
        "oldest_title": scan_tabs[0].title,
        "source":       "sheets",
    }


async def _load_season_from_db(season_key):
    dates = await db.get_latest_dates(season_key, n=1)
    if not dates:
        return None
 
    latest_date = dates[0]
    oldest_date = await db.get_oldest_date(season_key)
 
    if oldest_date == latest_date:
        return None  # only one scan — no deltas possible yet
 
    latest_data = await db.get_scan(season_key, latest_date)
    oldest_data = await db.get_scan(season_key, oldest_date)
 
    return {
        "latest":       latest_data,
        "prev":         oldest_data,              # <-- season start
        "oldest":       oldest_data,
        "latest_title": latest_date.isoformat(),
        "prev_title":   oldest_date.isoformat(),
        "oldest_title": oldest_date.isoformat(),
        "source":       "database",
    }

async def refresh_season_cache():
    """
    Prefer the database. Fall back to Google Sheets for any season that isn't
    in the database yet, so the bot never ends up with an empty cache.
    """
    for season_key in SEASON_SHEETS:
        try:
            data = await _load_season_from_db(season_key)
            if data is None:
                data = await _load_season_from_sheets(season_key)
                await asyncio.sleep(2)  # be polite to the Sheets API
            if data:
                bot_cache["seasons"][season_key] = data
        except Exception as e:
            print(f"⚠️ Failed to cache season '{season_key}': {e}")

# -----------------------------------------------------------------------------
# BACKFILL — import existing Google Sheets tabs into the database
# -----------------------------------------------------------------------------

def _parse_tab_date(title: str):
    """
    Turn a tab title into a date. Handles 2026-08-31, 2026_08_31, 20260831,
    31-08-2026, 08/31/2026. Returns None if it doesn't look like a date.
    """
    t = title.strip()

    m = re.search(r"(20\d{2})[-_./]?(\d{1,2})[-_./]?(\d{1,2})", t)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    m = re.search(r"(\d{1,2})[-_./](\d{1,2})[-_./](20\d{2})", t)
    if m:
        for day, month in ((m.group(1), m.group(2)), (m.group(2), m.group(1))):
            try:
                return date(int(m.group(3)), int(month), int(day))
            except ValueError:
                continue

    return None


@bot.command(name="backfill")
@scan_admin()
async def backfill_cmd(ctx, season: str, confirm: str = None):
    """
    Import a season's existing Google Sheets tabs into the database.

        !backfill sos2            -> dry run, shows what it WOULD import
        !backfill sos2 confirm    -> actually imports

    Safe to run more than once: re-importing a date just replaces it.
    """
    season = season.lower()
    if season not in SEASON_SHEETS:
        await ctx.send(f"❌ Unknown season. Options: {', '.join(SEASON_SHEETS.keys())}")
        return

    committing = (confirm == "confirm")
    sheet_name = SEASON_SHEETS[season]

    status = await ctx.send(
        f"{'📥 Importing' if committing else '🔍 Dry run for'} **{season.upper()}** "
        f"(`{sheet_name}`)... reading tab list."
    )

    try:
        tabs = await asyncio.to_thread(client.open(sheet_name).worksheets)
    except Exception as e:
        await status.edit(content=f"❌ Could not open `{sheet_name}`: {e}")
        return

    planned = []   # (tab, date)
    skipped = []   # tab titles with no parseable date

    for tab in tabs:
        if tab.title.strip().lower() == "roster":
            continue
        d = _parse_tab_date(tab.title)
        if d is None:
            skipped.append(tab.title)
        else:
            planned.append((tab, d))

    if not planned:
        msg = (
            f"❌ None of the tabs in `{sheet_name}` have a date in their title, "
            f"so nothing can be imported.\n"
            f"Tab names found: {', '.join(t.title for t in tabs[:10])}"
        )
        await status.edit(content=msg)
        return

    # ---- Dry run: report and stop ----
    if not committing:
        lines = [f"`{t.title}` → **{d}**" for t, d in planned[:25]]
        embed = discord.Embed(
            title=f"🔍 Backfill dry run — {season.upper()}",
            description="\n".join(lines),
            color=discord.Color.blurple(),
        )
        if len(planned) > 25:
            embed.description += f"\n*...and {len(planned) - 25} more.*"
        if skipped:
            embed.add_field(
                name="⏭️ Skipped (no date in title)",
                value=", ".join(f"`{s}`" for s in skipped[:10]),
                inline=False,
            )
        embed.set_footer(
            text=f"{len(planned)} tab(s) would be imported. "
                 f"Check the dates are right, then run: !backfill {season} confirm"
        )
        await status.edit(content=None, embed=embed)
        return

    # ---- Real import ----
    imported = 0
    total_rows = 0
    failures = []

    for i, (tab, d) in enumerate(planned, 1):
        try:
            await status.edit(
                content=f"📥 Importing **{season.upper()}** — "
                        f"tab {i}/{len(planned)} (`{tab.title}`)..."
            )
            values = await asyncio.to_thread(tab.get_all_values)
            if len(values) < 2:
                failures.append(f"`{tab.title}`: empty")
                continue

            headers = [str(h).strip() for h in values[0]]
            result = await db.ingest_scan(
                season=season,
                scan_date=d,
                headers=headers,
                rows=values[1:],
                source_file=f"backfill:{sheet_name}/{tab.title}",
                ingested_by=str(ctx.author),
            )
            imported += 1
            total_rows += result["rows"]
            await asyncio.sleep(2)   # Sheets API rate limit
        except Exception as e:
            failures.append(f"`{tab.title}`: {e}")

    embed = discord.Embed(
        title=f"✅ Backfill complete — {season.upper()}",
        color=discord.Color.green() if not failures else discord.Color.orange(),
    )
    embed.add_field(name="Tabs imported", value=f"{imported} / {len(planned)}", inline=True)
    embed.add_field(name="Total rows", value=f"{total_rows:,}", inline=True)
    if failures:
        embed.add_field(
            name="⚠️ Problems",
            value="\n".join(failures[:5]),
            inline=False,
        )
    await status.edit(content=None, embed=embed)

    await refresh_season_cache()

def parse_window(token):
    """
    Recognise a window argument. Returns one of:
        ("days", 7)                -> "7d" / "7days" / "7"  (only if suffixed)
        ("date", date(2026,8,15))  -> "2026-08-15"
        ("season", None)           -> "season" / "all" / "full"
        None                       -> not a window token
    """
    if not token:
        return None
    t = str(token).strip().lower()
 
    if t in ("season", "all", "full", "total"):
        return ("season", None)
 
    m = re.fullmatch(r"(\d+)\s*d(?:ays?)?", t)
    if m:
        return ("days", int(m.group(1)))
 
    m = re.fullmatch(r"(\d+)\s*w(?:eeks?)?", t)
    if m:
        return ("days", int(m.group(1)) * 7)
 
    m = re.fullmatch(r"(20\d{2})-(\d{2})-(\d{2})", t)
    if m:
        try:
            return ("date", date(int(m.group(1)), int(m.group(2)), int(m.group(3))))
        except ValueError:
            return None
 
    return None
 
async def get_window_data(season, window=None, include_excluded=False):
    """
    Returns {latest, prev, latest_title, prev_title, label} for a window,
    with excluded IDs removed.
    """
    parsed = parse_window(window) if window else None
 
    if parsed is None or parsed[0] == "season":
        cached = bot_cache["seasons"].get(season)
        if not cached:
            return None
        latest, prev = cached["latest"], cached["prev"]
        latest_title, prev_title = cached["latest_title"], cached["prev_title"]
        label = "Season to date"
    else:
        dates = await db.get_latest_dates(season, n=1)
        if not dates:
            return None
        latest_date = dates[0]
 
        if parsed[0] == "days":
            target = latest_date - timedelta(days=parsed[1])
            label = f"Last {parsed[1]} day{'s' if parsed[1] != 1 else ''}"
        else:
            target = parsed[1]
            label = f"Since {target}"
 
        prev_date = await db.nearest_date_on_or_before(season, target)
        if prev_date is None:
            prev_date = await db.get_oldest_date(season)
            label += " (limited by available history)"
        if prev_date is None or prev_date == latest_date:
            return None
 
        latest = await db.get_scan(season, latest_date)
        prev   = await db.get_scan(season, prev_date)
        if not latest or not prev:
            return None
        latest_title, prev_title = latest_date.isoformat(), prev_date.isoformat()
 
    if not include_excluded:
        latest = strip_excluded(latest, "lord_id")
        prev   = strip_excluded(prev, "lord_id")
 
    return {
        "latest": latest,
        "prev": prev,
        "latest_title": latest_title,
        "prev_title": prev_title,
        "label": label,
    }
 
 
def split_args(args, valid_seasons):
    """
    Pull a season and a window out of loose trailing arguments, in any order.
 
        !progress 123 1d
        !progress 123 sos4
        !progress 123 sos4 7d
        !progress 123 7d sos4
 
    Returns (season, window, leftovers).
    """
    season = DEFAULT_SEASON
    window = None
    leftovers = []
 
    for a in args:
        t = str(a).strip().lower()
        if t in valid_seasons:
            season = t
        elif parse_window(t) is not None:
            window = t
        else:
            leftovers.append(a)
 
    return season, window, leftovers

EXCLUDED_IDS = set()
 
 
async def refresh_exclusions():
    global EXCLUDED_IDS
    try:
        EXCLUDED_IDS = await db.load_exclusions()
    except Exception as e:
        print(f"⚠️ Could not load exclusions: {e}")
 
 
def strip_excluded(table, id_col="lord_id"):
    """
    Remove excluded players from a [headers, row, ...] table.
    Returns the table unchanged if there's nothing to strip.
    """
    if not table or not EXCLUDED_IDS:
        return table
 
    headers = table[0]
    lower = [str(h).strip().lower() for h in headers]
    try:
        idx = lower.index(id_col.strip().lower())
    except ValueError:
        return table
 
    return [headers] + [
        r for r in table[1:]
        if idx >= len(r) or str(r[idx]).strip() not in EXCLUDED_IDS
    ]

# =============================================================================
# SERVER 375 EXPORT → DATABASE  (replaces sheet375_ingest.py entirely)
# =============================================================================
# Paste into main.py. Requires the db.py additions from db_375.py.
#
# Workflow: export from the in-game tool with start = SEASON_START_375 and
# end = today, then drop the .xlsx into the admin channel with !ingest375.
# No more pasting into the Google Sheet.
# =============================================================================
 
 
# -----------------------------------------------------------------------------
# 1. CONFIG
# -----------------------------------------------------------------------------
 
DATASET_375 = "s375"
ID_COLUMN_375 = "Character ID"
 
# The date your season started. Every export should use this as its start date
# so the files stay cumulative and comparable. Ingest warns if one doesn't.
SEASON_START = date(2026, 8, 28)
 
# -----------------------------------------------------------------------------
# 3. CACHE
# -----------------------------------------------------------------------------
 
async def load_375_cache():
    """
    bot_cache["375_data"] holds SEASON-TO-DATE values — i.e. the newest export
    exactly as uploaded, since those files are already cumulative.
    Falls back to the live Google Sheet if nothing is stored yet.
    """
    try:
        data = await db.materialize_period(DATASET_375, ID_COLUMN_375)
        if data:
            dates = await db.get_latest_dates(DATASET_375, n=1)
            bot_cache["375_data"] = data
            bot_cache["375_date"] = dates[0] if dates else None
            return
    except Exception as e:
        print(f"⚠️ Could not load 375 data from DB: {e}")
 
    try:
        sheet = await asyncio.to_thread(client.open, SERVER_375_SHEET)
        bot_cache["375_data"] = await asyncio.to_thread(sheet.sheet1.get_all_values)
        bot_cache["375_date"] = None
    except Exception as e:
        print(f"⚠️ Failed to refresh Server 375 sheet: {e}")
 
 
@tasks.loop(minutes=10)
async def fetch_sheets_background():
    try:
        print("🔄 [Background Task] Refreshing caches...")
        await refresh_exclusions()
        await load_375_cache()
        await refresh_season_cache()
        print("✅ [Background Task] Caches refreshed.")
    except Exception as e:
        print(f"❌ [Background Task] Critical Error: {e}")
 
 
# -----------------------------------------------------------------------------
# 4. WINDOWS — the same window argument drives BOTH datasets
# -----------------------------------------------------------------------------
 
async def get_375_window(window=None):
    """
    Return 375 data for a window, in the usual [headers, row, ...] shape.
 
    window=None / "season"  -> the latest export as-is (season to date)
    window="5d"             -> latest MINUS the export from 5 days ago
    window="2026-08-30"     -> latest MINUS that date's export
 
    Returns (data, label) or (None, reason).
    """
    parsed = parse_window(window) if window else None
 
    dates = await db.get_latest_dates(DATASET_375, n=1)
    if not dates:
        return None, "No 375 exports stored yet."
    end_date = dates[0]
 
    if parsed is None or parsed[0] == "season":
        data = await db.materialize_period(DATASET_375, ID_COLUMN_375, end_date=end_date)
        return data, "Season to date"
 
    if parsed[0] == "days":
        target = end_date - timedelta(days=parsed[1])
        label = f"Last {parsed[1]} day{'s' if parsed[1] != 1 else ''}"
    else:
        target = parsed[1]
        label = f"Since {target}"
 
    base_date = await db.nearest_date_on_or_before(DATASET_375, target)
 
    if base_date is None or base_date == end_date:
        data = await db.materialize_period(DATASET_375, ID_COLUMN_375, end_date=end_date)
        return data, "Season to date (not enough 375 history for that window)"
 
    data = await db.materialize_period(
        DATASET_375, ID_COLUMN_375, base_date=base_date, end_date=end_date
    )
    return data, f"{label} ({base_date} → {end_date})"
 
 
async def get_combined_window(season, window=None):
    """
    One call that resolves BOTH datasets for the same window, so a command can
    show scan stats and 375 stats over a matching period.
 
    Returns a dict, or None if the season data isn't available.
    """
    win = await get_window_data(season, window)
    if win is None:
        return None
 
    data_375, label_375 = await get_375_window(window)
 
    win["data_375"] = data_375
    win["label_375"] = label_375
    return win

# -------------------------------------------------------------
# UTC TIME & DATE CHANNEL UPDATER
# -------------------------------------------------------------

# Replace these with the actual IDs of the channels you created
DATE_CHANNEL_ID = 1535650935525609552  # Channel for "Date UTC:M/D/YYYY"
TIME_CHANNEL_ID = 1535650977921900636  # Channel for "Time UTC:HH:MM"

@tasks.loop(minutes=10)
async def update_utc_channels():
    try:
        now_utc = datetime.now(timezone.utc)
        
        # Formats: "📅 Date UTC: 08/08/2026" & "⏰ Time UTC: 14:05"
        date_str = f"📅 Date UTC: {now_utc.strftime('%m/%d/%Y')}"
        time_str = f"⏰ Time UTC: {now_utc.strftime('%H:%M')}"

        date_channel = bot.get_channel(DATE_CHANNEL_ID)
        time_channel = bot.get_channel(TIME_CHANNEL_ID)

        if date_channel and date_channel.name != date_str:
            await date_channel.edit(name=date_str)

        if time_channel and time_channel.name != time_str:
            await time_channel.edit(name=time_str)

    except Exception as e:
        print(f"Error updating UTC time channels: {e}")

# Wait until the bot is completely logged in before starting the task loop
@update_utc_channels.before_loop
async def before_utc_update():
    await bot.wait_until_ready()

# =============================================================================
# REBUILT LEADERBOARDS + MULTI-SERVER MERITS SHEETS
# =============================================================================
# Paste into main.py. Requires lb.py alongside main.py.
#
#     import lb
#
# This REPLACES these commands — delete the old versions:
#     topdeads, lowdeads, totaldeads, topmerits, lowmerits, topkills,
#     topheal, topmana, topinf, lowinf, topcav, lowcav, toparcher,
#     lowarcher, topmage, lowmage, toprssheal, lowrssheal, topbuild,
#     lowbuild, topdest, lowdest, generate_375_leaderboard
#
# Also replaces the ingest375 command with a server-aware version.
# =============================================================================


# -----------------------------------------------------------------------------
# 1. MERITS SHEETS — one dataset per server
# -----------------------------------------------------------------------------
# The in-game tool exports a cumulative merits breakdown per server. Store each
# server under its own dataset key so you can pull 375 and 357 independently.
# -----------------------------------------------------------------------------

ID_COLUMN_MERITS = "Character ID"
SEASON_START = date(2026, 8, 28)


def merits_dataset(server):
    """
    Dataset key for a server's merits export, e.g. 375 -> 's375'.
    Keeps the 's' prefix so the 375 data you already ingested still matches.
    """
    return f"s{server}"


@bot.command(name="ingestmerits", aliases=["im", "ingest375"])
@scan_admin()
async def ingest_merits_cmd(ctx, *args):
    """
    Upload a server's merits export (.xlsx or .csv).

        !ingestmerits                     375, dates from the filename
        !ingestmerits 357                 for server 357
        !ingestmerits 357 2026-09-03      explicit end date
        !ingestmerits 357 2026-08-28 2026-09-03

    Always export with the season start as the start date, so the files stay
    cumulative and windows work.
    """
    async with ctx.typing():
        if not ctx.message.attachments:
            await ctx.send("❌ Attach the export file to the same message.")
            return

        attachment = ctx.message.attachments[0]

        server = lb.DEFAULT_SERVER
        dates_given = []
        for a in args:
            srv = lb.parse_server(a)
            if srv and srv != "all":
                server = srv
                continue
            try:
                dates_given.append(datetime.strptime(a.strip(), "%Y-%m-%d").date())
            except ValueError:
                await ctx.send(f"❌ Didn't understand `{a}`.")
                return

        file_start, file_end = db.date_range_from_filename(attachment.filename)
        if len(dates_given) >= 2:
            period_start, period_end = dates_given[0], dates_given[1]
        elif len(dates_given) == 1:
            period_start, period_end = (file_start or SEASON_START), dates_given[0]
        else:
            period_start = file_start or SEASON_START
            period_end = file_end or datetime.now(UTC).date()

        if period_end < period_start:
            await ctx.send("❌ The end date is before the start date.")
            return

        try:
            raw = await attachment.read()
            headers, rows = db.parse_scan_file(raw, attachment.filename)
            result = await db.ingest_period(
                season=merits_dataset(server),
                period_start=period_start,
                period_end=period_end,
                headers=headers,
                rows=rows,
                id_column=ID_COLUMN_MERITS,
                source_file=attachment.filename,
                ingested_by=str(ctx.author),
            )
        except Exception as e:
            await ctx.send(f"❌ **Ingest failed:** {e}")
            return

        embed = discord.Embed(
            title=f"✅ Merits export stored — {lb.server_label(server)}",
            description=f"**{period_start}** → **{period_end}**",
            color=lb.server_color(server),
        )
        embed.add_field(name="Players", value=f"{result['rows']:,}", inline=True)
        embed.add_field(name="Columns", value=str(result["columns"]), inline=True)

        warnings = []
        if result["replaced"] is not None:
            warnings.append(f"↻ Replaced the existing export for {period_end}.")
        if period_start != SEASON_START:
            warnings.append(
                f"⚠️ Start date is **{period_start}**, expected **{SEASON_START}**. "
                f"Windows need every export to share a start date."
            )
        if warnings:
            embed.add_field(name="Notes", value="\n".join(warnings), inline=False)

        embed.set_footer(text=f"Dataset: {merits_dataset(server)} · !scans {merits_dataset(server)}")
        await ctx.send(embed=embed)

        if server == lb.DEFAULT_SERVER:
            await load_375_cache()


# -----------------------------------------------------------------------------
# 2. WINDOW RESOLUTION for merits datasets
# -----------------------------------------------------------------------------

async def get_merits_window(server, window=None, include_excluded=False):
    """Returns (table, label) for a server's merits data over a window."""
    dataset = merits_dataset(server)
    dates = await db.get_latest_dates(dataset, n=1)
    if not dates:
        return None, f"No merits export stored for {lb.server_label(server)}."
    end_date = dates[0]
 
    parsed = lb.parse_window(window) if window else None
 
    if parsed is None or parsed[0] == "season":
        data = await db.materialize_period(dataset, ID_COLUMN_MERITS, end_date=end_date)
        label = f"Season to date · through {end_date}"
    else:
        if parsed[0] == "days":
            target = end_date - timedelta(days=parsed[1])
            label = f"Last {parsed[1]} day{'s' if parsed[1] != 1 else ''}"
        else:
            target = parsed[1]
            label = f"Since {target}"
 
        base_date = await db.nearest_date_on_or_before(dataset, target)
        if base_date is None or base_date == end_date:
            data = await db.materialize_period(dataset, ID_COLUMN_MERITS, end_date=end_date)
            label = "Season to date · not enough history for that window"
        else:
            data = await db.materialize_period(
                dataset, ID_COLUMN_MERITS, base_date=base_date, end_date=end_date
            )
            label = f"{label} · {base_date} → {end_date}"
 
    if data and not include_excluded:
        data = strip_excluded(data, ID_COLUMN_MERITS)
 
    return data, label

# -----------------------------------------------------------------------------
# 3. SCAN LEADERBOARDS — one engine, thin wrappers
# -----------------------------------------------------------------------------

async def run_scan_leaderboard(ctx, args, *, title, emoji, value, unit="",
                               top=True, min_power=25_000_000,
                               detail=None, default_server=lb.DEFAULT_SERVER):
    """
    Generic scan-based leaderboard.

    value: column name, list of columns (summed), or callable(get) -> int
    detail: optional fn(entry) -> str, rendered under each line
    """
    if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
        mentions = ", ".join(f"<#{c}>" for c in ALLOWED_COMMAND_CHANNEL_ID)
        await ctx.send(f"❌ Commands are only allowed in {mentions}.")
        return

    async with ctx.typing():
        opts, unknown = lb.parse_args(
            args, SEASON_SHEETS, DEFAULT_SEASON, default_server=default_server
        )
        if unknown:
            await ctx.send(
                f"❌ Didn't understand `{unknown[0]}`.\n"
                f"Servers: {', '.join(lb.SERVER_NAMES)} or `all` · "
                f"Windows: `7d`, `2w`, `season` · Seasons: {', '.join(SEASON_SHEETS)}"
            )
            return

        win = await get_window_data(opts["season"], opts["window"])
        if win is None:
            await ctx.send(
                f"❌ Not enough scan history. Check `!scans {opts['season']}`."
            )
            return

        gains = lb.materialize_gains(win["latest"], win["prev"], id_col="lord_id")

        try:
            entries, total = lb.rank_table(
                gains,
                value,
                server=opts["server"],
                min_power=min_power,
                top=top,
                limit=opts["limit"],
            )
        except ValueError as e:
            await ctx.send(f"❌ {e}")
            return

        if not entries:
            await ctx.send(
                f"📭 No players matched — {lb.server_label(opts['server'])}, "
                f"≥{lb.fmt(min_power)} power."
            )
            return

        direction = "Top" if top else "Lowest"
        subtitle = (
            f"**{lb.server_label(opts['server'])}** · {win['label']}\n"
            f"*≥{lb.fmt(min_power)} power · {total:,} players eligible*"
        )
        footer = f"{win['prev_title']} → {win['latest_title']}"

        await lb.send_leaderboard(
            ctx,
            title=f"{emoji} {direction} {len(entries)} — {title}",
            subtitle=subtitle,
            footer=footer,
            color=lb.server_color(opts["server"]),
            entries=entries,
            unit=unit,
            show_detail=detail,
        )



# --- Scan command wrappers ---------------------------------------------------

@bot.command(aliases=["td"])
async def topdeads(ctx, *args):
    """!topdeads [server] [count] [window] — e.g. !topdeads 357 50 7d"""
    await run_scan_leaderboard(ctx, args, title="Dead Units", emoji="💀",
                               value="units_dead", top=True)


@bot.command(aliases=["ld"])
async def lowdeads(ctx, *args):
    await run_scan_leaderboard(ctx, args, title="Dead Units", emoji="🔻",
                               value="units_dead", top=False,
                               min_power=50_000_000)


@bot.command(aliases=["tm"])
async def topmerits(ctx, *args):
    """!topmerits [server] [count] [window] — e.g. !topmerits 357 50"""
    await run_scan_leaderboard(ctx, args, title="Merits", emoji="🧠",
                               value="merits", top=True)


@bot.command(aliases=["lm"])
async def lowmerits(ctx, *args):
    await run_scan_leaderboard(ctx, args, title="Merits", emoji="🔻",
                               value="merits", top=False,
                               min_power=50_000_000)


@bot.command(aliases=["tk"])
async def topkills(ctx, *args):
    await run_scan_leaderboard(ctx, args, title="Kills", emoji="⚔️",
                               value="units_killed", top=True)


@bot.command(aliases=["th"])
async def topheal(ctx, *args):
    await run_scan_leaderboard(ctx, args, title="Units Healed", emoji="❤️",
                               value="units_healed", top=True)


@bot.command(aliases=["tmana"])
async def topmana(ctx, *args):
    await run_scan_leaderboard(ctx, args, title="Mana Gathered", emoji="💧",
                               value="mana", top=True)


@bot.command()
async def topt5(ctx, *args):
    await run_scan_leaderboard(ctx, args, title="T5 Kills", emoji="🟥",
                               value="killcount_t5", top=True)


@bot.command()
async def topefficiency(ctx, *args):
    """Merits per million power — who punches above their weight."""
    def ratio(get):
        power = get("highest_power")
        return round(get("merits") / (power / 1_000_000)) if power else 0

    await run_scan_leaderboard(ctx, args, title="Merits per 1M Power",
                               emoji="📊", value=ratio, top=True,
                               min_power=50_000_000)


# -----------------------------------------------------------------------------
# 4. MERITS-SHEET LEADERBOARDS
# -----------------------------------------------------------------------------

async def run_merits_leaderboard(ctx, args, *, title, emoji, value, unit="",
                                 top=True, min_power=50_000_000, detail=None):
    if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
        mentions = ", ".join(f"<#{c}>" for c in ALLOWED_COMMAND_CHANNEL_ID)
        await ctx.send(f"❌ Commands are only allowed in {mentions}.")
        return

    async with ctx.typing():
        opts, unknown = lb.parse_args(args, SEASON_SHEETS, DEFAULT_SEASON)
        if unknown:
            await ctx.send(f"❌ Didn't understand `{unknown[0]}`.")
            return

        server = opts["server"] or lb.DEFAULT_SERVER   # this dataset is per-server
        data, label = await get_merits_window(server, opts["window"])
        if data is None:
            await ctx.send(f"❌ {label}")
            return

        try:
            entries, total = lb.rank_table(
                data,
                value,
                id_col="Character ID",
                name_col="Character Name",
                power_col="Historical Highest Power",
                server_col=None,
                min_power=min_power,
                top=top,
                limit=opts["limit"],
            )
        except ValueError as e:
            await ctx.send(f"❌ {e}")
            return

        if not entries:
            await ctx.send("📭 No players matched those filters.")
            return

        direction = "Top" if top else "Lowest"
        subtitle = (
            f"**{lb.server_label(server)}** · {label}\n"
            f"*≥{lb.fmt(min_power)} power · {total:,} players eligible*"
        )

        await lb.send_leaderboard(
            ctx,
            title=f"{emoji} {direction} {len(entries)} — {title}",
            subtitle=subtitle,
            footer=f"Merits export · {merits_dataset(server)}",
            color=lb.server_color(server),
            entries=entries,
            unit=unit,
            show_detail=detail,
        )


# --- Merits command wrappers -------------------------------------------------

@bot.command(aliases=["topinfantry"])
async def topinf(ctx, *args):
    """!topinf [server] [count] [window] — e.g. !topinf 357 25 7d"""
    await run_merits_leaderboard(ctx, args, title="Infantry Merits",
                                 emoji="⚔️", value="Infantry Only")


@bot.command(aliases=["lowinfantry"])
async def lowinf(ctx, *args):
    await run_merits_leaderboard(ctx, args, title="Infantry Merits",
                                 emoji="⚔️", value="Infantry Only", top=False)


@bot.command(aliases=["topcavalry"])
async def topcav(ctx, *args):
    await run_merits_leaderboard(ctx, args, title="Cavalry Merits",
                                 emoji="🐎", value="Cavalry Only")


@bot.command(aliases=["lowcavalry"])
async def lowcav(ctx, *args):
    await run_merits_leaderboard(ctx, args, title="Cavalry Merits",
                                 emoji="🐎", value="Cavalry Only", top=False)


@bot.command(aliases=["topmarksman", "toparchers"])
async def toparcher(ctx, *args):
    await run_merits_leaderboard(ctx, args, title="Archer Merits",
                                 emoji="🏹", value="Marksman Only")


@bot.command(aliases=["lowmarksman", "lowarchers"])
async def lowarcher(ctx, *args):
    await run_merits_leaderboard(ctx, args, title="Archer Merits",
                                 emoji="🏹", value="Marksman Only", top=False)


@bot.command(aliases=["topmagic", "topmages"])
async def topmage(ctx, *args):
    await run_merits_leaderboard(ctx, args, title="Magic Merits",
                                 emoji="🪄", value="Magic Only")


@bot.command(aliases=["lowmagic", "lowmages"])
async def lowmage(ctx, *args):
    await run_merits_leaderboard(ctx, args, title="Magic Merits",
                                 emoji="🪄", value="Magic Only", top=False)


@bot.command(aliases=["toprsshealing", "toprssheals"])
async def toprssheal(ctx, *args):
    await run_merits_leaderboard(ctx, args, title="RSS Healing", emoji="❤️",
                                 value=["T4 Healed", "T5 Healed"])


@bot.command(aliases=["lowrsshealing", "lowrssheals"])
async def lowrssheal(ctx, *args):
    await run_merits_leaderboard(ctx, args, title="RSS Healing", emoji="❤️",
                                 value=["T4 Healed", "T5 Healed"], top=False)


@bot.command(aliases=["topbuildtime"])
async def topbuild(ctx, *args):
    await run_merits_leaderboard(ctx, args, title="Build Time", emoji="🔨",
                                 value="Build Time")


@bot.command(aliases=["lowbuildtime"])
async def lowbuild(ctx, *args):
    await run_merits_leaderboard(ctx, args, title="Build Time", emoji="🔨",
                                 value="Build Time", top=False)


@bot.command(aliases=["topdestruction", "topdestruct"])
async def topdest(ctx, *args):
    await run_merits_leaderboard(ctx, args, title="Destruction", emoji="🧨",
                                 value="Destruction Time")


@bot.command(aliases=["lowdestruction", "lowdestruct"])
async def lowdest(ctx, *args):
    await run_merits_leaderboard(ctx, args, title="Destruction", emoji="🧨",
                                 value="Destruction Time", top=False)


@bot.command(aliases=["topenemy", "toprealmerits"])
async def topreal(ctx, *args):
    """Merits earned against actual enemies."""
    await run_merits_leaderboard(ctx, args, title="Enemy (Real) Merits",
                                 emoji="🎯", value="Enemy Merits")


@bot.command(aliases=["toptraded", "traders"])
async def toptraders(ctx, *args):
    """
    Total Merits minus Enemy Merits — merits NOT earned against enemies.

        !toptraders            375
        !toptraders 357 25     server 357, top 25
        !toptraders 357 7d     last 7 days
    """
    def traded(get):
        return max(0, get("Total Merits") - get("Enemy Merits"))

    def detail(e):
        pct = (e["value"] / e["total"] * 100) if e.get("total") else 0
        return (f"   └ Total `{lb.fmt(e.get('total', 0))}` · "
                f"Enemy `{lb.fmt(e.get('enemy', 0))}` · **{pct:.0f}% traded**")

    if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
        mentions = ", ".join(f"<#{c}>" for c in ALLOWED_COMMAND_CHANNEL_ID)
        await ctx.send(f"❌ Commands are only allowed in {mentions}.")
        return

    async with ctx.typing():
        opts, unknown = lb.parse_args(args, SEASON_SHEETS, DEFAULT_SEASON)
        if unknown:
            await ctx.send(f"❌ Didn't understand `{unknown[0]}`.")
            return

        server = opts["server"] or lb.DEFAULT_SERVER
        data, label = await get_merits_window(server, opts["window"])
        if data is None:
            await ctx.send(f"❌ {label}")
            return

        entries, total = lb.rank_table(
            data, traded,
            id_col="Character ID",
            name_col="Character Name",
            power_col="Historical Highest Power",
            server_col=None,
            min_power=50_000_000,
            top=True,
            limit=opts["limit"],
            drop_zero=True,
            extra_cols={"total": "Total Merits", "enemy": "Enemy Merits"},
        )

        if not entries:
            await ctx.send("📭 No players matched those filters.")
            return

        await lb.send_leaderboard(
            ctx,
            title=f"🤝 Top {len(entries)} — Traded Merits",
            subtitle=(
                f"**{lb.server_label(server)}** · {label}\n"
                f"*Total merits minus enemy merits · {total:,} eligible*"
            ),
            footer=f"Merits export · {merits_dataset(server)}",
            color=lb.server_color(server),
            entries=entries,
            show_detail=detail,
        )

@bot.command(name="exclude")
@scan_admin()
async def exclude_cmd(ctx, lord_id: str, *, reason: str = None):
    """
    Permanently exclude an account from all stats.
 
        !exclude 12345678
        !exclude 12345678 dead account, no activity since July
    """
    lid = lord_id.strip()
    if not lid.isdigit():
        await ctx.send("❌ That doesn't look like a Lord ID.")
        return
 
    # Try to find their name so the confirmation is readable
    name = None
    cached = bot_cache["seasons"].get(DEFAULT_SEASON)
    if cached:
        headers = cached["latest"][0]
        lower = [str(h).strip().lower() for h in headers]
        if "lord_id" in lower and "name" in lower:
            i_id, i_name = lower.index("lord_id"), lower.index("name")
            for r in cached["latest"][1:]:
                if i_id < len(r) and str(r[i_id]).strip() == lid:
                    name = str(r[i_name]).strip() if i_name < len(r) else None
                    break
 
    await db.add_exclusion(lid, reason, str(ctx.author))
    await refresh_exclusions()
 
    embed = discord.Embed(
        title="🚫 Account excluded",
        description=f"**{name or 'Unknown'}** — `{lid}`",
        color=discord.Color.orange(),
    )
    if reason:
        embed.add_field(name="Reason", value=reason, inline=False)
    embed.set_footer(
        text=f"{len(EXCLUDED_IDS)} account(s) excluded · undo with !unexclude {lid}"
    )
    await ctx.send(embed=embed)
 
 
@bot.command(name="unexclude", aliases=["include"])
@scan_admin()
async def unexclude_cmd(ctx, lord_id: str):
    """Put an account back into the stats."""
    lid = lord_id.strip()
    removed = await db.remove_exclusion(lid)
    await refresh_exclusions()
 
    if removed:
        await ctx.send(f"✅ `{lid}` is counted again. "
                       f"({len(EXCLUDED_IDS)} still excluded.)")
    else:
        await ctx.send(f"ℹ️ `{lid}` wasn't on the exclusion list.")
 
 
@bot.command(name="excluded", aliases=["exclusions"])
async def excluded_cmd(ctx):
    """Show the exclusion list."""
    if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
        mentions = ", ".join(f"<#{c}>" for c in ALLOWED_COMMAND_CHANNEL_ID)
        await ctx.send(f"❌ Commands are only allowed in {mentions}.")
        return
 
    rows = await db.list_exclusions()
    if not rows:
        await ctx.send("✅ No accounts are excluded.")
        return
 
    lines = []
    for r in rows:
        reason = f" — *{r['reason']}*" if r["reason"] else ""
        lines.append(f"`{r['lord_id']}`{reason}")
 
    embed = discord.Embed(
        title=f"🚫 Excluded accounts ({len(rows)})",
        description="\n".join(lines[:40]),
        color=discord.Color.orange(),
    )
    if len(rows) > 40:
        embed.description += f"\n*...and {len(rows) - 40} more.*"
    embed.set_footer(text="These are removed from every leaderboard and total.")
    await ctx.send(embed=embed)

@bot.command()
async def mana(ctx, lord_id: str, season: str = DEFAULT_SEASON):
    async with ctx.typing():
        if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
            channels_mentions = ", ".join([f"<#{channel_id}>" for channel_id in ALLOWED_COMMAND_CHANNEL_ID])
            await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
            return
            
    try:
        season = season.lower()
        if season not in SEASON_SHEETS:
            await ctx.send(f"❌ Invalid season. Options: {', '.join(SEASON_SHEETS.keys())}")
            return

        # CACHE CHECK
        if season not in bot_cache["seasons"]:
            await ctx.send("⏳ The bot is currently syncing with Google Sheets. Please try again in a few seconds!")
            return

        # Load from bot memory (Requires update to background task, see below)
        data_latest  = bot_cache["seasons"][season]["latest"]
        data_oldest  = bot_cache["seasons"][season]["oldest"]
        latest_title = bot_cache["seasons"][season]["latest_title"]
        oldest_title = bot_cache["seasons"][season]["oldest_title"]
        
        headers = data_latest[0]
        
        def get_idx(name, default):
            return headers.index(name) if name in headers else default

        id_idx = get_idx("lord_id", 0)
        name_idx = 1
        alliance_idx = 3
        mana_idx = 26
        server_idx = 5 

        def to_int(val):
            if not val: return 0
            try: return int(str(val).replace(',', '').replace('-', '').strip())
            except: return 0

        oldest_lookup = {row[id_idx].strip(): row for row in data_oldest[1:] if len(row) > mana_idx}

        row_latest = next((r for r in data_latest[1:] if len(r) > id_idx and r[id_idx].strip() == lord_id), None)
        row_oldest = oldest_lookup.get(lord_id)

        if not row_latest or not row_oldest:
            await ctx.send("❌ Lord ID not found in both the start and end of this season.")
            return

        s375_gains = []
        for row in data_latest[1:]:
            l_id = row[id_idx].strip()
            if len(row) > server_idx and str(row[server_idx]).strip() == "375":
                old_row = oldest_lookup.get(l_id)
                if old_row:
                    gain = to_int(row[mana_idx]) - to_int(old_row[mana_idx])
                    s375_gains.append((l_id, gain))

        s375_gains.sort(key=lambda x: x[1], reverse=True)
        rank = next((i+1 for i, (lid, _) in enumerate(s375_gains) if lid == lord_id), None)

        mana_gain = to_int(row_latest[mana_idx]) - to_int(row_oldest[mana_idx])
        name = row_latest[name_idx].strip()
        alliance = row_latest[alliance_idx].strip()

        mana_value = round((mana_gain / 250_000_000) * 100)

        embed = discord.Embed(
            title=f"🌿 Mana : {season.upper()}",
            description=f"Total gain from **{oldest_title}** to **{latest_title}**",
            color=discord.Color.blue()
        )
        embed.add_field(name="Lord", value=f"[{alliance}] {name}", inline=True)
        
        embed.add_field(
            name="💧 Mana gathered", 
            value=f"Total: **{mana_gain:,}**\n*You gathered mana worth **${mana_value:,}*** ", 
            inline=False
        )
        
        if rank:
            embed.add_field(name="🏅 NVR Rank", value=f"#{rank} / {len(s375_gains)}", inline=True)
        else:
            embed.set_footer(text="ℹ️ Player is not in NVR.")

        await ctx.send(embed=embed)

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def groupstats(ctx, season: str = DEFAULT_SEASON):
    allowed_channels = {1378735765827358791, 1383515877793595435, 1236059889411952690}
    
    if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
            channels_mentions = ", ".join([f"<#{channel_id}>" for channel_id in ALLOWED_COMMAND_CHANNEL_ID])
            await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
            return

    TEAM_ROSTER = {
        "1038031": "Sun", "1209648": "Moon", "2554608": "Moon", "1268188": "Sun",
        "2283588": "Moon", "3668102": "Sun", "11403527": "Sun", "14685384": "Moon",
        "3238703": "Moon", "1652362": "Sun", "11288601": "Sun", "2771301": "Moon",
        "2069785": "Moon", "11537218": "Sun", "11399110": "Sun", "2360876": "Moon",
        "8981467": "Moon", "4562030": "Sun", "1185328": "Sun", "4019235": "Moon",
        "15384392": "Moon", "14920281": "Sun", "7504081": "Sun", "2176073": "Moon",
        "2411806": "Moon", "4982945": "Sun", "11371644": "Sun", "5605513": "Moon",
        "3569766": "Moon", "3452794": "Sun", "1301820": "Sun", "1178290": "Moon",
        "3600529": "Moon", "4781116": "Sun", "3730372": "Moon", "1355903": "Moon",
        "17409364": "Moon", "12907861": "Sun", "8617664": "Sun", "6409636": "Moon",
        "11372964": "Moon", "14893533": "Sun", "4112915": "Sun", "14721147": "Moon",
        "16633476": "Moon", "3383792": "Sun", "3521913": "Sun", "14721627": "Moon",
        "1890674": "Moon", "1191528": "Sun", "11589778": "Sun", "15529642": "Moon",
        "12121490": "Moon", "14625955": "Sun", "1903216": "Sun", "1421566": "Moon",
        "1379913": "Moon", "2217685": "Sun", "1442822": "Sun", "11769711": "Moon",
        "16322115": "Moon", "15719441": "Sun", "11312335": "Sun", "93496": "Moon",
        "16007668": "Moon", "1327811": "Sun", "4942439": "Sun", "11487055": "Moon",
        "11659353": "Moon", "3005418": "Sun", "8365897": "Sun", "3154267": "Moon",
        "3884083": "Moon", "12913373": "Sun", "8167052": "Sun", "1358230": "Moon",
        "15168167": "Moon", "8344083": "Sun", "12867862": "Sun", "5710153": "Moon",
        "1475373": "Sun", "1896011": "Sun", "3665158": "Sun", "8498158": "Moon",
        "1480794": "Moon", "7871135": "Sun", "14855893": "Sun", "12239902": "Moon",
        "921581": "Moon", "10026132": "Sun", "12391559": "Sun", "11018782": "Moon",
        "11409242": "Moon", "12861502": "Sun", "3911741": "Sun", "8654500": "Moon",
        "15406991": "Sun", "1201472": "Sun", "2102190": "Moon", "2355170": "Moon",
        "12426797": "Sun", "12049853": "Sun", "15203473": "Moon", "2899559": "Moon",
        "1159399": "Sun", "12049278": "Sun", "7514081": "Moon", "1955276": "Moon",
        "11599446": "Sun", "14892554": "Sun", "11516385": "Moon", "16497032": "Moon",
        "15996144": "Sun", "12054525": "Sun", "7979635": "Moon", "7298996": "Moon",
        "1727336": "Sun", "11648388": "Sun", "11529501": "Moon", "3937721": "Moon",
        "6554196": "Sun", "19300504": "Sun", "12993192": "Moon", "1240639": "Moon",
        "12600393": "Sun", "3324298": "Sun", "3763091": "Moon", "15985931": "Moon",
        "1930701": "Sun", "3446240": "Sun", "9561066": "Moon", "12581309": "Moon",
        "15140100": "Sun", "9556439": "Sun", "1191427": "Moon", "3571729": "Moon",
        "11939697": "Sun", "11042149": "Sun", "9076185": "Moon", "12672252": "Moon",
        "11306195": "Sun", "12451416": "Sun", "11597010": "Moon", "3550420": "Moon",
        "15238376": "Sun", "11434627": "Sun", "1434504": "Moon", "537109": "Moon",
        "3453241": "Sun", "12909862": "Sun", "11042696": "Moon", "11491223": "Moon",
        "18032877": "Sun", "13255722": "Sun", "16053174": "Moon", "20773329": "Moon",
        "14249731": "Sun", "15988260": "Sun", "16024377": "Moon", "5751068": "Moon",
        "15500649": "Sun", "20781093": "Moon", "15976283": "Moon"
    }
    
    try:
        season = season.lower()
        
        # Check if synced in cache
        if season not in bot_cache["seasons"]:
            await ctx.send("⏳ The bot is currently syncing with Google Sheets. Please try again in a few seconds!")
            return

        # Load from bot memory instantly
        data_latest = bot_cache["seasons"][season]["latest"]
        data_prev   = bot_cache["seasons"][season]["prev"]
        latest_title = bot_cache["seasons"][season]["latest_title"]
        prev_title   = bot_cache["seasons"][season]["prev_title"]
        headers = data_latest[0]

        def find_idx(name, fallback):
            if name in headers: return headers.index(name)
            for i, h in enumerate(headers):
                if name.lower() in h.lower(): return i
            return fallback

        def to_int(val):
            try:
                v = str(val).replace(',', '').replace(' ', '').strip()
                return int(v) if v not in ("", "-") else 0
            except: return 0

        # Indices (Using robust header lookups matching your working commands)
        id_idx     = find_idx("lord_id", 0)
        name_idx   = find_idx("name", 1)
        power_idx  = find_idx("highest_power", 2)
        kills_idx  = find_idx("units_killed", 9) 
        merits_idx = find_idx("merits", 11) 
        heal_idx   = find_idx("units_healed", 18)
        dead_idx   = find_idx("units_dead", 17)

        max_needed_idx = max(heal_idx, kills_idx, merits_idx, power_idx, dead_idx)

        prev_map = {
            str(row[id_idx]).strip(): row for row in data_prev[1:]
            if len(row) > max_needed_idx and str(row[id_idx]).strip()
        }

        group_data = {
            "Sun":  {"power": 0, "kills": 0, "deads": 0, "heals": 0, "merits": 0, "players": []},
            "Moon": {"power": 0, "kills": 0, "deads": 0, "heals": 0, "merits": 0, "players": []}
        }

        for row in data_latest[1:]:
            if len(row) <= max_needed_idx: continue
            lid = (row[id_idx] or "").strip()
            group = TEAM_ROSTER.get(lid)
            if not group: continue 

            prev_row = prev_map.get(lid)
            if prev_row is None: continue

            # Safely calculate gains with a floor of 0 to prevent sheet correction bugs
            kills_gain  = max(0, to_int(row[kills_idx]) - to_int(prev_row[kills_idx]))
            deads_gain  = max(0, to_int(row[dead_idx]) - to_int(prev_row[dead_idx]))
            heals_gain  = max(0, to_int(row[heal_idx]) - to_int(prev_row[heal_idx]))
            merits_gain = max(0, to_int(row[merits_idx]) - to_int(prev_row[merits_idx]))

            p_gain = {
                "name": row[name_idx],
                "power": to_int(row[power_idx]),
                "kills": kills_gain,
                "deads": deads_gain,
                "heals": heals_gain,
                "merits": merits_gain
            }

            g_stats = group_data[group]
            g_stats["power"]  += p_gain["power"]
            g_stats["kills"]  += p_gain["kills"]
            g_stats["deads"]  += p_gain["deads"]
            g_stats["heals"]  += p_gain["heals"]
            g_stats["merits"] += p_gain["merits"]
            g_stats["players"].append(p_gain)

        # UI FORMATTING
        def format_group_section(name, emoji, stats):
            power = stats["power"]
            merits = stats["merits"]
            efficiency = (merits / power * 100) if power > 0 else 0
            
            def fmt(num):
                if num >= 1_000_000_000: return f"{num / 1_000_000_000:.2f}B"
                elif num >= 1_000_000: return f"{num / 1_000_000:.2f}M"
                elif num >= 1_000: return f"{num / 1_000:.1f}K"
                return str(num)
            
            stats_block = (
                f"```yaml\n"
                f"Power:  {fmt(power)}\n"
                f"Merits: {fmt(merits)}\n"
                f"Kills:  {fmt(stats['kills'])}\n"
                f"Deads:  {fmt(stats['deads'])}\n"
                f"Heals:  {fmt(stats['heals'])}\n"
                f"Eff:    {efficiency:.2f}%\n"
                f"```"
            )

            # Top Performers Block
            top_players = sorted(stats["players"], key=lambda x: x["merits"], reverse=True)[:3]
            medals = ["🥇", "🥈", "🥉"]
            top_str = ""
            for i, p in enumerate(top_players):
                raw_name = p['name']
                display_name = raw_name[:12] + ".." if len(raw_name) > 12 else raw_name
                
                if i == 1: rank_icon = "🥈"
                elif i == 2: rank_icon = "🥉"
                else: rank_icon = "🥇" # Fix index mapping for medals loop
                
                # Using proper medal ordering based on enumerate index
                medal_icon = medals[i] if i < len(medals) else "▫️"

                top_str += f"{medals[i]} **{display_name}**\n└ `{fmt(p['merits'])}` Merits\n"

            return f"{emoji} __**GROUP {name.upper()}**__", stats_block, top_str

        # Create Single Embed
        embed = discord.Embed(
            title="📊 Group Stats - Sun vs Moon",
            description=f"**Comparing:** `{prev_title}` ➔ `{latest_title}`\n" + "▬" * 15,
            color=0x2f3136
        )

        # Sun Group Fields
        title_s, stats_s, top_s = format_group_section("Sun", "☀️", group_data["Sun"])
        embed.add_field(name=title_s, value=stats_s, inline=True)
        embed.add_field(name="⭐ TOP PERFORMERS", value=top_s, inline=True)
        
        # Spacer Field
        embed.add_field(name="\u200b", value="▬" * 30, inline=False)

        # Moon Group Fields
        title_m, stats_m, top_m = format_group_section("Moon", "🌙", group_data["Moon"])
        embed.add_field(name=title_m, value=stats_m, inline=True)
        embed.add_field(name="⭐ TOP PERFORMERS", value=top_m, inline=True)

        embed.set_footer(text="If you read this, sun sucks.")
        embed.timestamp = datetime.now(UTC) 

        await ctx.send(embed=embed)

    except Exception as e:
        await ctx.send(f"❌ **Error:** {e}")
        
@bot.command(aliases=['grouplb', 'gl'])
async def groupleaderboard(ctx, season: str = DEFAULT_SEASON):
    async with ctx.typing():
        allowed_channels = {1378735765827358791, 1383515877793595435, 1236059889411952690}

        if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
            channels_mentions = ", ".join([f"<#{channel_id}>" for channel_id in ALLOWED_COMMAND_CHANNEL_ID])
            await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
            return

        TEAM_ROSTER = {
            "1038031": "Sun", "1209648": "Moon", "2554608": "Moon", "1268188": "Sun",
            "2283588": "Moon", "3668102": "Sun", "11403527": "Sun", "14685384": "Moon",
            "3238703": "Moon", "1652362": "Sun", "11288601": "Sun", "2771301": "Moon",
            "2069785": "Moon", "11537218": "Sun", "11399110": "Sun", "2360876": "Moon",
            "8981467": "Moon", "4562030": "Sun", "1185328": "Sun", "4019235": "Moon",
            "15384392": "Moon", "14920281": "Sun", "7504081": "Sun", "2176073": "Moon",
            "2411806": "Moon", "4982945": "Sun", "11371644": "Sun", "5605513": "Moon",
            "3569766": "Moon", "3452794": "Sun", "1301820": "Sun", "1178290": "Moon",
            "3600529": "Moon", "4781116": "Sun", "3730372": "Moon", "1355903": "Moon",
            "17409364": "Moon", "12907861": "Sun", "8617664": "Sun", "6409636": "Moon",
            "11372964": "Moon", "14893533": "Sun", "4112915": "Sun", "14721147": "Moon",
            "16633476": "Moon", "3383792": "Sun", "3521913": "Sun", "14721627": "Moon",
            "1890674": "Moon", "1191528": "Sun", "11589778": "Sun", "15529642": "Moon",
            "12121490": "Moon", "14625955": "Sun", "1903216": "Sun", "1421566": "Moon",
            "1379913": "Moon", "2217685": "Sun", "1442822": "Sun", "11769711": "Moon",
            "16322115": "Moon", "15719441": "Sun", "11312335": "Sun", "93496": "Moon",
            "16007668": "Moon", "1327811": "Sun", "4942439": "Sun", "11487055": "Moon",
            "11659353": "Moon", "3005418": "Sun", "8365897": "Sun", "3154267": "Moon",
            "3884083": "Moon", "12913373": "Sun", "8167052": "Sun", "1358230": "Moon",
            "15168167": "Moon", "8344083": "Sun", "12867862": "Sun", "5710153": "Moon",
            "1475373": "Sun", "1896011": "Sun", "3665158": "Sun", "8498158": "Moon",
            "1480794": "Moon", "7871135": "Sun", "14855893": "Sun", "12239902": "Moon",
            "921581": "Moon", "10026132": "Sun", "12391559": "Sun", "11018782": "Moon",
            "11409242": "Moon", "12861502": "Sun", "3911741": "Sun", "8654500": "Moon",
            "15406991": "Sun", "1201472": "Sun", "2102190": "Moon", "2355170": "Moon",
            "12426797": "Sun", "12049853": "Sun", "15203473": "Moon", "2899559": "Moon",
            "1159399": "Sun", "12049278": "Sun", "7514081": "Moon", "1955276": "Moon",
            "11599446": "Sun", "14892554": "Sun", "11516385": "Moon", "16497032": "Moon",
            "15996144": "Sun", "12054525": "Sun", "7979635": "Moon", "7298996": "Moon",
            "1727336": "Sun", "11648388": "Sun", "11529501": "Moon", "3937721": "Moon",
            "6554196": "Sun", "19300504": "Sun", "12993192": "Moon", "1240639": "Moon",
            "12600393": "Sun", "3324298": "Sun", "3763091": "Moon", "15985931": "Moon",
            "1930701": "Sun", "3446240": "Sun", "9561066": "Moon", "12581309": "Moon",
            "15140100": "Sun", "9556439": "Sun", "1191427": "Moon", "3571729": "Moon",
            "11939697": "Sun", "11042149": "Sun", "9076185": "Moon", "12672252": "Moon",
            "11306195": "Sun", "12451416": "Sun", "11597010": "Moon", "3550420": "Moon",
            "15238376": "Sun", "11434627": "Sun", "1434504": "Moon", "537109": "Moon",
            "3453241": "Sun", "12909862": "Sun", "11042696": "Moon", "11491223": "Moon",
            "18032877": "Sun", "13255722": "Sun", "16053174": "Moon", "20773329": "Moon",
            "14249731": "Sun", "15988260": "Sun", "16024377": "Moon", "5751068": "Moon",
            "15500649": "Sun", "20781093": "Moon", "15976283": "Moon"
        }

        try:
            season = season.lower()

            if season not in bot_cache["seasons"] or bot_cache.get("375_data") is None:
                await ctx.send("⏳ The bot is currently syncing with Google Sheets. Please try again in a few seconds!")
                return

            data_latest = bot_cache["seasons"][season]["latest"]
            data_prev   = bot_cache["seasons"][season]["prev"]
            latest_title = bot_cache["seasons"][season]["latest_title"]
            prev_title   = bot_cache["seasons"][season]["prev_title"]
            headers = data_latest[0]

            def find_idx(name):
                for i, h in enumerate(headers):
                    if name.lower() in h.lower():
                        return i
                raise ValueError(f"Required column matching '{name}' not found.")

            def to_int(val):
                try:
                    v = str(val).replace(',', '').replace(' ', '').strip()
                    return int(v) if v not in ("", "-") else 0
                except:
                    return 0

            id_idx     = find_idx("lord_id")
            name_idx   = find_idx("name")
            merits_idx = find_idx("merits")
            dead_idx   = find_idx("units_dead")
            max_needed_idx = max(id_idx, name_idx, merits_idx, dead_idx)

            prev_map = {
                str(row[id_idx]).strip(): row for row in data_prev[1:]
                if len(row) > max_needed_idx and str(row[id_idx]).strip()
            }

            data_375 = bot_cache["375_data"]
            headers_375 = data_375[0]

            id_col_375 = headers_375.index("Character ID")
            inf_col_375 = headers_375.index("Infantry Only")

            inf_map = {}
            for r in data_375[1:]:
                if len(r) > max(id_col_375, inf_col_375):
                    inf_map[str(r[id_col_375]).strip()] = to_int(r[inf_col_375])

            sun_players = []
            moon_players = []

            for row in data_latest[1:]:
                if len(row) <= max_needed_idx:
                    continue

                lid = str(row[id_idx] or "").strip()
                group = TEAM_ROSTER.get(lid)
                if not group:
                    continue

                prev_row = prev_map.get(lid)
                if prev_row is None:
                    continue

                merits_gain = max(0, to_int(row[merits_idx]) - to_int(prev_row[merits_idx]))
                deads_gain  = max(0, to_int(row[dead_idx]) - to_int(prev_row[dead_idx]))
                inf_val = inf_map.get(lid, 0)

                score = (merits_gain * 1) + (inf_val * 2) + (deads_gain * 5)

                p_data = {
                    "name": row[name_idx],
                    "score": score,
                    "merits": merits_gain,
                    "infantry": inf_val,
                    "deads": deads_gain
                }

                if group == "Sun":
                    sun_players.append(p_data)
                elif group == "Moon":
                    moon_players.append(p_data)

            sun_players.sort(key=lambda x: x["score"], reverse=True)
            moon_players.sort(key=lambda x: x["score"], reverse=True)

            def fmt(num):
                if num >= 1_000_000:
                    return f"{num/1_000_000:.2f}M"
                elif num >= 1_000:
                    return f"{num/1_000:.1f}K"
                return str(num)

            RANK_ICONS = {1: "🥇", 2: "🥈", 3: "🥉"}

            def build_team_description(team_players):
                """Plain markdown list instead of a code block — code blocks render
                emoji at a different width than text, which is what was causing the
                jagged/misaligned columns in the old output."""
                if not team_players:
                    return "*No qualifying players this period.*"
                lines = []
                for i, p in enumerate(team_players[:10], 1):
                    icon = RANK_ICONS.get(i, f"`#{i}`")
                    name = str(p['name'])[:18]
                    lines.append(f"{icon} **{name}** — `{fmt(p['score'])} pts`")
                    lines.append(f"> 🧠 {fmt(p['merits'])}  ⚔️ {fmt(p['infantry'])}  💀 {fmt(p['deads'])}")
                return "\n".join(lines)

            sun_total = sum(p['score'] for p in sun_players)
            moon_total = sum(p['score'] for p in moon_players)

            if sun_total > moon_total:
                lead_text = f"🌞 Sun leads by **{fmt(sun_total - moon_total)}**"
            elif moon_total > sun_total:
                lead_text = f"🌙 Moon leads by **{fmt(moon_total - sun_total)}**"
            else:
                lead_text = "⚖️ Dead even"

            # --- Header embed: comparison window, scoring key, overall lead ---
            header_embed = discord.Embed(
                title="🏆 Group Leaderboard — Sun vs Moon",
                description=(
                    f"**{prev_title}**  ➜  **{latest_title}**\n"
                    f"Scoring: 🧠 Merits ×1 · ⚔️ Infantry ×2 · 💀 Deads ×5\n\n"
                    f"{lead_text}"
                ),
                color=0x2F3136
            )
            header_embed.set_footer(
                text=f"Top 10 shown · {len(sun_players)} Sun · {len(moon_players)} Moon tracked"
            )
            header_embed.timestamp = datetime.now(UTC)

            # --- One colored embed per team instead of two fields crammed into one ---
            sun_embed = discord.Embed(
                title=f"🌞 Team Sun — {fmt(sun_total)} pts",
                description=build_team_description(sun_players),
                color=0xF5A623  # gold
            )

            moon_embed = discord.Embed(
                title=f"🌙 Team Moon — {fmt(moon_total)} pts",
                description=build_team_description(moon_players),
                color=0x5865F2  # indigo
            )

            # NOTE: sending multiple embeds in one message requires discord.py >= 2.0
            await ctx.send(embeds=[header_embed, sun_embed, moon_embed])

        except Exception as e:
            await ctx.send(f"❌ **Error:** {e}")
        
@bot.command()
async def kills(ctx, lord_id: str, season: str = DEFAULT_SEASON):
    try:
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season)
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Options: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = await asyncio.to_thread(client.open(sheet_name).worksheets)
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = tabs[-1]
        previous = tabs[-2]

        data_latest = latest.get_all_values()
        data_prev = previous.get_all_values()
        headers = data_latest[0]

        id_index = headers.index("lord_id")
        name_index = 1
        alliance_index = 3
        power_index = 12

        total_idx = 9    # Column J
        t5_idx = 36      # Column AK
        t4_idx = 37      # Column AL
        t3_idx = 38      # Column AM
        t2_idx = 39      # Column AN
        t1_idx = 40      # Column AO

        def to_int(val):
            try:
                return int(val.replace(',', '').replace('-', '').strip())
            except:
                return 0

        def find_row(data):
            for row in data[1:]:
                if row[id_index] == lord_id:
                    return row
            return None

        row_latest = find_row(data_latest)
        row_prev = find_row(data_prev)

        if not row_latest or not row_prev:
            await ctx.send("❌ Lord ID not found in both sheets.")
            return

        power = to_int(row_latest[power_index])
        if power < 25_000_000:
            await ctx.send("❌ Player is below 25M power.")
            return

        name = row_latest[name_index].strip()
        alliance = row_latest[alliance_index].strip()
        tag = f"[{alliance}] {name}"

        def get_diff(idx):
            return to_int(row_latest[idx]) - to_int(row_prev[idx])

        def get_now(idx):
            return to_int(row_latest[idx])

        total = get_now(total_idx)
        total_diff = get_diff(total_idx)
        t5 = get_now(t5_idx)
        t5_diff = get_diff(t5_idx)
        t4 = get_now(t4_idx)
        t4_diff = get_diff(t4_idx)
        t3 = get_now(t3_idx)
        t3_diff = get_diff(t3_idx)
        t2 = get_now(t2_idx)
        t2_diff = get_diff(t2_idx)
        t1 = get_now(t1_idx)
        t1_diff = get_diff(t1_idx)

        await ctx.send(
            f"📊 **Kill Stats for `{tag}`**\n"
            f"`{previous.title}` → `{latest.title}`\n\n"
            f"⚔️ **Total:** {total:,} (+{total_diff:,})\n"
            f"T5: {t5:,} (+{t5_diff:,})\n"
            f"T4: {t4:,} (+{t4_diff:,})\n"
            f"T3: {t3:,} (+{t3_diff:,})\n"
            f"T2: {t2:,} (+{t2_diff:,})\n"
            f"T1: {t1:,} (+{t1_diff:,})"
        )

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command(aliases=['xppvsraz', 'duel2'])
async def duel_xpp_raz(ctx, season: str = DEFAULT_SEASON):
    """Custom asymmetric 1v1 Challenge: xpp vs raz"""
    if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
        channels_mentions = ", ".join([f"<#{c}>" for c in ALLOWED_COMMAND_CHANNEL_ID])
        await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
        return

    async with ctx.typing():
        try:
            season = season.lower()
            if season not in SEASON_SHEETS:
                await ctx.send(f"❌ Invalid season. Options: {', '.join(SEASON_SHEETS.keys())}")
                return

            # 1. CACHE CHECK: Ensure background task has synced both sheets
            if season not in bot_cache["seasons"] or bot_cache.get("375_data") is None:
                await ctx.send("⏳ The bot is currently syncing with Google Sheets. Please try again in a few seconds!")
                return

            # 2. LOAD DATA DIRECTLY FROM CACHE
            data_latest = bot_cache["seasons"][season]["latest"]
            data_prev   = bot_cache["seasons"][season]["prev"]
            latest_title = bot_cache["seasons"][season]["latest_title"]
            prev_title   = bot_cache["seasons"][season]["prev_title"]
            headers = data_latest[0]

            id_idx = headers.index("lord_id") if "lord_id" in headers else 0
            name_idx = 1
            merits_idx = headers.index("merits")

            prev_map = {row[id_idx].strip(): row for row in data_prev[1:] if len(row) > merits_idx}

            # 3. LOAD SERVER 375 DATA FROM CACHE (For Magic)
            data_375 = bot_cache["375_data"]
            headers_375 = data_375[0]
            
            id_col_375 = headers_375.index("Character ID")
            magic_col_375 = headers_375.index("Magic Only")

            def to_int(v):
                try:
                    val = str(v).replace(",", "").strip()
                    return int(val) if val not in ("-", "") else 0
                except:
                    return 0
            
            # Map Character ID -> Magic Merits
            magic_map = {str(r[id_col_375]).strip(): to_int(r[magic_col_375]) for r in data_375[1:] if len(r) > magic_col_375}

            # 4. CONTENDERS SETUP
            p1_id = "15500649" # xpp
            p2_id = "5751068"  # raz
            
            contenders = {
                p1_id: {"display": "xpp", "emoji": "🔵"},
                p2_id: {"display": "raz", "emoji": "🔴"}
            }
            
            results = {}

            # 5. CALCULATE SCORES
            for row in data_latest[1:]:
                lid = str(row[id_idx]).strip()
                if lid in contenders:
                    prev_row = prev_map.get(lid)
                    if not prev_row: continue
                    
                    merit_gain = to_int(row[merits_idx]) - to_int(prev_row[merits_idx])
                    magic_total = magic_map.get(lid, 0)
                    
                    # Asymmetric Scoring Logic
                    if lid == p1_id:
                        # xpp: Base Merits (1x) + Magic Total (adds the extra 1x to equal 2x mages)
                        score = merit_gain + magic_total
                    else:
                        # raz: Base Merits only (1x everything)
                        score = merit_gain
                    
                    results[lid] = {
                        "name": row[name_idx],
                        "merit_gain": merit_gain,
                        "magic_total": magic_total,
                        "score": score
                    }

            if len(results) < 2:
                await ctx.send("❌ Could not find both contenders in the current scan data.")
                return

            # 6. DETERMINE LEADER
            p1_data = results[p1_id]
            p2_data = results[p2_id]
            
            if p1_data["score"] > p2_data["score"]:
                leader_text = f"🏆 **{contenders[p1_id]['display']}** is leading by **{p1_data['score'] - p2_data['score']:,}** pts!"
                color = discord.Color.blue()
            elif p2_data["score"] > p1_data["score"]:
                leader_text = f"🏆 **{contenders[p2_id]['display']}** is leading by **{p2_data['score'] - p1_data['score']:,}** pts!"
                color = discord.Color.red()
            else:
                leader_text = "⚖️ **IT'S A PERFECT TIE!**"
                color = discord.Color.gold()

            # Calculate 10-block Tug-of-War Bar
            total_points = p1_data["score"] + p2_data["score"]
            if total_points > 0:
                p1_blocks = round((p1_data["score"] / total_points) * 10)
                p2_blocks = 10 - p1_blocks
                tug_of_war = ("🟦" * p1_blocks) + ("🟥" * p2_blocks)
            else:
                tug_of_war = "⬛" * 10

            # 7. BUILD UI
            embed = discord.Embed(
                title="⚔️ THE DUEL: xpp vs raz ⚔️",
                description=f"*Rules: xpp (Mages 2x, Rest 1x) | raz (All 1x)*\n\n{leader_text}\n{tug_of_war}\n" + "▬" * 15,
                color=color
            )
            
            def fmt(num): return f"{num:,}"

            # xpp's field (shows the magic bonus)
            embed.add_field(
                name=f"{contenders[p1_id]['emoji']} {p1_data['name']}",
                value=(
                    f"**Score:** `{fmt(p1_data['score'])}` pts\n"
                    f"├ Merits: {fmt(p1_data['merit_gain'])}\n"
                    f"└ Magic (2x): +{fmt(p1_data['magic_total'])}"
                ),
                inline=True
            )
            
            embed.add_field(name="🆚", value="\u200b\n\u200b", inline=True) # Spacer

            # raz's field (standard score)
            embed.add_field(
                name=f"{contenders[p2_id]['emoji']} {p2_data['name']}",
                value=(
                    f"**Score:** `{fmt(p2_data['score'])}` pts\n"
                    f"└ Merits: {fmt(p2_data['merit_gain'])}\n"
                    f"*(Standard 1x rules)*"
                ),
                inline=True
            )
            
            # Using the cached titles for the footer
            embed.set_footer(text=f"Comparing: {prev_title} → {latest_title}")
            await ctx.send(embed=embed)

        except Exception as e:
            await ctx.send(f"❌ **Error generating duel:** {e}")

@bot.command(aliases=['mage1v1', 'duel', 'challenge'])
async def duel_challenge(ctx, season: str = DEFAULT_SEASON):
    """Custom 1v1 Challenge: Tinzy vs Balakas"""
    if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
        channels_mentions = ", ".join([f"<#{c}>" for c in ALLOWED_COMMAND_CHANNEL_ID])
        await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
        return

    async with ctx.typing():
        try:
            season = season.lower()
            if season not in SEASON_SHEETS:
                await ctx.send(f"❌ Invalid season. Options: {', '.join(SEASON_SHEETS.keys())}")
                return

            # 1. CACHE CHECK: Ensure background task has synced both sheets
            if season not in bot_cache["seasons"] or bot_cache.get("375_data") is None:
                await ctx.send("⏳ The bot is currently syncing with Google Sheets. Please try again in a few seconds!")
                return

            # 2. LOAD DATA DIRECTLY FROM CACHE
            data_latest = bot_cache["seasons"][season]["latest"]
            data_prev   = bot_cache["seasons"][season]["prev"]
            latest_title = bot_cache["seasons"][season]["latest_title"]
            prev_title   = bot_cache["seasons"][season]["prev_title"]
            headers = data_latest[0]

            id_idx = headers.index("lord_id") if "lord_id" in headers else 0
            name_idx = 1
            merits_idx = headers.index("merits")

            prev_map = {row[id_idx].strip(): row for row in data_prev[1:] if len(row) > merits_idx}

            # 3. LOAD SERVER 375 DATA FROM CACHE (For Infantry)
            data_375 = bot_cache["375_data"]
            headers_375 = data_375[0]
            
            id_col_375 = headers_375.index("Character ID")
            inf_col_375 = headers_375.index("Infantry Only")
            
            inf_map = {str(r[id_col_375]).strip(): int(str(r[inf_col_375]).replace(",", "").strip() or 0) for r in data_375[1:] if len(r) > inf_col_375}

            # 4. CONTENDERS SETUP
            contenders = {
                "1240639": {"display": "Tinzy", "emoji": "🔵"},
                "12451416": {"display": "BaLaKaS", "emoji": "🔴"}
            }
            
            results = {}

            # 5. CALCULATE SCORES
            for row in data_latest[1:]:
                lid = str(row[id_idx]).strip()
                if lid in contenders:
                    prev_row = prev_map.get(lid)
                    if not prev_row: continue
                    
                    # Math: Merit Gain (1x) + Infantry Total (2x)
                    merit_gain = int(str(row[merits_idx]).replace(",", "").strip() or 0) - int(str(prev_row[merits_idx]).replace(",", "").strip() or 0)
                    inf_total = inf_map.get(lid, 0)
                    
                    score = merit_gain + (inf_total * 2)
                    
                    results[lid] = {
                        "name": row[name_idx],
                        "merit_gain": merit_gain,
                        "inf_total": inf_total,
                        "score": score
                    }

            if len(results) < 2:
                await ctx.send("❌ Could not find both contenders in the current scan data.")
                return

            # 6. DETERMINE LEADER
            p1_id = "1240639"
            p2_id = "12451416"
            p1_data = results[p1_id]
            p2_data = results[p2_id]
            
            if p1_data["score"] > p2_data["score"]:
                leader_text = f"🏆 **{contenders[p1_id]['display']}** is leading by **{p1_data['score'] - p2_data['score']:,}** pts!"
                color = discord.Color.blue()
            elif p2_data["score"] > p1_data["score"]:
                leader_text = f"🏆 **{contenders[p2_id]['display']}** is leading by **{p2_data['score'] - p1_data['score']:,}** pts!"
                color = discord.Color.red()
            else:
                leader_text = "⚖️ **IT'S A PERFECT TIE!**"
                color = discord.Color.gold()

            # Calculate Tug-of-War Bar (10 blocks total)
            total_points = p1_data["score"] + p2_data["score"]
            if total_points > 0:
                p1_blocks = round((p1_data["score"] / total_points) * 10)
                p2_blocks = 10 - p1_blocks
                tug_of_war = ("🟦" * p1_blocks) + ("🟥" * p2_blocks)
            else:
                tug_of_war = "⬛" * 10 # Empty bar if both are at 0

            # 7. BUILD UI
            embed = discord.Embed(
                title="⚔️ THE MAGE DUEL ⚔️",
                description=f"*Rules: All Merits (1x) | Infantry Merits (2x)*\n\n{leader_text}\n{tug_of_war}\n" + "━" * 15,
                color=color
            )
            
            # Format numbers helper
            def fmt(num): return f"{num:,}"

            embed.add_field(
                name=f"{contenders[p1_id]['emoji']} {p1_data['name']}",
                value=(
                    f"**Score:** `{fmt(p1_data['score'])}` pts\n"
                    f"└ Merits: {fmt(p1_data['merit_gain'])}\n"
                    f"└ Infantry: {fmt(p1_data['inf_total'])}"
                ),
                inline=True
            )
            
            embed.add_field(name="🆚", value="\u200b\n\u200b", inline=True) # Spacer

            embed.add_field(
                name=f"{contenders[p2_id]['emoji']} {p2_data['name']}",
                value=(
                    f"**Score:** `{fmt(p2_data['score'])}` pts\n"
                    f"└ Merits: {fmt(p2_data['merit_gain'])}\n"
                    f"└ Infantry: {fmt(p2_data['inf_total'])}"
                ),
                inline=True
            )
            
            # Use cached titles
            embed.set_footer(text=f"Comparing: {prev_title} → {latest_title}")
            await ctx.send(embed=embed)

        except Exception as e:
            await ctx.send(f"❌ **Error generating duel:** {e}")

@bot.command()
async def allmana(ctx, season: str = DEFAULT_SEASON):
    """Shows the total mana gathered by the entire alliance and its dollar value."""
    async with ctx.typing():
        
        if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
            # This creates a nicely formatted string of clickable channel links for the error message
            channels_mentions = ", ".join([f"<#{channel_id}>" for channel_id in ALLOWED_COMMAND_CHANNEL_ID])
            await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
            return
    try:
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season)
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Options: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = await asyncio.to_thread(client.open(sheet_name).worksheets)
        if len(tabs) < 2:
            await ctx.send("❌ Need at least two tabs to calculate gain.")
            return

        latest = tabs[-1]
        previous = tabs[-2]
        
        data_latest = latest.get_all_values()
        data_prev = previous.get_all_values()
        headers = data_latest[0]

        # Find the Mana column (Change "S" to your actual column letter if different)
        def col_to_idx(col):
            return sum((ord(c) - 64) * 26**i for i, c in enumerate(reversed(col.upper()))) - 1
            
        id_idx = headers.index("lord_id")
        mana_idx = col_to_idx("AA")
        serv_idx = col_to_idx("F")

        # Map previous data for quick lookup
        prev_map = {row[id_idx]: row[mana_idx] for row in data_prev[1:] if len(row) > mana_idx}

        total_mana_gain = 0
        player_count = 0

        for row in data_latest[1:]:
            # 1. Basic length check
            if len(row) <= mana_idx or len(row) <= serv_idx: 
                continue
            
            # 2. STRICT LATEST SERVER FILTER: Only proceed if they are 375 NOW
            server_val = str(row[serv_idx]).strip()
            if server_val != "375":
                continue

            lid = row[id_idx].strip()
            
            # 3. GAIN CALCULATION
            # If they were in the previous sheet, we subtract. 
            # If they are new to the alliance, we count their gain as 0 (to be safe)
            if lid in prev_map:
                try:
                    curr_mana = int(str(row[mana_idx]).replace(",", "").strip() or 0)
                    old_mana = int(str(prev_map[lid]).replace(",", "").strip() or 0)
                    
                    gain = curr_mana - old_mana
                    if gain > 0:
                        total_mana_gain += gain
                        player_count += 1
                except ValueError:
                    continue

        # Calculate Dollar Value ($100 per 250M)
        total_value = round((total_mana_gain / 250_000_000) * 100)

        # Build the Embed
        embed = discord.Embed(
            title=f"🏰 Alliance Mana Report: {season.upper()}",
            description=f"Gain From **{previous.title}** to **{latest.title}**",
            color=discord.Color.blue()
        )
        
        embed.add_field(name="💧 Total Mana Gathered", value=f"**{total_mana_gain:,}**", inline=False)
        embed.add_field(
            name="💰 Value", 
            value=f"The alliance gathered mana worth **{total_value:,}$**", 
            inline=False
        )
        
        await ctx.send(embed=embed)

    except Exception as e:
        await ctx.send(f"❌ Error calculating alliance mana: {e}")

@bot.command(aliases=['checkfarm', 'farm'])
async def farmcheck(ctx, farm_id: str):
    async with ctx.typing():
        
        # 1. Channel Restriction Check
        if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
            channels_mentions = ", ".join([f"<#{channel_id}>" for channel_id in ALLOWED_COMMAND_CHANNEL_ID])
            await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
            return

        try:
            # 2. Get the NVR Farms Sheet Name
            sheet_name = SEASON_SHEETS.get("farms")
            if not sheet_name:
                await ctx.send("❌ Could not find the 'farms' key configured in `SEASON_SHEETS`.")
                return

            # 3. Fetch Data Asynchronously
            tabs = await asyncio.to_thread(client.open(sheet_name).worksheets)
            if not tabs:
                await ctx.send("❌ No worksheets found in the NVR Farms sheet.")
                return

            latest = tabs[-1]
            data = await asyncio.to_thread(latest.get_all_values)
            if not data:
                await ctx.send("❌ The worksheet is empty.")
                return

            headers = [str(h).strip().lower() for h in data[0]]

            # 4. Find Column Indexes ("ID" in Col A, "Whos Farm" in Col B)
            try:
                id_idx = headers.index("id")
            except ValueError:
                id_idx = 0  # Fallback to Column A

            try:
                owner_idx = headers.index("whos farm")
            except ValueError:
                owner_idx = 1  # Fallback to Column B

            # 5. Search for the Farm ID in Column A
            search_id = farm_id.strip()
            found_owner = None

            for row in data[1:]:
                if len(row) > id_idx and row[id_idx].strip() == search_id:
                    found_owner = row[owner_idx].strip() if len(row) > owner_idx else "Unknown Owner"
                    break

            # 6. Build & Send Embed Response
            if found_owner is not None:
                embed = discord.Embed(
                    title="✅ Farm Account Verified",
                    description="This farm ID is registered and verified in our database.",
                    color=discord.Color.green()
                )
                embed.add_field(name="🆔 Farm ID", value=f"`{search_id}`", inline=True)
                embed.add_field(name="👤 Belongs To", value=f"**{found_owner or 'Unspecified'}**", inline=True)
            else:
                embed = discord.Embed(
                    title="❌ Farm Account Not Verified",
                    description=f"No verified farm account found for ID `{search_id}`.",
                    color=discord.Color.red()
                )
                embed.add_field(name="🆔 Searched ID", value=f"`{search_id}`", inline=True)

            # Fixed: Footer no longer references 'season'
            embed.set_footer(text=f"📋 Sheet: {sheet_name} | Tab: {latest.title}")
            await ctx.send(embed=embed)

        except Exception as e:
            await ctx.send(f"❌ Error: {e}")

# =============================================================================
# REBUILT !progress
# =============================================================================
# Replaces your existing progress command entirely. Delete the old one first.
#
# What changed:
#   - Merits-sheet stats now work for ANY server you have an export for, not
#     just 375. Looks up the player's home server and pulls that dataset.
#   - Accepts a window: !progress 123456 7d
#   - Season-to-date by default.
#   - Cleaner embed layout.
# =============================================================================


# -----------------------------------------------------------------------------
# Helper — merits-sheet profile for one player, with ranks
# -----------------------------------------------------------------------------

async def get_merits_profile(server, lord_id, window=None, min_power=50_000_000):
    """
    Pull one player's merits-sheet values plus their rank for each stat within
    their own server.

    Ranks are computed among players at/above min_power, but the requested
    player is always included even if they're below it.

    Returns (profile_dict, label) or (None, reason).
    """
    data, label = await get_merits_window(server, window)
    if data is None:
        return None, label

    headers, rows = lb.as_dicts(data)

    col = lambda *names: lb.find_col(headers, *names)
    c_id    = col("Character ID")
    c_power = col("Historical Highest Power")
    if not c_id:
        return None, "That export is missing a Character ID column."

    target = str(lord_id).strip()
    player = next((r for r in rows if str(r.get(c_id, "")).strip() == target), None)
    if player is None:
        return None, f"Not found in the {lb.server_label(server)} merits export."

    # Ranking pool: everyone big enough, plus the player themselves
    pool = [r for r in rows if lb.to_int(r.get(c_power, 0)) >= min_power]
    if player not in pool:
        pool.append(player)

    def value_of(row, spec):
        """spec: a column name, list of names (summed), or callable(get)."""
        get = lambda c: lb.to_int(row.get(col(c), 0))
        if callable(spec):
            return spec(get)
        if isinstance(spec, str):
            return get(spec)
        return sum(get(c) for c in spec)

    def stat(spec):
        """Returns (value, rank, pool_size)."""
        mine = value_of(player, spec)
        ranked = sorted(pool, key=lambda r: value_of(r, spec), reverse=True)
        rank = next(
            (i for i, r in enumerate(ranked, 1)
             if str(r.get(c_id, "")).strip() == target),
            None,
        )
        return mine, rank, len(pool)

    traded = lambda get: max(0, get("Total Merits") - get("Enemy Merits"))

    profile = {
        "label":     label,
        "pool":      len(pool),
        "infantry":  stat("Infantry Only"),
        "cavalry":   stat("Cavalry Only"),
        "archer":    stat("Marksman Only"),
        "magic":     stat("Magic Only"),
        "other":     stat("Other Merits"),
        "gathering": stat("Gathering"),
        "total":     stat("Total Merits"),
        "enemy":     stat("Enemy Merits"),
        "traded":    stat(traded),
        "t4_heal":   stat("T4 Healed"),
        "t5_heal":   stat("T5 Healed"),
        "build":     stat("Build Time"),
        "destroy":   stat("Destruction Time"),
        "t4_dead":   stat("T4 Deaths"),
        "t5_dead":   stat("T5 Deaths"),
    }
    return profile, label


def _stat_line(emoji, name, stat):
    """Render one '(value) (#rank)' line."""
    value, rank, _ = stat
    rank_str = f" `#{rank}`" if rank else ""
    return f"{emoji} **{name}:** {value:,}{rank_str}"


# -----------------------------------------------------------------------------
# The command
# -----------------------------------------------------------------------------

@bot.command(aliases=['stats'])
async def progress(ctx, lord_id: str, *args):
    """
    Full progress report.

        !progress 11659353              season to date
        !progress 11659353 7d           last 7 days
        !progress 11659353 sos4         a different season
        !progress 11659353 sos4 7d
        !progress 11659353 2026-08-30   since a specific date
    """
    if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
        mentions = ", ".join(f"<#{c}>" for c in ALLOWED_COMMAND_CHANNEL_ID)
        await ctx.send(f"❌ Commands are only allowed in {mentions}.")
        return

    async with ctx.typing():
        try:
            season, window, unknown = split_args(args, SEASON_SHEETS)
            if unknown:
                await ctx.send(
                    f"❌ Didn't understand `{unknown[0]}`.\n"
                    f"Seasons: {', '.join(SEASON_SHEETS)} · "
                    f"Windows: `7d`, `2w`, `season`, `2026-08-30`"
                )
                return

            win = await get_window_data(season, window)
            if win is None:
                await ctx.send(f"❌ Not enough scan history. Try `!scans {season}`.")
                return

            data_latest = win["latest"]
            data_prev   = win["prev"]
            headers     = data_latest[0]

            def idx(*names):
                col = lb.find_col(headers, *names)
                return headers.index(col) if col else None

            id_idx     = idx("lord_id")
            name_idx   = idx("name")
            alliance_idx = idx("alliance_tag", "alliance")
            server_idx = idx("home_server")
            power_idx  = idx("highest_power")
            merit_idx  = idx("merits")
            kills_idx  = idx("units_killed")
            dead_idx   = idx("units_dead")
            healed_idx = idx("units_healed")
            mana_g_idx = idx("mana")

            target = str(lord_id).strip()
            row_latest = next(
                (r for r in data_latest[1:]
                 if id_idx is not None and id_idx < len(r)
                 and str(r[id_idx]).strip() == target),
                None,
            )
            if row_latest is None:
                await ctx.send(
                    "❌ That Lord ID isn't in the latest scan. "
                    "If they migrated in recently they may not appear yet."
                )
                return

            prev_map = {
                str(r[id_idx]).strip(): r for r in data_prev[1:]
                if id_idx < len(r) and str(r[id_idx]).strip()
            }
            row_prev = prev_map.get(target)
            if row_prev is None:
                await ctx.send(
                    "❌ That Lord ID isn't in the earlier scan for this window, "
                    "so gains can't be calculated."
                )
                return

            def val(row, i):
                return lb.to_int(row[i]) if i is not None and i < len(row) else 0

            def gain(i):
                return max(0, val(row_latest, i) - val(row_prev, i))

            name         = str(row_latest[name_idx]).strip() if name_idx is not None else "?"
            alliance     = str(row_latest[alliance_idx]).strip() if alliance_idx is not None else ""
            player_server = "".join(
                ch for ch in str(row_latest[server_idx]) if ch.isdigit()
            ) if server_idx is not None else ""

            power_now  = val(row_latest, power_idx)
            power_gain = gain(power_idx)
            merits_now = val(row_latest, merit_idx)
            merits_gain = gain(merit_idx)
            kills_gain = gain(kills_idx)
            dead_gain  = gain(dead_idx)
            heal_gain  = gain(healed_idx)
            mana_gain  = gain(mana_g_idx)
            merit_ratio = (merits_now / power_now * 100) if power_now else 0

            # --- Ranks within the player's own server -------------------------

            def rank_gain(col_idx):
                if col_idx is None:
                    return None
                vals = []
                for r in data_latest[1:]:
                    if server_idx is None or server_idx >= len(r):
                        continue
                    rs = "".join(ch for ch in str(r[server_idx]) if ch.isdigit())
                    if rs != player_server:
                        continue
                    rid = str(r[id_idx]).strip()
                    base = prev_map.get(rid)
                    if not base:
                        continue
                    vals.append((rid, max(0, val(r, col_idx) - val(base, col_idx))))
                vals.sort(key=lambda x: x[1], reverse=True)
                return next((i for i, (rid, _) in enumerate(vals, 1) if rid == target), None)

            def rank_total(col_idx):
                if col_idx is None:
                    return None
                vals = []
                for r in data_latest[1:]:
                    if server_idx is None or server_idx >= len(r):
                        continue
                    rs = "".join(ch for ch in str(r[server_idx]) if ch.isdigit())
                    if rs != player_server:
                        continue
                    vals.append((str(r[id_idx]).strip(), val(r, col_idx)))
                vals.sort(key=lambda x: x[1], reverse=True)
                return next((i for i, (rid, _) in enumerate(vals, 1) if rid == target), None)

            r_power  = rank_total(power_idx)
            r_merits = rank_total(merit_idx)
            r_kills  = rank_gain(kills_idx)
            r_dead   = rank_gain(dead_idx)
            r_heal   = rank_gain(healed_idx)
            r_mana   = rank_gain(mana_g_idx)

            def rk(r):
                return f" `#{r}`" if r else ""

            # --- Build the embed ---------------------------------------------

            display = f"[{alliance}] {name}" if alliance else name
            embed = discord.Embed(
                title=f"📈 {display}",
                description=(
                    f"**{lb.server_label(player_server)}** · "
                    f"`{season.upper()}` · {win['label']}"
                ),
                color=lb.server_color(player_server),
            )

            embed.add_field(
                name="🟩 Highest Power",
                value=f"{power_now:,}" + (f" (+{power_gain:,})" if power_gain else "") + rk(r_power),
                inline=False,
            )
            embed.add_field(name="🧠 Total Merits", value=f"{merits_now:,}{rk(r_merits)}", inline=True)
            embed.add_field(name="📊 Merit Ratio", value=f"{merit_ratio:.2f}%", inline=True)
            embed.add_field(name="💧 Mana", value=f"+{mana_gain:,}{rk(r_mana)}", inline=True)
            embed.add_field(name="⚔️ Kills", value=f"+{kills_gain:,}{rk(r_kills)}", inline=True)
            embed.add_field(name="💀 Deads", value=f"+{dead_gain:,}{rk(r_dead)}", inline=True)
            embed.add_field(name="❤️ Healed", value=f"+{heal_gain:,}{rk(r_heal)}", inline=True)

            embed.add_field(
                name="\u200b",
                value=f"**🧾 Merits Breakdown — {lb.server_label(player_server)}**",
                inline=False,
            )

            # --- Merits sheet, for whichever server they're on ----------------

            profile, merits_label = await get_merits_profile(
                player_server, target, window=window
            )

            if profile is None:
                embed.add_field(
                    name="\u200b",
                    value=(
                        f"*{merits_label}*\n"
                        f"Upload one with `!ingestmerits {player_server}` to see "
                        f"troop-type merits, healing and build stats here."
                    ),
                    inline=False,
                )
            else:
                embed.add_field(
                    name="Troop Merits",
                    value="\n".join([
                        _stat_line("⚔️", "Infantry", profile["infantry"]),
                        _stat_line("🐎", "Cavalry",  profile["cavalry"]),
                        _stat_line("🏹", "Archer",   profile["archer"]),
                        _stat_line("🪄", "Magic",    profile["magic"]),
                    ]),
                    inline=True,
                )
                embed.add_field(
                    name="Utility",
                    value="\n".join([
                        _stat_line("❤️", "T5 Healed", profile["t5_heal"]),
                        _stat_line("❤️", "T4 Healed", profile["t4_heal"]),
                        _stat_line("🔨", "Build",     profile["build"]),
                        _stat_line("🧨", "Destroy",   profile["destroy"]),
                    ]),
                    inline=True,
                )

                total_v = profile["total"][0]
                enemy_v = profile["enemy"][0]
                traded_v = profile["traded"][0]
                pct = (enemy_v / total_v * 100) if total_v else 0

                embed.add_field(
                    name="Combat Breakdown",
                    value=(
                        f"{_stat_line('🎯', 'Enemy (Real)', profile['enemy'])}\n"
                        f"{_stat_line('🤝', 'Traded', profile['traded'])}\n"
                        f"📈 **Real merit share:** {pct:.1f}%\n"
                    ),
                    inline=False,
                )
                embed.set_footer(
                    text=(
                        f"📅 Scans: {win['prev_title']} → {win['latest_title']}\n"
                        f"🧾 Merits: {merits_label} · ranked among {profile['pool']} "
                        f"players ≥50M power\n"
                        f"🔍 Try: !progress {target} 7d"
                    )
                )

            if profile is None:
                embed.set_footer(
                    text=(
                        f"📅 Scans: {win['prev_title']} → {win['latest_title']}\n"
                        f"🔍 Try: !progress {target} 7d"
                    )
                )

            embed.timestamp = datetime.now(UTC)
            await ctx.send(embed=embed)

        except Exception as e:
            await ctx.send(f"❌ **Error:** {e}")

from discord.ext import commands
import discord

@bot.command()
async def matchups2(ctx, season: str = "test"):
    allowed_channels = {1515777892016193656}
    if ctx.channel.id not in allowed_channels:
        await ctx.send("❌ Command not allowed here.")
        return

    try:
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season, season)

        tabs = await asyncio.to_thread(client.open(sheet_name).worksheets)
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = tabs[-1]
        previous = tabs[-2]
        data_latest = latest.get_all_values()
        data_prev   = previous.get_all_values()
        headers = data_latest[0]

        # header lookups with safe fallback to known positions (0-based)
        def find_idx(name, fallback):
            return headers.index(name) if name in headers else fallback

        def to_int(val):
            try:
                v = str(val).replace(',', '').replace(' ', '').strip()
                if v in ("", "-"): return 0
                return int(v)
            except:
                return 0

        def fmt_gain(n): return f"+{n:,}" if n > 0 else f"{n:,}"
        def format_title_with_dates(prev_name, latest_name):
            return f"📊 War Matchups ({prev_name} → {latest_name})"

        def emoji_bracket(server):
            return {
                "375": "🔴 ", "99": "🔴 ", "249": "🔴 ",
                "40": "🔵 ", "92": "🔵 ", "49": "🔵 "
            }.get(server, "")

        SERVER_MAP = {
            "375": "NVR", "99": "BTX", "92": "wAo", "249": "WB",
            "40": "TFS", "49": "NTS"
        }
        matchups = [("375", "40"), ("99", "92"), ("249", "49")]

        # indices
        id_idx     = find_idx("lord_id",        0)
        server_idx = find_idx("home_server",    5)
        dead_idx   = find_idx("units_dead",     17)
        heal_idx   = find_idx("units_healed",   18)
        gold_idx   = find_idx("gold_spent",     31)
        wood_idx   = find_idx("wood_spent",     32)
        ore_idx    = find_idx("stone_spent",    33)
        mana_idx   = find_idx("mana_spent",     34)
        merits_idx = find_idx("merits (only 50m+ power)", 11)  # fallback near K/L if header missing

        # tiers (AK..AO → 36..40 fallback)
        t5_idx = find_idx("t5_kills", 36)
        t4_idx = find_idx("t4_kills", 37)
        t3_idx = find_idx("t3_kills", 38)
        t2_idx = find_idx("t2_kills", 39)
        t1_idx = find_idx("t1_kills", 40)

        max_needed_idx = max(mana_idx, t1_idx, merits_idx)

        # prev rows by lord_id (keep last occurrence)
        prev_map = {
            row[id_idx]: row for row in data_prev[1:]
            if len(row) > max_needed_idx and row[id_idx]
        }

        # aggregate
        stat_map = {s: {
            "kills": 0, "kills_gain": 0,
            "dead": 0,  "dead_gain": 0,
            "healed": 0,"healed_gain": 0,
            "gold": 0, "wood": 0, "ore": 0, "mana": 0,
            "merits": 0, "merits_gain": 0,
            "t5": 0, "t5_gain": 0,
            "t4": 0, "t4_gain": 0,
            "t3": 0, "t3_gain": 0,
            "t2": 0, "t2_gain": 0,
            "t1": 0, "t1_gain": 0,
        } for s in SERVER_MAP}

        for row in data_latest[1:]:
            if len(row) <= max_needed_idx:
                continue

            # MUST exist in both sheets
            lid = (row[id_idx] or "").strip()
            prev_row = prev_map.get(lid)
            if not lid or prev_row is None:
                continue

            # server (use latest, normalized to digits)
            sid_raw = (row[server_idx] or "").strip()
            sid = "".join(ch for ch in sid_raw if ch.isdigit())
            if sid not in SERVER_MAP:
                continue

            # current
            dead = to_int(row[dead_idx]);   heal = to_int(row[heal_idx])
            gold = to_int(row[gold_idx]);   wood = to_int(row[wood_idx])
            ore  = to_int(row[ore_idx]);    mana = to_int(row[mana_idx])
            merits = to_int(row[merits_idx])
            t5 = to_int(row[t5_idx]); t4 = to_int(row[t4_idx]); t3 = to_int(row[t3_idx])
            t2 = to_int(row[t2_idx]); t1 = to_int(row[t1_idx])

            # previous
            dead_prev = to_int(prev_row[dead_idx]);   heal_prev = to_int(prev_row[heal_idx])
            gold_prev = to_int(prev_row[gold_idx]);   wood_prev = to_int(prev_row[wood_idx])
            ore_prev  = to_int(prev_row[ore_idx]);    mana_prev = to_int(prev_row[mana_idx])
            merits_prev = to_int(prev_row[merits_idx])
            t5_prev = to_int(prev_row[t5_idx]); t4_prev = to_int(prev_row[t4_idx]); t3_prev = to_int(prev_row[t3_idx])
            t2_prev = to_int(prev_row[t2_idx]); t1_prev = to_int(prev_row[t1_idx])

            s = stat_map[sid]
            # totals (restricted to IDs present in both)
            s["dead"]   += dead
            s["healed"] += heal
            s["merits"] += merits
            s["t5"]     += t5; s["t4"] += t4; s["t3"] += t3; s["t2"] += t2; s["t1"] += t1
            # deltas
            s["dead_gain"]    += (dead   - dead_prev)
            s["healed_gain"]  += (heal   - heal_prev)
            s["gold"]         += (gold   - gold_prev)
            s["wood"]         += (wood   - wood_prev)
            s["ore"]          += (ore    - ore_prev)
            s["mana"]         += (mana   - mana_prev)
            s["merits_gain"]  += (merits - merits_prev)
            s["t5_gain"]      += (t5 - t5_prev)
            s["t4_gain"]      += (t4 - t4_prev)
            s["t3_gain"]      += (t3 - t3_prev)
            s["t2_gain"]      += (t2 - t2_prev)
            s["t1_gain"]      += (t1 - t1_prev)

        # derive kills from tiers so totals match breakdown
        for sid, s in stat_map.items():
            tier_total = s["t5"] + s["t4"] + s["t3"] + s["t2"] + s["t1"]
            tier_gain  = s["t5_gain"] + s["t4_gain"] + s["t3_gain"] + s["t2_gain"] + s["t1_gain"]
            s["kills"] = tier_total
            s["kills_gain"] = tier_gain

        def format_side(name, stats):
            return (
                f"{name}\n"
                f"\n"
                f"▶ Combat Stats\n"
                f"⚔️ Kills:   {stats['kills']:,} ({fmt_gain(stats['kills_gain'])})\n"
                f"💀 Deads:   {stats['dead']:,} ({fmt_gain(stats['dead_gain'])})\n"
                f"❤️ Heals:   {stats['healed']:,} ({fmt_gain(stats['healed_gain'])})\n"
                f"🏅 Merits:  {stats['merits']:,} ({fmt_gain(stats['merits_gain'])})\n"
                f"\n"
                f"▶ Kill Breakdown\n"
                f"🟥 T5: {stats['t5']:,} ({fmt_gain(stats['t5_gain'])})\n"
                f"🟦 T4: {stats['t4']:,} ({fmt_gain(stats['t4_gain'])})\n"
                f"🟩 T3: {stats['t3']:,} ({fmt_gain(stats['t3_gain'])})\n"
                f"🟨 T2: {stats['t2']:,} ({fmt_gain(stats['t2_gain'])})\n"
                f"⬜ T1: {stats['t1']:,} ({fmt_gain(stats['t1_gain'])})\n"
                f"\n"
                f"▶ Resources Spent (Δ)\n"
                f"💰 Gold:  {stats['gold']:,}\n"
                f"🪵 Wood:  {stats['wood']:,}\n"
                f"⛏️ Ore:   {stats['ore']:,}\n"
                f"💧 Mana:  {stats['mana']:,}\n"
            )

        title = format_title_with_dates(previous.title, latest.title)

        for a, b in matchups:
            name_a = f"{emoji_bracket(a)}{SERVER_MAP[a]}"
            name_b = f"{emoji_bracket(b)}{SERVER_MAP[b]}"
            stats_a = stat_map[a]
            stats_b = stat_map[b]

            block = (
                f"{name_a} vs {name_b}\n\n"
                f"{format_side(name_a, stats_a)}"
                f"\n━━━━━━━━━━━━━━\n\n"
                f"{format_side(name_b, stats_b)}"
            )

            embed = discord.Embed(
                title=f"{title} — {SERVER_MAP[a]} vs {SERVER_MAP[b]}",
                description=f"```{block}```",
                color=0x00ffcc
            )
            await ctx.send(embed=embed)

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

# =============================================================================
# REBUILT !matchups  (v3 — power tracking, RSS healing costs, spaced layout)
# =============================================================================
# Replaces the previous matchups command.
#
#     !matchups              all pairings, season to date
#     !matchups 357          just the pairing involving 357
#     !matchups 1d           yesterday — power loss, healing spend, the lot
#     !matchups 357 7d
#
# Everything is filtered to accounts at/above 50M HIGHEST power, on both the
# scan side and the merits side, so the two datasets describe the same players.
# =============================================================================


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

MIN_POWER = 50_000_000

# Mana cost of healing, at max healing-cost reduction.
# 1M T5 troops ≈ 66M mana · 1M T4 troops ≈ 16M mana
MANA_PER_T5 = 66.0     # mana per single T5 unit healed
MANA_PER_T4 = 16.0     # mana per single T4 unit healed


def healing_mana_cost(t5, t4):
    """Estimated mana spent healing, from T5/T4 unit counts."""
    return int(t5 * MANA_PER_T5 + t4 * MANA_PER_T4)


# -----------------------------------------------------------------------------
# Scan aggregation
# -----------------------------------------------------------------------------

SCAN_STATS = [
    ("kills",  "⚔️", "Kills",  "units_killed"),
    ("dead",   "💀", "Deads",  "units_dead"),
    ("healed", "❤️", "Units Healed", "units_healed"),
    ("merits", "🧠", "Merits", "merits"),
]


def aggregate_scan(latest, prev, servers, min_power=MIN_POWER):
    """
    Sum gains per side, and track power separately since power is an absolute
    value rather than an accumulating counter.

    Only counts players present in BOTH scans, on one of the given servers,
    at/above min_power highest power.
    """
    headers = [str(h).strip() for h in latest[0]]
    lower = [h.lower() for h in headers]

    def i(*names):
        for n in names:
            if n.lower() in lower:
                return lower.index(n.lower())
        return None

    i_id      = i("lord_id")
    i_server  = i("home_server")
    i_power   = i("power")             # current power
    i_hpower  = i("highest_power")     # historical highest
    cols = {k: i(name) for k, _, _, name in SCAN_STATS}

    def val(row, idx):
        return lb.to_int(row[idx]) if idx is not None and idx < len(row) else 0

    prev_map = {
        str(r[i_id]).strip(): r for r in prev[1:]
        if i_id is not None and i_id < len(r) and str(r[i_id]).strip()
    }

    wanted = {str(s) for s in servers}
    totals = {k: 0 for k, *_ in SCAN_STATS}
    totals.update({
        "power_now": 0, "power_then": 0, "power_change": 0,
        "highest_power": 0, "players": 0, "losers": 0,
    })

    for row in latest[1:]:
        if i_id is None or i_id >= len(row):
            continue
        sid = "".join(ch for ch in str(row[i_server]) if ch.isdigit()) if i_server is not None else ""
        if sid not in wanted:
            continue

        if val(row, i_hpower) < min_power:
            continue

        base = prev_map.get(str(row[i_id]).strip())
        if base is None:
            continue

        totals["players"] += 1

        p_now  = val(row, i_power)
        p_then = val(base, i_power)
        totals["power_now"]  += p_now
        totals["power_then"] += p_then
        totals["highest_power"] += val(row, i_hpower)
        if p_now < p_then:
            totals["losers"] += 1

        for key, *_ in SCAN_STATS:
            c = cols[key]
            if c is not None:
                totals[key] += max(0, val(row, c) - val(base, c))

    totals["power_change"] = totals["power_now"] - totals["power_then"]
    return totals


# -----------------------------------------------------------------------------
# Merits-sheet aggregation
# -----------------------------------------------------------------------------

MERIT_STATS = [
    ("infantry", "⚔️", "Infantry",  "Infantry Only"),
    ("cavalry",  "🐎", "Cavalry",   "Cavalry Only"),
    ("archer",   "🏹", "Archer",    "Marksman Only"),
    ("magic",    "🪄", "Magic",     "Magic Only"),
]


async def aggregate_merits(servers, window=None, min_power=MIN_POWER):
    """Sum merits-sheet columns across servers. Returns (totals, label) or (None, reason)."""
    combined = {key: 0 for key, *_ in MERIT_STATS}
    combined.update({
        "total": 0, "enemy": 0, "traded": 0,
        "t4_healed": 0, "t5_healed": 0, "heal_mana": 0,
        "build": 0, "destroy": 0,
        "t5_dead": 0, "t4_dead": 0, "players": 0,
    })
    label = None
    found = False

    for server in servers:
        data, lbl = await get_merits_window(server, window)
        if data is None:
            continue
        found = True
        label = label or lbl

        headers, rows = lb.as_dicts(data)
        col = lambda *n: lb.find_col(headers, *n)
        c_power = col("Historical Highest Power")
        mapping = {key: col(name) for key, _, _, name in MERIT_STATS}
        c_total, c_enemy = col("Total Merits"), col("Enemy Merits")
        c_t4h, c_t5h     = col("T4 Healed"), col("T5 Healed")
        c_build, c_dest  = col("Build Time"), col("Destruction Time")
        c_t5d, c_t4d     = col("T5 Deaths"), col("T4 Deaths")

        for r in rows:
            if lb.to_int(r.get(c_power, 0)) < min_power:
                continue
            combined["players"] += 1
            for key, c in mapping.items():
                if c:
                    combined[key] += lb.to_int(r.get(c, 0))

            total = lb.to_int(r.get(c_total, 0))
            enemy = lb.to_int(r.get(c_enemy, 0))
            t4h   = lb.to_int(r.get(c_t4h, 0))
            t5h   = lb.to_int(r.get(c_t5h, 0))

            combined["total"]     += total
            combined["enemy"]     += enemy
            combined["traded"]    += max(0, total - enemy)
            combined["t4_healed"] += t4h
            combined["t5_healed"] += t5h
            combined["build"]     += lb.to_int(r.get(c_build, 0))
            combined["destroy"]   += lb.to_int(r.get(c_dest, 0))
            combined["t5_dead"]   += lb.to_int(r.get(c_t5d, 0))
            combined["t4_dead"]   += lb.to_int(r.get(c_t4d, 0))

    if not found:
        return None, "no merits export stored"

    combined["heal_mana"] = healing_mana_cost(combined["t5_healed"], combined["t4_healed"])
    return combined, label


# -----------------------------------------------------------------------------
# Rendering
# -----------------------------------------------------------------------------

BAR_WIDTH = 10
DIVIDER = "\u2500" * 20


def section(rows, divider=True):
    """
    Join rendered rows into a field value: blank line between each, and a rule
    at the end so sections don't run into one another.
    """
    body = "\n\n".join(r for r in rows if r)
    return body + (f"\n\u200b\n{DIVIDER}" if divider else "")


def vs_bar(a, b, left="🟥", right="🟦"):
    total = abs(a) + abs(b)
    if total <= 0:
        return "⬛" * BAR_WIDTH
    la = max(0, min(BAR_WIDTH, round(abs(a) / total * BAR_WIDTH)))
    return left * la + right * (BAR_WIDTH - la)


def vs_row(emoji, label, a, b, suffix=""):
    marker = "◀" if a > b else ("▶" if b > a else "=")
    return (
        f"{emoji} **{label}** {marker}\n"
        f"`{lb.fmt(a):>8}` {vs_bar(a, b)} `{lb.fmt(b):<8}`{suffix}"
    )


def vs_row_power(emoji, label, a, b):
    """
    For power change, where negative is bad. The bar shows relative magnitude
    of LOSS, and the marker points at whoever came off worse.
    """
    def sign(n):
        return f"+{lb.fmt(n)}" if n > 0 else lb.fmt(n)

    if a < b:
        note = "🔻 left side lost more"
    elif b < a:
        note = "🔻 right side lost more"
    else:
        note = "even"

    return (
        f"{emoji} **{label}**\n"
        f"`{sign(a):>9}` {vs_bar(a, b)} `{sign(b):<9}`\n"
        f"*{note}*"
    )


def side_name(servers):
    return " & ".join(lb.SERVER_NAMES.get(s, s) for s in servers)


def side_detail(servers):
    return " & ".join(f"{lb.SERVER_NAMES.get(s, s)} ({s})" for s in servers)


# -----------------------------------------------------------------------------
# The command
# -----------------------------------------------------------------------------

@bot.command()
async def matchups(ctx, *args):
    """War comparison between paired servers."""
    if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
        mentions = ", ".join(f"<#{c}>" for c in ALLOWED_COMMAND_CHANNEL_ID)
        await ctx.send(f"❌ Commands are only allowed in {mentions}.")
        return

    async with ctx.typing():
        try:
            opts, unknown = lb.parse_args(
                args, SEASON_SHEETS, DEFAULT_SEASON, default_server=None
            )
            if unknown:
                await ctx.send(f"❌ Didn't understand `{unknown[0]}`.")
                return

            season, window = opts["season"], opts["window"]
            cfg = season_cfg(season)

            pairings = cfg["matchups"]
            if not pairings:
                await ctx.send(
                    f"❌ No matchups configured for `{season}`. Add them to `SEASON_CONFIG`."
                )
                return

            if opts["server"]:
                target = str(opts["server"])
                pairings = [p for p in pairings if target in p[0] + p[1]]
                if not pairings:
                    await ctx.send(
                        f"❌ {lb.server_label(target)} isn't in any pairing this season."
                    )
                    return

            win = await get_window_data(season, window)
            if win is None:
                await ctx.send(f"❌ Not enough scan history. Try `!scans {season}`.")
                return

            for team_a, team_b in pairings:
                a = aggregate_scan(win["latest"], win["prev"], team_a)
                b = aggregate_scan(win["latest"], win["prev"], team_b)

                ma, label_a = await aggregate_merits(team_a, window)
                mb, label_b = await aggregate_merits(team_b, window)

                embed = discord.Embed(
                    title=f"⚔️ {side_name(team_a)}  vs  {side_name(team_b)}",
                    description=(
                        f"🟥 {side_detail(team_a)}  ·  🟦 {side_detail(team_b)}\n"
                        f"**{win['label']}** · `{cfg['label']}` · *≥50M power only*"
                    ),
                    color=lb.server_color(team_a[0]),
                )

                # --- Power ------------------------------------------------
                embed.add_field(
                    name="⚡  P O W E R",
                    value=section([
                        vs_row_power("📉", "Power Change",
                                     a["power_change"], b["power_change"]),
                        f"*{a['losers']} of {a['players']} lost power · "
                        f"{b['losers']} of {b['players']} lost power*",
                        vs_row("🟩", "Current Power", a["power_now"], b["power_now"]),
                        vs_row("🏔️", "Highest Power",
                               a["highest_power"], b["highest_power"]),
                    ]),
                    inline=False,
                )

                # --- Combat -----------------------------------------------
                embed.add_field(
                    name=f"📊  C O M B A T   —   {a['players']:,} vs {b['players']:,}",
                    value=section([
                        vs_row(emoji, label, a[key], b[key])
                        for key, emoji, label, _ in SCAN_STATS
                    ], divider=bool(ma or mb)),
                    inline=False,
                )

                # --- RSS healing ------------------------------------------
                if ma and mb:
                    embed.add_field(
                        name="🧪  R S S   H E A L I N G",
                        value=section([
                            vs_row("💧", "Est. Mana Cost",
                                   ma["heal_mana"], mb["heal_mana"]),
                            vs_row("🟥", "T5 Healed",
                                   ma["t5_healed"], mb["t5_healed"]),
                            vs_row("🟦", "T4 Healed",
                                   ma["t4_healed"], mb["t4_healed"]),
                            f"*Estimated at {MANA_PER_T5:.0f} mana per T5 · "
                            f"{MANA_PER_T4:.0f} per T4*",
                        ]),
                        inline=False,
                    )

                    embed.add_field(
                        name="🛡️ T R O O P   M E R I T S",
                        value=section([
                            vs_row(emoji, label, ma[key], mb[key])
                            for key, emoji, label, _ in MERIT_STATS
                        ]),
                        inline=False,
                    )

                    pct_a = (ma["enemy"] / ma["total"] * 100) if ma["total"] else 0
                    pct_b = (mb["enemy"] / mb["total"] * 100) if mb["total"] else 0
                    embed.add_field(
                        name="🔍  M E R I T   Q U A L I T Y",
                        value=section([
                            vs_row("🎯", "Enemy (Real) Merits",
                                   ma["enemy"], mb["enemy"]),
                            f"*Real share: {pct_a:.0f}% vs {pct_b:.0f}%*",
                            vs_row("🤝", "Traded Merits",
                                   ma["traded"], mb["traded"]),
                        ], divider=False),
                        inline=False,
                    )

                elif ma or mb:
                    got = ma or mb
                    have = side_name(team_a) if ma else side_name(team_b)
                    missing = side_name(team_b) if ma else side_name(team_a)
                    embed.add_field(
                        name=f"🧪  M E R I T S   —   {have} only",
                        value=section([
                            f"💧 **Est. Healing Mana:** {got['heal_mana']:,}\n"
                            f"🟥 **T5 Healed:** {got['t5_healed']:,}\n"
                            f"🟦 **T4 Healed:** {got['t4_healed']:,}",
                            "\n".join(
                                f"{emoji} **{label}:** {got[key]:,}"
                                for key, emoji, label, _ in MERIT_STATS
                            ),
                            f"*No merits export for {missing}. "
                            f"Upload one with `!ingestmerits <server>`.*",
                        ], divider=False),
                        inline=False,
                    )

                footer = f"📅 Scans: {win['prev_title']} → {win['latest_title']}"
                if ma or mb:
                    footer += f"\n🧾 Merits: {label_a or label_b}"
                embed.set_footer(text=footer)
                embed.timestamp = datetime.now(UTC)

                await ctx.send(embed=embed)

                if (team_a, team_b) != pairings[-1]:
                    await ctx.send("\u200b\n" + "\u2501" * 30 + "\n\u200b")

        except Exception as e:
            await ctx.send(f"❌ **Error:** {e}")

import os
TOKEN = os.getenv("TOKEN")

# Replace with your actual war-status channel ID
CHANNEL_ID = 1369071691111600168

# Allowed role ID
ALLOWED_ROLE_ID = 1527800467353112716

# Permission check decorator
def role_check():
    async def predicate(ctx):
        if any(role.id == ALLOWED_ROLE_ID for role in ctx.author.roles):
            return True
        await ctx.send("❌ You don’t have permission to use this command.")
        return False
    return commands.check(predicate)


@bot.event
async def on_ready():
    await db.init_db() 
    await db.upgrade_schema()
    await db.ensure_exclusions_table()
    await refresh_exclusions()
    await bot.load_extension("spydetect")
    await bot.load_extension("dashboard")
    await bot.load_extension("activity_checks")
    print(f"✅ Bot is online as {bot.user}")
    if not fetch_sheets_background.is_running():
        fetch_sheets_background.start()

    # Start the UTC channel updater loop
    if not update_utc_channels.is_running():
        update_utc_channels.start()
    
# =============================================================================
# REBUILT HELP COMMAND
# =============================================================================
# Replaces your existing `commands` help command.
#
# NOTE THE RENAME: the function is now show_commands, not commands. The old
# name shadowed `from discord.ext import commands`, which would break any
# @scan_admin() command defined below it. Users still type !help / !commands.
#
# Also delete the `kills` command — it's gone from the help text.
# =============================================================================


HELP_CATEGORIES = {
    "player": {
        "emoji": "📊",
        "label": "Player Stats",
        "desc": "Look up an individual account",
    },
    "scans": {
        "emoji": "🏆",
        "label": "Scan Leaderboards",
        "desc": "Kills, deads, merits, healing, mana",
    },
    "merits": {
        "emoji": "⚔️",
        "label": "Merit Leaderboards",
        "desc": "Troop types, healing, build, traded merits",
    },
    "war": {
        "emoji": "🆚",
        "label": "War & Groups",
        "desc": "Matchups, Sun vs Moon, duels",
    },
    "args": {
        "emoji": "🎛️",
        "label": "Arguments Guide",
        "desc": "Servers, time windows, counts, seasons",
    },
    "admin": {
        "emoji": "🔧",
        "label": "Admin",
        "desc": "Uploading data and managing exclusions",
    },
}


def _help_embed(key, is_admin=False):
    cat = HELP_CATEGORIES[key]
    embed = discord.Embed(
        title=f"{cat['emoji']}  {cat['label']}",
        color=0x5865F2,
    )

    if key == "player":
        embed.description = (
            "**`!progress <lord_id>`**  ·  alias `!stats`\n"
            "Full profile: power, merits, kills, deads, healing, mana — with "
            "gains and server ranks. Also shows troop-type merits, RSS healing "
            "and build stats if a merits export exists for that player's server.\n\n"
            "```\n"
            "!progress 11659353\n"
            "!progress 11659353 7d\n"
            "!progress 11659353 sos4\n"
            "!progress 11659353 2026-08-30\n"
            "```\n"
            "**`!mana <lord_id>`**\n"
            "Mana gathered this season, with rank and a rough cash value.\n\n"
        )

    elif key == "scans":
        embed.description = (
            "Built from the daily scans. Cover **all six servers**, so you can "
            "look at rivals as easily as your own.\n\n"
            "**Top**\n"
            "`!topmerits` `!topdeads` `!topkills` `!topheal` `!topmana`\n"
            "`!topt5` — T5 kills\n"
            "`!topefficiency` — merits per 1M power\n\n"
            "**Bottom**\n"
            "`!lowdeads` `!lowmerits`\n\n"
            "```\n"
            "!topmerits              top 10, server 375\n"
            "!topmerits 357 50       top 50 on YSS\n"
            "!topdeads 357 25 7d     last 7 days\n"
            "!topmerits all 20       every server\n"
            "```\n"
            "*Short aliases: `!tm` `!td` `!ld` `!lm` `!tk` `!th`*"
        )

    elif key == "merits":
        embed.description = (
            "Built from the merits export. Filtered to **≥50M power**.\n\n"
            "**Troop types**\n"
            "`!topinf` / `!lowinf` — Infantry\n"
            "`!topcav` / `!lowcav` — Cavalry\n"
            "`!toparcher` / `!lowarcher` — Archer\n"
            "`!topmage` / `!lowmage` — Magic\n\n"
            "**Utility**\n"
            "`!toprssheal` / `!lowrssheal` — RSS healing (T4 + T5)\n"
            "`!topbuild` / `!lowbuild` — Build time\n"
            "`!topdest` / `!lowdest` — Destruction time\n\n"
            "**Merit quality**\n"
            "`!topreal` — merits earned against actual enemies\n"
            "`!toptraders` — total minus enemy merits, with traded %\n\n"
            "```\n"
            "!topinf 50\n"
            "!toptraders 357 25\n"
            "!topmage 357 7d\n"
            "```\n"
            "*A server only works here if its export has been uploaded.*"
        )

    elif key == "war":
        embed.description = (
            "**`!matchups`**\n"
            "Head-to-head between paired servers: power change, combat stats, "
            "RSS healing spend, army composition and merit quality.\n\n"
            "```\n"
            "!matchups           all pairings\n"
            "!matchups 357       just that pairing\n"
            "!matchups 1d        yesterday — power loss\n"
            "!matchups 357 7d\n"
            "```\n"
            "**`!groupleaderboard`**  ·  aliases `!gl`, `!grouplb`\n"
            "Sun vs Moon player rankings.\n\n"
            "**`!groupstats`**\n"
            "Sun vs Moon team totals.\n\n"
            "**`!duel`** · **`!duel2`**\n"
            "The running 1v1 challenges.\n\n"
            "**`!allmana`**\n"
            "Alliance-wide mana gathered."
        )

    elif key == "args":
        embed.description = (
            "Most commands take the same arguments, **in any order**.\n\n"
            "**🌐 Server** — defaults to 375\n"
            "```\n"
            "375  357  756  341  320      by number\n"
            "s5                            single-digit servers\n"
            "nvr  yss  sab                 by tag\n"
            "all                           every server\n"
            "```\n"
            "**🔢 Count** — defaults to 10, max 100\n"
            "```\n"
            "!topmerits 50\n"
            "```\n"
            "**📅 Time window** — defaults to the whole season\n"
            "```\n"
            "1d  7d  14d       last N days\n"
            "2w                last N weeks\n"
            "2026-08-30        since a date\n"
            "season            explicit season-to-date\n"
            "```\n"
            "**🗂️ Season** — defaults to the current one\n"
            "```\n"
            "!progress 123456 sos4\n"
            "```\n"
            "⚠️ A bare number is read as a **count**, so use `s5` for OMG.\n"
            "⚠️ Windows only reach back as far as stored scans. If you ask for "
            "more history than exists, it falls back and tells you."
        )

    elif key == "admin":
        if not is_admin:
            embed.description = (
                "🔒 These commands need the scan-admin role.\n\n"
                "Data is uploaded daily by the alliance leadership — the scan "
                "CSV and the merits export. Everything else reads from that."
            )
            return embed

        embed.description = (
            "**📥 Daily uploads** — attach the file to the message\n"
            "```\n"
            "!ingest sos2              the scan CSV\n"
            "!ingestmerits             375 merits export\n"
            "!ingestmerits 357         a rival's export\n"
            "```\n"
            "Dates are read from the filename. Re-uploading the same date "
            "replaces it, so a bad upload is safe to redo.\n\n"
            "**🗂️ Managing stored data**\n"
            "```\n"
            "!scans sos2               list stored scan dates\n"
            "!scans s375               list merits exports\n"
            "!unscan sos2 2026-09-01   delete one day\n"
            "!backfill sos2            import old sheet tabs\n"
            "!resync                   force a cache refresh\n"
            "```\n"
            "**🚫 Exclusions** — dead accounts, removed from every stat\n"
            "```\n"
            "!exclude 12345678 dead since July\n"
            "!unexclude 12345678\n"
            "!excluded\n"
            "```\n"
            "⚠️ Always export merits with the **season start** as the start "
            "date, or windows won't line up."
        )

    return embed


def _help_home(is_admin=False):
    embed = discord.Embed(
        title="📜  NVR Bot",
        description=(
            "Pick a category below, or use `!help <command>` for one command.\n\n"
            "**Quick start**\n"
            "```\n"
            "!progress 11659353        your stats\n"
            "!topmerits 50             top 50 on 375\n"
            "!topmerits 357 50         top 50 on YSS\n"
            "!matchups 1d              yesterday's war\n"
            "```"
        ),
        color=0x5865F2,
    )
    for key, cat in HELP_CATEGORIES.items():
        if key == "admin" and not is_admin:
            continue
        embed.add_field(
            name=f"{cat['emoji']} {cat['label']}",
            value=cat["desc"],
            inline=True,
        )
    embed.set_footer(text="Square brackets aren't typed — !topmerits 50, not !topmerits [50]")
    return embed


class HelpView(discord.ui.View):
    def __init__(self, author_id, is_admin, timeout=300):
        super().__init__(timeout=timeout)
        self.author_id = author_id
        self.is_admin = is_admin
        self.message = None

        options = [
            discord.SelectOption(
                label="Overview", value="home", emoji="📜",
                description="Back to the start",
            )
        ]
        for key, cat in HELP_CATEGORIES.items():
            if key == "admin" and not is_admin:
                continue
            options.append(
                discord.SelectOption(
                    label=cat["label"], value=key,
                    emoji=cat["emoji"], description=cat["desc"][:100],
                )
            )
        self.select.options = options

    async def interaction_check(self, interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "Run `!help` yourself to browse it.", ephemeral=True
            )
            return False
        return True

    async def on_timeout(self):
        self.clear_items()
        if self.message:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass

    @discord.ui.select(placeholder="Choose a category…")
    async def select(self, interaction, select):
        value = select.values[0]
        embed = _help_home(self.is_admin) if value == "home" else _help_embed(value, self.is_admin)
        await interaction.response.edit_message(embed=embed, view=self)


# -----------------------------------------------------------------------------
# Per-command lookup: !help topmerits
# -----------------------------------------------------------------------------

COMMAND_HELP = {
    "progress": ("📊 !progress <lord_id> [window] [season]",
                 "Full profile with gains, ranks and merit breakdown.\n"
                 "`!progress 11659353 7d`"),
    "mana": ("💧 !mana <lord_id> [season]",
             "Mana gathered this season, with rank."),
    "topmerits": ("🧠 !topmerits [server] [count] [window]",
                  "Highest merit gains.\n`!topmerits 357 50 7d`"),
    "topdeads": ("💀 !topdeads [server] [count] [window]",
                 "Most units lost.\n`!topdeads 357 25`"),
    "lowdeads": ("🔻 !lowdeads [server] [count] [window]",
                 "Fewest units lost — who isn't fighting."),
    "lowmerits": ("🔻 !lowmerits [server] [count] [window]",
                  "Lowest merit gains."),
    "topkills": ("⚔️ !topkills [server] [count] [window]", "Most kills."),
    "topheal": ("❤️ !topheal [server] [count] [window]", "Most units healed."),
    "topmana": ("💧 !topmana [server] [count] [window]", "Most mana gathered."),
    "topefficiency": ("📊 !topefficiency [server] [count] [window]",
                      "Merits per 1M power — who punches above their weight."),
    "toptraders": ("🤝 !toptraders [server] [count] [window]",
                   "Total merits minus enemy merits, with traded %."),
    "topreal": ("🎯 !topreal [server] [count] [window]",
                "Merits earned against actual enemies."),
    "topinf": ("⚔️ !topinf [server] [count] [window]", "Infantry merits."),
    "matchups": ("🆚 !matchups [server] [window]",
                 "Head-to-head war comparison.\n`!matchups 1d`"),
    "groupleaderboard": ("🏆 !gl [window]", "Sun vs Moon rankings."),
    "excluded": ("🚫 !excluded", "Accounts excluded from all stats."),
    "scans": ("🗂️ !scans <dataset>", "Stored dates.\n`!scans sos2` · `!scans s375`"),
}


@bot.command(name="commands", aliases=["help", "info", "guide"])
async def show_commands(ctx, query: str = None):
    """Browse the bot's commands."""
    if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
        mentions = ", ".join(f"<#{c}>" for c in ALLOWED_COMMAND_CHANNEL_ID)
        await ctx.send(f"❌ Commands are only allowed in {mentions}.")
        return

    is_admin = any(
        r.id == SCAN_ADMIN_ROLE_ID for r in getattr(ctx.author, "roles", [])
    )

    # !help <command>
    if query:
        key = query.strip().lstrip("!").lower()
        entry = COMMAND_HELP.get(key)
        if entry is None:
            cmd = bot.get_command(key)
            if cmd:
                key = cmd.name
                entry = COMMAND_HELP.get(key)
        if entry:
            title, body = entry
            embed = discord.Embed(title=title, description=body, color=0x5865F2)
            embed.set_footer(text="!help for everything · !help args for argument syntax")
            await ctx.send(embed=embed)
            return
        await ctx.send(
            f"❓ No help entry for `{query}`. Try `!help` to browse."
        )
        return

    view = HelpView(ctx.author.id, is_admin)
    message = await ctx.send(embed=_help_home(is_admin), view=view)
    view.message = message

bot.run(TOKEN)
