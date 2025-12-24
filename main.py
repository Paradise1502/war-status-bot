import gspread
from oauth2client.service_account import ServiceAccountCredentials
from string import ascii_uppercase
import os
import json
import discord
from discord.ext import commands
from discord.ext import tasks
from datetime import datetime, timedelta, UTC
import asyncio
import unicodedata

# Google Sheets Auth
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_json = os.getenv("CREDENTIALS_JSON")
creds_dict = json.loads(creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

EVENT_SHEET_NAME = "Event Schedule"      # The spreadsheet name
EVENT_TAB_NAME = "events"               # The tab name
ANNOUNCE_CHANNEL_ID = 1383515877793595435  # 👈 set your daily-announcement channel

# Season sheet mapping
SEASON_SHEETS = {
    "hk1": "Call of Dragons - HK1",
    "hk2": "Call of Dragons - HK2",
    "hk3": "Call of Dragons - HK3",
    "sos2": "Call of Dragons - SoS2",
    "sos5": "Call of Dragons - SoS5",
    "sos6": "Call of Dragons - SoS6",
    "statue": "Activity",
    "test": "testsheet",
    "sos4": "Call of Dragons - SoS4"# 👈 add this line
}

DEFAULT_SEASON = "sos6"

# Now your bot setup
intents = discord.Intents.default()
intents.guilds = True
intents.messages = True
intents.reactions = True
intents.guild_reactions = True
intents.message_content = True  # Not needed for reactions, but good for commands

bot = commands.Bot(command_prefix="!", intents=intents)

# Global flag
VACATION_MODE = True
VACATION_MSG = "in Downtime"

# Simple check before every command
@bot.check
async def global_vacation_check(ctx):
    if VACATION_MODE:
        await ctx.send(VACATION_MSG)
        return False
    return True

@bot.command()
async def rssheal(ctx, lord_id: str, season: str = DEFAULT_SEASON):
    try:
        # Lowercase just in case someone writes "SoS2"
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season)

        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Available: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = client.open(sheet_name).worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = tabs[-1]
        previous = tabs[-2]

        latest_data = latest.get_all_values()
        previous_data = previous.get_all_values()

        headers = latest_data[0]
        name_index = 1  # Column B
        id_index = headers.index("lord_id")

        # Column indices: AF=31, AG=32, AH=33, AI=34 (zero-indexed)
        gold_idx, wood_idx, ore_idx, mana_idx = 31, 32, 33, 34

        def find_row(data):
            for row in data[1:]:
                if row[id_index] == lord_id:
                    return row
            return None

        row_latest = find_row(latest_data)
        username = row_latest[name_index]
        row_prev = find_row(previous_data)

        if not row_latest or not row_prev:
            await ctx.send("❌ Lord ID not found in both sheets.")
            return

        def to_int(val):
            try: return int(val)
            except: return 0

        gold = to_int(row_latest[gold_idx]) - to_int(row_prev[gold_idx])
        wood = to_int(row_latest[wood_idx]) - to_int(row_prev[wood_idx])
        ore  = to_int(row_latest[ore_idx])  - to_int(row_prev[ore_idx])
        mana = to_int(row_latest[mana_idx]) - to_int(row_prev[mana_idx])

        await ctx.send(
            f"📊 RSS Spent by `{username}` (`{lord_id}`) between `{previous.title}` → `{latest.title}`:\n"
            f"🪙 Gold: {gold:,}\n"
            f"🪵 Wood: {wood:,}\n"
            f"⛏️ Ore: {ore:,}\n"
            f"💧 Mana: {mana:,}"
        )

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

from datetime import timedelta

def format_time_diff(diff: timedelta):
    days = diff.days
    hours = diff.seconds // 3600
    return f"(in {days}d {hours}h)" if days or hours else "(now)"

EVENT_MSG_FILE = "last_event_msg.json"

# New background task function
async def send_upcoming_events():
    try:
        sheet = client.open("Event Schedule").sheet1
        data = sheet.get_all_records()

        now = datetime.now(UTC)
        later = now + timedelta(days=3)

        upcoming = []
        for row in data:
            event_time = datetime.fromisoformat(row["start_time_utc"].strip().replace("Z", "+00:00"))
            if now <= event_time <= later:
                unix_ts = int(event_time.timestamp())
                upcoming.append((unix_ts, row["message"]))

        if not upcoming:
            return

        msg = "**\ud83d\uddd3\ufe0f Upcoming Events (next 3 days):**\n"
        for unix_ts, message in sorted(upcoming):
            event_dt = datetime.fromtimestamp(unix_ts, UTC)
            diff = event_dt - now
            time_diff_str = format_time_diff(diff)
            msg += f"> <t:{unix_ts}:F> — {message} {time_diff_str}\n"

        channel_id = 1290167968080330782
        channel = bot.get_channel(channel_id)
        if not channel:
            return

        # 🔥 Try to delete previous message
        if os.path.exists(EVENT_MSG_FILE):
            with open(EVENT_MSG_FILE, "r") as f:
                data = json.load(f)
                last_msg_id = data.get("message_id")
                if last_msg_id:
                    try:
                        old_msg = await channel.fetch_message(last_msg_id)
                        await old_msg.delete()
                    except Exception as e:
                        print(f"Couldn't delete previous message: {e}")

        # ✅ Send new message
        new_msg = await channel.send(msg)

        # 💾 Save new message ID
        with open(EVENT_MSG_FILE, "w") as f:
            json.dump({"message_id": new_msg.id}, f)

    except Exception as e:
        print(f"[Scheduled Task Error] {e}")

# Background scheduler
@tasks.loop(count=1)
async def scheduled_event_check():
    await bot.wait_until_ready()
    now = datetime.now(UTC)
    target = now.replace(hour=12, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    delay = (target - now).total_seconds()
    await asyncio.sleep(delay)
    await send_upcoming_events()
    scheduled_event_check.restart()

@bot.command()
async def test_events(ctx):
    await send_upcoming_events()

# =================== EVENT ADD (DO=entered UTC time) + AUTO-PING ===================
from datetime import datetime, timedelta, timezone
from discord.ext import tasks, commands
import asyncio, os, json, re

UTC = timezone.utc

# ---------- Fixed Target Channel (all pings go here) ----------
TARGET_CHANNEL_ID = 1257468695400153110

# ---------- Google Sheet config ----------
SHEET_NAME = "Event Schedule"
SHEET_HEADERS = ["event_name","start_time_utc","channel_id","message","event_type","ping_role_id"]

# ---------- Roles ----------
DEFAULT_ROLE_ID = 1235729244605120572  # test role; change later if needed

# ---------- Reminder schedules ----------
REMINDERS = {
    "caravan": [timedelta(days=1), timedelta(hours=1), timedelta(minutes=10)],
    "shadow_fort": [timedelta(days=1), timedelta(hours=1), timedelta(minutes=10)],
    "alliance_mobilization": [timedelta(days=1)],
    "behemoth": [timedelta(days=1), timedelta(hours=1), timedelta(minutes=10)],
    "pass": [timedelta(days=1), timedelta(hours=1), timedelta(minutes=10)],# only 1 day prior
}

# ---------- Tolerances & state ----------
FIRE_WINDOW    = timedelta(minutes=7)   # fire up to +7m late
CATCHUP_WINDOW = timedelta(hours=12)    # backfill up to 12h late after downtime
SENT_STATE_FILE = "sent_event_pings.json"

# =================== Helpers ===================
def _load_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _save_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f)
    os.replace(tmp, path)

def _ensure_headers(ws):
    heads = [h.strip() for h in ws.row_values(1)]
    if heads != SHEET_HEADERS:
        ws.update("A1", [SHEET_HEADERS])

def _append_row(row):
    ws = client.open(SHEET_NAME).sheet1
    _ensure_headers(ws)
    ws.append_row([row.get(h, "") for h in SHEET_HEADERS], value_input_option="RAW")

def _parse_event_time_utc(s: str) -> datetime | None:
    """
    Accepts:
      - 'MM/DD HH'
      - 'MM/DD HH:MM'
      - 'MM/DD/YYYY HH' or 'MM/DD/YYYY HH:MM'
      - optional trailing 'utc'
    Returns: timezone-aware UTC datetime (event DO time).
    """
    s = s.strip().lower().replace(" utc", "")
    now = datetime.now(UTC)
    m = re.match(r"^\s*(\d{1,2})/(\d{1,2})(?:/(\d{4}))?\s+(\d{1,2})(?::(\d{2}))?\s*$", s)
    if not m:
        return None
    mm = int(m.group(1)); dd = int(m.group(2))
    yyyy = int(m.group(3)) if m.group(3) else now.year
    HH = int(m.group(4)); MM = int(m.group(5)) if m.group(5) else 0
    try:
        dt = datetime(yyyy, mm, dd, HH, MM, 0, tzinfo=UTC)
        # If year omitted and it's >1 day in the past, roll to next year (year boundary helper)
        if not m.group(3) and (dt < now - timedelta(days=1)):
            dt = datetime(yyyy + 1, mm, dd, HH, MM, 0, tzinfo=UTC)
        return dt
    except ValueError:
        return None

def _read_events():
    """
    Reads sheet rows and returns normalized event dicts.
    NOTE: start_time_utc is the DO (actual event) time in UTC.
    """
    ws = client.open(SHEET_NAME).sheet1
    data = ws.get_all_records()
    events = []
    for r in data:
        try:
            etype = str(r.get("event_type","")).strip().lower()
            if etype not in REMINDERS:
                continue

            start_str = str(r.get("start_time_utc","")).strip()
            if not start_str:
                continue
            iso = start_str[:-1] + "+00:00" if start_str.endswith("Z") else start_str
            do_dt = datetime.fromisoformat(iso)
            do_dt = do_dt.replace(tzinfo=UTC) if do_dt.tzinfo is None else do_dt.astimezone(UTC)

            role_raw = str(r.get("ping_role_id","")).strip()
            role_id = int(role_raw) if role_raw.isdigit() else DEFAULT_ROLE_ID

            # Always use the fixed target channel
            channel_id = TARGET_CHANNEL_ID
            name = str(r.get("event_name","")).strip()
            msg  = str(r.get("message","")).strip()

            eid = f"{etype}|{int(do_dt.timestamp())}"

            events.append({
                "id": eid,
                "type": etype,
                "start": do_dt,
                "role_id": role_id,
                "channel_id": channel_id,
                "name": name,
                "message": msg,
            })
        except Exception as ex:
            print(f"[read_events] skip row error: {ex}")
            continue
    return events

# =================== Auto-ping core ===================
async def _maybe_fire_reminders():
    now = datetime.now(UTC)
    sent = _load_json(SENT_STATE_FILE, {})

    for e in _read_events():
        for off in REMINDERS[e["type"]]:
            fire_time = e["start"] - off

            should_fire = (
                (fire_time <= now <= fire_time + FIRE_WINDOW)
                or (now > fire_time and (now - fire_time) <= CATCHUP_WINDOW)
            )
            if not should_fire:
                continue

            key = f'{e["id"]}@-{int(off.total_seconds())}'
            if key in sent:
                continue  # already sent

            ch = bot.get_channel(TARGET_CHANNEL_ID)
            if not ch:
                print(f"[auto-ping] channel not found: {TARGET_CHANNEL_ID}")
                continue

            ts = int(e["start"].timestamp())
            pretty = {
                "caravan": "Caravan",
                "shadow_fort": "Shadow Fort",
                "alliance_mobilization": "Alliance Mobilization",
                "behemoth": "Behemoth",
                "pass": "Pass Opening",
            }[e["type"]]

            txt = f"<@&{e['role_id']}> **{pretty}** — starts <t:{ts}:R> (<t:{ts}:F>)"
            if e["message"]:
                txt += f"\n{e['message']}"

            try:
                await ch.send(txt)
                sent[key] = now.isoformat()
            except Exception as ex:
                print(f"[auto-ping send error] {ex}")

    _save_json(SENT_STATE_FILE, sent)

@tasks.loop(seconds=30)  # keep tight; you can switch to minutes=1 later
async def event_autoping_loop():
    await bot.wait_until_ready()
    try:
        await _maybe_fire_reminders()
    except Exception as e:
        # Prevent the loop from dying silently
        print(f"[event_autoping_loop ERROR] {e}")

# =================== Commands ===================
# NOTE: Remove the permission gate; add your own role/user gate if needed.
@bot.command()
async def add(ctx, kind: str, *, when: str):
    """
    Add an event at an exact UTC time (DO time).
    Examples:
      !add caravan 10/22 14
      !add shadow_fort 10/25 13:30
      !add alliance_mobilization 10/29 14
    """
    kind = kind.lower().strip()
    if kind not in REMINDERS:
        return await ctx.send("Type must be: `caravan`, `shadow_fort`, or `alliance_mobilization`.")

    do_dt = _parse_event_time_utc(when)
    if not do_dt:
        return await ctx.send("Invalid time. Use `MM/DD HH[:MM]` UTC (optionally `/YYYY`).")

    names = {"caravan":"Caravan","shadow_fort":"Shadow Fort","alliance_mobilization":"Alliance Mobilization"}
    messages = {
        "caravan": "🛒 Caravan at 14:00 UTC.",
        "shadow_fort": "🏰 Shadow Fort at 14:00 UTC.",
        "alliance_mobilization": "📣 Alliance Mobilization.",
    }

    row = {
        "event_name": names[kind],
        "start_time_utc": do_dt.isoformat().replace("+00:00","Z"),  # DO time in sheet
        "channel_id": str(TARGET_CHANNEL_ID),                       # fixed channel
        "message": messages[kind],
        "event_type": kind,
        "ping_role_id": str(DEFAULT_ROLE_ID),
    }

    try:
        _append_row(row)
    except Exception as e:
        return await ctx.send(f"Sheet write failed: {e}")

    ts = int(do_dt.timestamp())
    await ctx.send(
        f"✅ {names[kind]} set for <t:{ts}:F>. "
        f"Pings: {'−1d, −1h, −10m' if kind!='alliance_mobilization' else '−1d only'}."
    )

@bot.command()
async def peek(ctx, n: int = 10):
    """Show upcoming events with DO times & their computed fire times (UTC)."""
    evs = _read_events()
    lines = []
    for e in evs[:n]:
        fires = [e["start"] - off for off in REMINDERS[e["type"]]]
        fires_str = ", ".join(ft.strftime("%Y-%m-%d %H:%M:%S") for ft in fires)
        lines.append(f"{e['type']:<21} DO={e['start']:%Y-%m-%d %H:%M:%S} | fires: {fires_str}")
    if not lines:
        return await ctx.send("No events parsed.")
    await ctx.send("```\n" + "\n".join(lines) + "\n```")

@bot.command()
async def loopstatus(ctx):
    await ctx.send(f"loop running: {event_autoping_loop.is_running()}")

@bot.command()
@commands.has_permissions(manage_guild=True)
async def eventreset(ctx):
    """Clear sent-state so reminders can fire again (testing)."""
    try:
        if os.path.exists(SENT_STATE_FILE):
            os.remove(SENT_STATE_FILE)
        await ctx.send("✅ Sent-state reset. Reminders can fire again.")
    except Exception as e:
        await ctx.send(f"Couldn't reset: {e}")

@bot.command()
@commands.has_permissions(manage_guild=True)
async def due(ctx, minutes: int = 120):
    """Show reminders scheduled within ±<minutes> minutes from now."""
    now = datetime.now(UTC)
    lo, hi = now - timedelta(minutes=minutes), now + timedelta(minutes=minutes)
    rows = []
    for e in _read_events():
        for off in REMINDERS[e["type"]]:
            fire = e["start"] - off
            if lo <= fire <= hi:
                rows.append(f"{e['type']:<16} fire={fire:%Y-%m-%d %H:%M}  DO={e['start']:%Y-%m-%d %H:%M}")
    await ctx.send("```\n" + ("\n".join(rows) if rows else "No reminders in that window.") + "\n```")

@bot.command()
@commands.has_permissions(manage_guild=True)
async def resend(ctx, *, contains: str = ""):
    """
    Force-send any due/overdue reminders (within catch-up window) whose label matches <contains>.
    Example: !resend caravan
    """
    now = datetime.now(UTC)
    count = 0
    for e in _read_events():
        label = f"{e['type']}|{e['name']}|{e['start']:%Y-%m-%d %H:%M}"
        if contains and contains.lower() not in label.lower():
            continue
        for off in REMINDERS[e["type"]]:
            fire = e["start"] - off
            if now >= fire and (now - fire) <= CATCHUP_WINDOW:
                ch = bot.get_channel(TARGET_CHANNEL_ID)
                if not ch:
                    continue
                ts = int(e["start"].timestamp())
                pretty = {"caravan":"Caravan","shadow_fort":"Shadow Fort","alliance_mobilization":"Alliance Mobilization"}[e["type"]]
                txt = f"<@&{e['role_id']}> **{pretty}** — starts <t:{ts}:R> (<t:{ts}:F>)"
                if e["message"]:
                    txt += f"\n{e['message']}"
                await ch.send(txt)
                count += 1
    await ctx.send(f"Resent {count} reminder(s).")
# =================== END MODULE ===================

# Config values
CONFIRM_CHANNEL_ID = 1235711595645243394  # ID of the channel with the message + reactions
WAR_CHANNEL_ID = 1369071691111600168  # ⬅️ replace with your war channel ID
REACTION_MESSAGE_ID = 1369072129068372008  # ⬅️ replace with your message ID

# Emoji → new channel name mapping
WAR_CHANNEL_REACTIONS = {
    "🔴": "〘🔴〙war-status-fullwar",
    "🟢": "〘🟢〙war-status-no-fighting",
    "🟡": "〘🟡〙war-status-skirmishes",
    "🧑‍🌾": "〘🧑‍🌾〙war-status-go-farm",
}

@bot.event
async def on_raw_reaction_add(payload):
    print(f"Reaction detected: emoji={payload.emoji}, user={payload.user_id}, message={payload.message_id}")

    if payload.message_id != REACTION_MESSAGE_ID:
        return

    emoji = str(payload.emoji)
    print(f"Parsed emoji: {emoji}")
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
        await confirm_channel.send(f"✅ War channel renamed to `{new_name}` based on reaction {emoji}")
    except Exception as e:
        await confirm_channel.send(f"❌ Failed to rename war channel: {e}")

@bot.command()
async def stats(ctx, lord_id: str, season: str = DEFAULT_SEASON):
    try:
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season)
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Options: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = client.open(sheet_name).worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest, previous = tabs[-1], tabs[-2]
        data_latest = latest.get_all_values()
        data_prev = previous.get_all_values()
        headers = data_latest[0]

        id_index = headers.index("lord_id")
        name_index = 1
        alliance_index = 3
        power_idx = 12
        kills_idx = 9
        healed_idx = 18
        dead_idx = 17

        def to_int(val):
            try: return int(val.replace(',', '').replace('-', '').strip())
            except: return 0

        # Build prev map
        prev_map = {row[id_index]: row for row in data_prev[1:] if len(row) > dead_idx}

        # Find target rows
        row_latest = next((row for row in data_latest[1:] if row[id_index] == lord_id), None)
        row_prev = prev_map.get(lord_id)

        if not row_latest or not row_prev:
            await ctx.send("❌ Lord ID not found in both sheets.")
            return

        name = row_latest[name_index].strip()
        alliance = row_latest[alliance_index].strip()

        power_latest = to_int(row_latest[power_idx])
        power_gain = power_latest - to_int(row_prev[power_idx])

        kills_latest = to_int(row_latest[kills_idx])
        kills_gain = kills_latest - to_int(row_prev[kills_idx])

        healed_latest = to_int(row_latest[healed_idx])
        healed_gain = healed_latest - to_int(row_prev[healed_idx])

        dead_latest = to_int(row_latest[dead_idx])
        dead_gain = dead_latest - to_int(row_prev[dead_idx])

        # Rankings by gain within MFD only
        def build_rank_map(index):
            return sorted([
                (row[alliance_index], row[name_index], to_int(row[index]) - to_int(prev_map[row[id_index]][index]))
                for row in data_latest[1:]
                if row[id_index] in prev_map and row[alliance_index].strip() == "MFD"
            ], key=lambda x: x[2], reverse=True)

        power_rank = [x[1] for x in build_rank_map(power_idx)].index(name) + 1 if alliance == "MFD" else None
        kills_rank = [x[1] for x in build_rank_map(kills_idx)].index(name) + 1 if alliance == "MFD" else None
        dead_rank = [x[1] for x in build_rank_map(dead_idx)].index(name) + 1 if alliance == "MFD" else None
        healed_rank = [x[1] for x in build_rank_map(healed_idx)].index(name) + 1 if alliance == "MFD" else None

        msg = f"📊 Stats for `{lord_id}` ({name})\n"
        msg += f"🔹 Alliance: [{alliance}]\n\n"
        msg += f"🏆 Power:  {power_latest:,} (+{power_gain:,})"
        msg += f" — Rank #{power_rank} in MFD\n" if power_rank else "\n"

        msg += f"⚔️ Kills:  {kills_latest:,} (+{kills_gain:,})"
        msg += f" — Rank #{kills_rank} in MFD\n" if kills_rank else "\n"

        msg += f"💀 Dead:   {dead_latest:,} (+{dead_gain:,})"
        msg += f" — Rank #{dead_rank} in MFD\n" if dead_rank else "\n"

        msg += f"💉 Healed: {healed_latest:,} (+{healed_gain:,})"
        msg += f" — Rank #{healed_rank} in MFD" if healed_rank else ""

        if alliance != "MFD":
            msg += "\n\n❌ Not in MFD — Ranks not available."

        await ctx.send(msg)

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")
        
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
    # "Name (last6id)  •  S77"
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

# ============================
# 🚪 Kickcheck Command (card layout + hard deads floor for flex/abs)
# ============================

@bot.command()
async def kickcheck(ctx, scope: str = "mfd", season_prev: str = "sos5"):
    """
    Kick decision tool (card layout):
      KEEP if:
        - (Merits% ≥ 12 OR MeritsΔ ≥ 12,000,000) AND Deads% ≥ 0.30%
        - OR (flex/abs): (Merits% ≥ 20 OR MeritsΔ ≥ 12,000,000) AND Deads% ≥ 0.20% (hard floor)
      Else: if not in season_prev and failing -> WARNING, otherwise KICK.

      Uses gains between the last two tabs; Power ≥ 50M.
      Default scope 'mfd' = S77 only; use 'all' for all servers.
    """
    allowed_channels = {1378735765827358791, 1383515877793595435}
    if ctx.channel.id not in allowed_channels:
        await ctx.send("❌ Command not allowed here.")
        return

    # thresholds
    MERIT_REQ      = 12.0        # % of power
    MERIT_ABS_REQ  = 12_000_000  # absolute merits override
    DEAD_REQ       = 0.30        # % of power (normal rule)
    HIGH_MERIT     = 20.0        # % triggers "flex"
    HARD_DEAD_MIN  = 0.20        # % hard floor even for flex/abs
    MIN_POWER      = 50_000_000

    try:
        # current season sheets
        sheet_name = SEASON_SHEETS.get(DEFAULT_SEASON, DEFAULT_SEASON)
        tabs = client.open(sheet_name).worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest, previous = tabs[-1], tabs[-2]
        data_latest = latest.get_all_values()
        data_prev   = previous.get_all_values()
        if not data_latest or not data_prev:
            await ctx.send("❌ One of the worksheets is empty.")
            return

        headers = [h.strip().lower() for h in data_latest[0]]
        hmap = {h: i for i, h in enumerate(headers)}
        def col(*aliases, required=True, fallback=None):
            for a in aliases:
                a = a.strip().lower()
                if a in hmap:
                    return hmap[a]
            if required:
                raise ValueError(f"Missing column: one of {aliases}")
            return fallback

        id_idx     = col("lord_id")
        name_idx   = col("name", required=False, fallback=1)
        server_idx = col("home_server", "server", "home server")
        power_idx  = col("power", "m")
        merits_idx = col("merits", "merit", "merits (only 50m+ power)")
        dead_idx   = col("units_dead", "dead", "deads")

        max_needed = max(power_idx, merits_idx, dead_idx, server_idx, name_idx)
        prev_map = {
            (r[id_idx] or "").strip(): r
            for r in data_prev[1:]
            if len(r) > max_needed and (r[id_idx] or "").strip()
        }

        ONLY_S77 = (scope.lower() != "all")

        # last-season presence check (sos5 by default)
        prev_season_ids = set()
        try:
            prev_season_name = SEASON_SHEETS.get(season_prev.lower(), season_prev)
            tabs_prev = client.open(prev_season_name).worksheets()
            if tabs_prev:
                last = tabs_prev[-1]
                vals = last.get_all_values()
                if vals:
                    hdr = [h.strip().lower() for h in vals[0]]
                    if "lord_id" in hdr:
                        idx = hdr.index("lord_id")
                        for r in vals[1:]:
                            if len(r) > idx and (r[idx] or "").strip():
                                prev_season_ids.add((r[idx] or "").strip())
        except Exception:
            prev_season_ids = set()

        keep, kick, warn = [], [], []

        for row in data_latest[1:]:
            if len(row) <= max_needed:
                continue
            lid = (row[id_idx] or "").strip()
            if not lid:
                continue
            prev = prev_map.get(lid)
            if not prev:
                continue  # need both tabs

            # server filter
            srv_raw = (row[server_idx] or "").strip()
            srv = "".join(ch for ch in srv_raw if ch.isdigit())
            if ONLY_S77 and srv != "77":
                continue

            power = to_int_eu(row[power_idx])
            if power < MIN_POWER:
                continue

            # gains
            merits_gain = max(0, to_int_eu(row[merits_idx]) - to_int_eu(prev[merits_idx]))
            dead_gain   = max(0, to_int_eu(row[dead_idx])   - to_int_eu(prev[dead_idx]))

            # ratios
            merit_ratio = (merits_gain / power) * 100 if power > 0 else 0.0
            dead_ratio  = (dead_gain   / power) * 100 if power > 0 else 0.0

            # flags
            flex   = (merit_ratio >= HIGH_MERIT)
            abs_ok = (merits_gain >= MERIT_ABS_REQ)

            # decision
            meets = False
            if flex or abs_ok:
                # must still clear hard floor for deads
                if dead_ratio >= HARD_DEAD_MIN:
                    meets = True
            else:
                if (merit_ratio >= MERIT_REQ) and (dead_ratio >= DEAD_REQ):
                    meets = True

            entry = {
                "lid": lid,
                "name": (row[name_idx] or "").strip(),
                "srv": srv,
                "power": power,
                "merit_ratio": merit_ratio,
                "dead_ratio": dead_ratio,
                "merits_gain": merits_gain,
                "dead_gain": dead_gain,
                "prev_season": (lid in prev_season_ids),
                "flex": flex,
                "abs_ok": abs_ok,
            }

            if meets:
                keep.append(entry)
            else:
                if not entry["prev_season"]:
                    warn.append(entry)
                else:
                    kick.append(entry)

        # Sort for readability
        keep.sort(key=lambda x: (-x["merit_ratio"], -x["dead_ratio"], -x["power"]))
        kick.sort(key=lambda x: (x["merit_ratio"], x["dead_ratio"], -x["power"]))
        warn.sort(key=lambda x: (x["merit_ratio"], x["dead_ratio"], -x["power"]))

        # Summary
        scope_label = "MFD S77" if (scope.lower() != "all") else "All servers"
        summary = (
            f"**Kick Check — {previous.title} → {latest.title}**\n"
            f"Scope: {scope_label}, power ≥ 50M\n"
            f"Rule: (Merits ≥ **{MERIT_REQ:.0f}%** **OR** MeritsΔ ≥ **{fmt_int_eu(MERIT_ABS_REQ)}**) "
            f"AND Deads ≥ **{DEAD_REQ:.2f}%**\n"
            f"Flex/Abs exception: auto-keep only if **Deads ≥ {HARD_DEAD_MIN:.2f}%**\n"
            f"⚠️ Not in '{season_prev}' and failing → WARNING\n"
            f"Totals — ❌ Kick: **{len(kick)}** • ✅ Keep: **{len(keep)}** • ⚠️ Warning: **{len(warn)}**"
        )
        await ctx.send(embed=discord.Embed(description=summary, color=discord.Color.blurple()))

        # Card sections
        await send_section_cards(ctx, "KICK", "❌", discord.Color.red().value, kick)
        await send_section_cards(ctx, "KEEP", "✅", discord.Color.green().value, keep)
        await send_section_cards(ctx, "WARNING (not in last season & failing)", "⚠️", discord.Color.orange().value, warn)

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")
        
@bot.command()
async def totaldeads(ctx, *args):
    """
    Rank by TOTAL deaths (current value in Column R).
    Default: ALL players (≥25M power) in the default season.
    Add 'mfd' to filter to MFD on Server 77.

    Examples:
      !totaldeads                    -> Top 10, ALL players, default season
      !totaldeads 25                 -> Top 25, ALL players
      !totaldeads sos5               -> Top 10, ALL players, season 'sos5'
      !totaldeads sos5 30            -> Top 30, ALL players, season 'sos5'
      !totaldeads mfd 50             -> Top 50, MFD on Server 77
      !totaldeads all 50             -> Explicitly ALL, Top 50
    """
    allowed_channels = {1378735765827358791, 1383515877793595435}
    if ctx.channel.id not in allowed_channels:
        await ctx.send(f"❌ Commands are only allowed in <#{1378735765827358791}>.")
        return

    # Defaults
    top_n = 10
    season = DEFAULT_SEASON
    filter_mfd = False            # <-- default is ALL (no MFD filter)
    min_power = 25_000_000

    # Parse args flexibly
    for arg in args:
        a = str(arg).strip().lower()
        if a.isdigit():
            top_n = max(1, min(100, int(a)))
            continue
        if a in ("mfd", "mfd77"):
            filter_mfd = True
            continue
        if a in ("all", "*"):
            filter_mfd = False
            continue
        if a in SEASON_SHEETS:
            season = a
            continue
        await ctx.send(f"❌ Invalid argument '{arg}'. Seasons: {', '.join(SEASON_SHEETS.keys())} | Filters: 'mfd', 'all'.")
        return

    try:
        sheet_name = SEASON_SHEETS.get(season.lower())
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Available: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = client.open(sheet_name).worksheets()
        if len(tabs) < 1:
            await ctx.send("❌ No sheets found.")
            return

        latest = tabs[-1]
        data_latest = latest.get_all_values()
        if not data_latest:
            await ctx.send("❌ Sheet data is empty.")
            return

        headers = data_latest[0]

        # Indices
        id_index = headers.index("lord_id")       if "lord_id" in headers else 0
        name_index = 1                            # Column B
        alliance_index = 3                        # Column D
        power_index = 12                          # Column M
        dead_index = 17                           # Column R
        server_idx = headers.index("home_server") if "home_server" in headers else 5  # Column F fallback

        def to_int(v):
            try:
                return int(str(v).replace(",", "").replace("-", "").strip())
            except:
                return 0

        def is_mfd(tag: str) -> bool:
            return bool(tag) and tag.strip().upper().startswith("MFD")

        rows = []
        for row in data_latest[1:]:
            if len(row) <= max(dead_index, power_index, alliance_index, server_idx, id_index):
                continue

            lord_id = (row[id_index] or "").strip()
            if not lord_id:
                continue

            power = to_int(row[power_index])
            if power < min_power:
                continue

            alliance = (row[alliance_index] or "").strip()
            if filter_mfd:
                server_val = (row[server_idx] or "").strip()
                if not is_mfd(alliance) or str(server_val) != "77":
                    continue

            dead_now = to_int(row[dead_index])
            name = (row[name_index] or "?").strip()
            full_name = f"[{alliance}] {name}"
            rows.append((full_name, dead_now))

        scope = "MFD (S77)" if filter_mfd else "All"
        if not rows:
            await ctx.send(f"**💀 Total Deaths — Top {top_n} — {scope}**\n`{latest.title}`:\n_No eligible players found (≥25M power)._")
            return

        rows.sort(key=lambda x: x[1], reverse=True)
        top_rows = rows[:top_n]

        # Build lines
        lines = [f"{i+1}. `{name}` — 💀 {total:,}" for i, (name, total) in enumerate(top_rows)]

        # Chunked send
        header = f"**💀 Total Deaths — Top {top_n} — {scope}**\n`{latest.title}`:\n"
        chunk = header
        chunks = []
        for line in lines:
            if len(chunk) + len(line) + 1 > 2000:
                chunks.append(chunk.rstrip())
                chunk = "(cont.)\n"
            chunk += line + "\n"
        if chunk.strip():
            chunks.append(chunk.rstrip())

        for ch in chunks:
            try:
                await ctx.send(ch)
            except discord.HTTPException as e:
                if getattr(e, "code", None) == 50035 or getattr(e, "status", None) == 400:
                    await ctx.send("⚠️ Character limit reached — result was too long for Discord (2000 chars). Try a smaller N.")
                    return
                if getattr(e, "status", None) == 429:
                    await ctx.send("⏳ Rate limited. Try again in a moment.")
                    return
                await ctx.send(f"❌ Discord error: {e}")
                return

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def activity(ctx, *args):
    """
    Compare FIRST vs LATEST tab and report, per SERVER (filtered list only):
      - Active players (merits increased, only if power >= 40M)
      - Total merits gained

    Usage:
      !activity statue
      !activity Activity 10
    """
    allowed_channels = {1378735765827358791, 1383515877793595435}
    if ctx.channel.id not in allowed_channels:
        await ctx.send(f"❌ Commands are only allowed in <#{1378735765827358791}>.")
        return

    if not args:
        await ctx.send("❌ Usage: `!activity <workbook_name|season_key> [topN]`")
        return

    # Server filter + labels
    SERVER_LABELS = {
        "183": "A2G",
        "99":  "BTX",
        "92":  "wAo",
        "283": "RFF",
        "77":  "MFD",
        "110": "RoG",
    }
    ALLOWED_SERVERS = set(SERVER_LABELS.keys())
    POWER_THRESHOLD = 40_000_000

    # Parse args
    top_n = 20
    tokens = []
    for a in args:
        if str(a).isdigit():
            top_n = max(1, min(200, int(a)))
        else:
            tokens.append(str(a))
    sheet_token = " ".join(tokens).strip()
    if not sheet_token:
        await ctx.send("❌ Provide a sheet name or season key. Example: `!activity statue`")
        return

    def safe_int(v, default=0):
        try:
            s = str(v).strip().replace(",", "")
            if s in ("", "-", "None", "null"): return default
            return int(float(s))
        except Exception:
            return default

    def extract_server(v: str) -> str:
        """Return only digits from the server field (e.g., 'S77' -> '77')."""
        if v is None:
            return ""
        digits = "".join(ch for ch in str(v).strip() if ch.isdigit())
        return digits

    try:
        # Season key OR exact title
        book_name = SEASON_SHEETS.get(sheet_token.lower(), sheet_token)
        wb = client.open(book_name)

        tabs = wb.worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Need at least two tabs (first + latest) in that workbook.")
            return

        ws_base  = tabs[0]    # FIRST tab
        ws_later = tabs[-1]   # LATEST tab

        base  = ws_base.get_all_values()
        later = ws_later.get_all_values()
        if not base or not later:
            await ctx.send("❌ One of the scan tabs is empty.")
            return

        # Headers
        hdr_b = {h: i for i, h in enumerate(base[0])}
        hdr_l = {h: i for i, h in enumerate(later[0])}
        for req in ("lord_id", "merits", "power"):
            if req not in hdr_b or req not in hdr_l:
                await ctx.send("❌ Both tabs must include headers: `lord_id`, `merits`, `power` (plus optional `home_server`).")
                return

        id_b, mer_b, pow_b = hdr_b["lord_id"], hdr_b["merits"], hdr_b["power"]
        id_l, mer_l, pow_l = hdr_l["lord_id"], hdr_l["merits"], hdr_l["power"]
        srv_b = hdr_b.get("home_server")
        srv_l = hdr_l.get("home_server")

        # De-dupe by lord_id per tab (keep LAST)
        base_map = {}
        for r in base[1:]:
            lid = (r[id_b] if len(r) > id_b else "").strip()
            if not lid: continue
            srv_val = (r[srv_b] if (srv_b is not None and len(r) > srv_b) else "")
            m = safe_int(r[mer_b])
            p = safe_int(r[pow_b])
            base_map[lid] = (extract_server(srv_val), m, p)

        later_map = {}
        for r in later[1:]:
            lid = (r[id_l] if len(r) > id_l else "").strip()
            if not lid: continue
            srv_val = (r[srv_l] if (srv_l is not None and len(r) > srv_l) else "")
            m = safe_int(r[mer_l])
            p = safe_int(r[pow_l])
            later_map[lid] = (extract_server(srv_val), m, p)

        # Aggregate by SERVER only (filtered set, power filter)
        agg = {}  # server -> {"active": int, "merits": int}
        for lid, (srv1, m1, p1) in base_map.items():
            if lid not in later_map:
                continue
            srv2, m2, p2 = later_map[lid]
            server = srv2 or srv1
            if server not in ALLOWED_SERVERS:
                continue

            # Require power >= threshold in either scan
            power = max(p1, p2)
            if power < POWER_THRESHOLD:
                continue

            delta = m2 - m1
            if delta <= 0:
                continue

            bucket = agg.get(server, {"active": 0, "merits": 0})
            bucket["active"] += 1
            bucket["merits"] += delta
            agg[server] = bucket

        # Build rows & sort (by merits desc, then active desc)
        rows = [(srv, d["active"], d["merits"]) for srv, d in agg.items() if srv in ALLOWED_SERVERS]
        rows.sort(key=lambda x: (x[2], x[1]), reverse=True)
        top_rows = rows[:top_n]

        header = (
            f"**📈 Activity Report (≥40M Power, first vs latest tab, by Server)**\n"
            f"`{ws_base.title}` → `{ws_later.title}` in `{wb.title}`\n"
        )
        if not top_rows:
            await ctx.send(header + "_No merit gains detected on the specified servers._")
            return

        # Lines (server + alliance tag)
        lines = []
        for i, (srv, active, merits) in enumerate(top_rows, start=1):
            tag = SERVER_LABELS.get(srv, "")
            lines.append(f"{i}. [{tag}] S{srv} — 👥 Active {active} — ⭐ +{merits:,}")

        # Chunked send
        chunk = header
        chunks = []
        for line in lines:
            if len(chunk) + len(line) + 1 > 2000:
                chunks.append(chunk.rstrip())
                chunk = "(cont.)\n"
            chunk += line + "\n"
        if chunk.strip():
            chunks.append(chunk.rstrip())

        for ch in chunks:
            await ctx.send(ch)

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def mana(ctx, lord_id: str, season: str = DEFAULT_SEASON):
    try:
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season)
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Options: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = client.open(sheet_name).worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest, previous = tabs[-1], tabs[-2]
        data_latest, data_prev = latest.get_all_values(), previous.get_all_values()
        headers = data_latest[0]

        id_index = headers.index("lord_id")
        name_index = 1
        alliance_index = 3
        mana_index = 26  # Column AA

        def to_int(val):
            try: return int(val.replace(',', '').replace('-', '').strip())
            except: return 0

        def find_row(data):
            for row in data[1:]:
                if len(row) > mana_index and row[id_index].strip() == lord_id:
                    return row
            return None

        row_latest = find_row(data_latest)
        row_prev = find_row(data_prev)

        if not row_latest or not row_prev:
            await ctx.send("❌ Lord ID not found in both sheets.")
            return

        alliance = row_latest[alliance_index].strip() if len(row_latest) > alliance_index else ""
        name = row_latest[name_index].strip()
        mana_gain = to_int(row_latest[mana_index]) - to_int(row_prev[mana_index])

        # Filter MFD players only for ranking
        mfd_gains = []
        for row in data_latest[1:]:
            if len(row) > mana_index and row[alliance_index].strip().startswith("MFD"):
                id_val = row[id_index].strip()
                row_old = next((r for r in data_prev[1:] if len(r) > mana_index and r[id_index].strip() == id_val), None)
                if row_old:
                    gain = to_int(row[mana_index]) - to_int(row_old[mana_index])
                    mfd_gains.append((id_val, gain))

        mfd_gains.sort(key=lambda x: x[1], reverse=True)
        rank = next((i+1 for i, (lid, _) in enumerate(mfd_gains) if lid == lord_id), None)

        message = f"🌿 Mana gathered by `[{alliance}] {name}` in `{season.upper()}`:\n💧 Mana: {mana_gain:,}"
        if alliance.startswith("MFD") and rank:
            message += f"\n🏅 MFD Rank: #{rank}"
        else:
            message += "\n❌ Not in MFD"

        await ctx.send(message)

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def topmana(ctx, *args):
    allowed_channels = {1378735765827358791, 1383515877793595435}
    if ctx.channel.id not in allowed_channels:
        await ctx.send(f"❌ Commands are only allowed in <#{1378735765827358791}>.")
        return

    # Defaults
    top_n = 10
    season = DEFAULT_SEASON

    # Parse args: first number = top N, any other token = season
    for arg in args:
        if arg.isdigit():
            top_n = int(arg)
        else:
            season = arg.lower()

    try:
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season)
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Available: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = client.open(sheet_name).worksheets()
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
        mana_idx = 26  # AA
        power_idx = 12 # M

        def to_int(val):
            try:
                return int(val.replace(',', '').replace('-', '').strip())
            except:
                return 0

        prev_map = {
            row[id_index]: {"mana": to_int(row[mana_idx])}
            for row in data_prev[1:]
            if len(row) > mana_idx and row[id_index]
        }

        gains = []
        for row in data_latest[1:]:
            if len(row) <= max(mana_idx, power_idx):
                continue
            lord_id = row[id_index]
            if lord_id not in prev_map:
                continue

            alliance = row[alliance_index].strip() if len(row) > alliance_index else ""
            name = f"[{alliance}] {row[name_index].strip()}"

            mana_now = to_int(row[mana_idx])
            mana_prev = prev_map[lord_id]["mana"]
            gain = mana_now - mana_prev
            power = to_int(row[power_idx])

            if power >= 25_000_000:
                gains.append((name, gain))

        if not gains:
            await ctx.send("No eligible players found (≥25M power and present in both sheets).")
            return

        gains.sort(key=lambda x: x[1], reverse=True)
        top_rows = gains[:top_n]

        # Build lines
        lines = [f"{i+1}. `{name}` — 💧 +{mana:,}" for i, (name, mana) in enumerate(top_rows)]

        # Chunked sending (<=2000 chars per message)
        header = f"📊 **Top {top_n} Mana Gains** (≥25M Power)\n`{previous.title}` → `{latest.title}`:\n"
        chunk = header
        for line in lines:
            if len(chunk) + len(line) + 1 > 2000:
                await ctx.send(chunk.rstrip())
                chunk = "(cont.)\n"
            chunk += line + "\n"
        if chunk.strip():
            await ctx.send(chunk.rstrip())

    except discord.HTTPException as e:
        # Friendly message on length/validation errors
        if getattr(e, "code", None) == 50035:
            await ctx.send("⚠️ Character limit reached — result was too long for Discord (2000 chars). Try a smaller range.")
        else:
            await ctx.send(f"❌ Discord error: {e}")
    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def topheal(ctx, top_n: int = 10, season: str = DEFAULT_SEASON):
    allowed_channels = {1378735765827358791, 1383515877793595435}
    if ctx.channel.id not in allowed_channels:
        await ctx.send(f"❌ Commands are only allowed in <#{1378735765827358791}>.")
        return

    try:
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season)
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Available: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = client.open(sheet_name).worksheets()
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
        heal_idx = 18   # Column S
        power_idx = 12  # Column M

        def to_int(val):
            try: return int(val.replace(',', '').replace('-', '').strip())
            except: return 0

        # Clean and map previous sheet IDs
        prev_map = {}
        for row in data_prev[1:]:
            if len(row) > heal_idx:
                raw_id = row[id_index].strip() if row[id_index] else ""
                if raw_id:
                    prev_map[raw_id] = to_int(row[heal_idx])

        gains = []
        for row in data_latest[1:]:
            if len(row) > max(heal_idx, power_idx):
                raw_id = row[id_index].strip() if row[id_index] else ""
                if raw_id not in prev_map:
                    continue  # skip if not in both

                alliance = row[alliance_index].strip() if len(row) > alliance_index else ""
                name = f"[{alliance}] {row[name_index].strip()}"
                healed_now = to_int(row[heal_idx])
                healed_prev = prev_map[raw_id]
                gain = healed_now - healed_prev
                power = to_int(row[power_idx])

                if power >= 25_000_000:
                    gains.append((name, gain))

        gains.sort(key=lambda x: x[1], reverse=True)
        result = "\n".join([f"{i+1}. `{name}` — ❤️‍🩹 +{heal:,}" for i, (name, heal) in enumerate(gains[:top_n])])

        await ctx.send(f"📊 **Top {top_n} Healers (Gain)** (≥25M Power)\n`{previous.title}` → `{latest.title}`:\n{result}")

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def toprssheal_mfd(ctx, *args):
    """
    Top/Bottom RSS heal gains for MFD members on Server 77 only.
    - Season arg like 'sos5' (must exist in SEASON_SHEETS)
    - Optional number arg for N: top/bottom N (default 10, clamped 1..50)
    """
    allowed_channels = {1378735765827358791, 1383515877793595435}
    if ctx.channel.id not in allowed_channels:
        await ctx.send(f"❌ Commands are only allowed in <#{1378735765827358791}>.")
        return

    # defaults
    season = DEFAULT_SEASON
    top_n = 10

    # parse args in any order
    try:
        for arg in args:
            a = str(arg).strip().lower()
            if a.isdigit():  # top_n
                top_n = max(1, min(50, int(a)))  # clamp to avoid spam
            else:
                # season key must exist
                if a in SEASON_SHEETS:
                    season = a
                else:
                    await ctx.send(f"❌ Invalid season '{arg}'. Available: {', '.join(SEASON_SHEETS.keys())}")
                    return

        sheet_name = SEASON_SHEETS.get(season)
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Available: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = client.open(sheet_name).worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = tabs[-1]
        previous = tabs[-2]

        data_latest = latest.get_all_values()
        data_prev = previous.get_all_values()
        headers = data_latest[0]

        # Column lookups (by header names where possible)
        id_index = headers.index("lord_id")
        name_index = 1               # Column B (Name)
        alliance_index = 3           # Column D (Alliance/tag)
        power_idx = 12               # Column M (Power)
        server_idx = headers.index("home_server") if "home_server" in headers else 5  # Column F fallback

        gold_idx = 31  # AF
        wood_idx = 32  # AG
        ore_idx  = 33  # AH
        mana_idx = 34  # AI

        def to_int(val):
            try:
                return int(str(val).replace(',', '').replace('-', '').strip())
            except:
                return 0

        def is_mfd(tag: str) -> bool:
            """Alliance tag starts with 'MFD' (covers MFD, MFD1, MFD2, etc.)."""
            if not tag:
                return False
            t = tag.strip().upper()
            return t.startswith("MFD")

        prev_map = {}
        for row in data_prev[1:]:
            if len(row) > mana_idx:
                raw_id = (row[id_index] or "").strip()
                if raw_id:
                    prev_map[raw_id] = {
                        "gold": to_int(row[gold_idx]),
                        "wood": to_int(row[wood_idx]),
                        "ore":  to_int(row[ore_idx]),
                        "mana": to_int(row[mana_idx]),
                    }

        # Build filtered gains list (MFD + server 77 + >=25M power + present in both sheets)
        records = []
        for row in data_latest[1:]:
            if len(row) <= mana_idx:
                continue

            raw_id = (row[id_index] or "").strip()
            if not raw_id or raw_id not in prev_map:
                continue

            # Server filter (must be 77)
            server_val = (row[server_idx] if len(row) > server_idx else "").strip()
            if str(server_val) != "77":
                continue

            # Alliance filter (MFD + variants)
            alliance = (row[alliance_index] if len(row) > alliance_index else "").strip()
            if not is_mfd(alliance):
                continue

            power = to_int(row[power_idx])
            if power < 25_000_000:
                continue

            name = (row[name_index] if len(row) > name_index else "?").strip()
            full_name = f"[{alliance}] {name}".strip()

            gold = to_int(row[gold_idx]) - prev_map[raw_id]["gold"]
            wood = to_int(row[wood_idx]) - prev_map[raw_id]["wood"]
            ore  = to_int(row[ore_idx])  - prev_map[raw_id]["ore"]
            mana = to_int(row[mana_idx]) - prev_map[raw_id]["mana"]
            total = gold + wood + ore + mana

            records.append((full_name, total, gold, wood, ore, mana))

        if not records:
            await ctx.send(
                f"📊 **MFD (S77) RSS spent** (≥25M Power)\n"
                f"`{previous.title}` → `{latest.title}`:\n_No eligible MFD players on Server 77 found._"
            )
            return

        # Sort once by total
        records.sort(key=lambda x: x[1], reverse=True)

        # Build Top N lines
        top_rows = records[:top_n]
        top_lines = [
            f"{i+1}. `{name}` — 💸 +{total:,} (🪙{gold:,} 🪵{wood:,} ⛏️{ore:,} 💧{mana:,})"
            for i, (name, total, gold, wood, ore, mana) in enumerate(top_rows)
        ]

        # Build Bottom N lines (lowest totals, include zeros)
        bottom_rows = sorted(records, key=lambda x: x[1])[:top_n]
        bottom_lines = [
            f"{i+1}. `{name}` — 💸 +{total:,} (🪙{gold:,} 🪵{wood:,} ⛏️{ore:,} 💧{mana:,})"
            for i, (name, total, gold, wood, ore, mana) in enumerate(bottom_rows)
        ]

        # Chunked sending (<=2000 chars per message)
        header_top = (
            f"📊 **MFD (S77) — Top {top_n} RSS Heal Gains** (≥25M Power)\n"
            f"`{previous.title}` → `{latest.title}`:\n"
        )
        header_bottom = f"\n📉 **MFD (S77) — Bottom {top_n} RSS Heal Gains**\n"

        # We combine both sections but still respect 2000 char chunks
        chunks = []
        chunk = header_top
        for line in top_lines:
            if len(chunk) + len(line) + 1 > 2000:
                chunks.append(chunk.rstrip())
                chunk = "(cont.)\n"
            chunk += line + "\n"

        # Append bottom header
        if len(chunk) + len(header_bottom) > 2000:
            chunks.append(chunk.rstrip())
            chunk = "(cont.)\n"
        chunk += header_bottom

        # Append bottom lines
        for line in bottom_lines:
            if len(chunk) + len(line) + 1 > 2000:
                chunks.append(chunk.rstrip())
                chunk = "(cont.)\n"
            chunk += line + "\n"

        if chunk.strip():
            chunks.append(chunk.rstrip())

        # Send chunks
        for ch in chunks:
            await ctx.send(ch)

    except discord.HTTPException as e:
        # Friendly messages
        if getattr(e, "code", None) == 50035 or getattr(e, "status", None) == 400:
            await ctx.send("⚠️ Character limit reached — result was too long for Discord (2000 chars). Try a smaller N.")
        elif getattr(e, "status", None) == 429:
            await ctx.send("⏳ Rate limited. Try again in a moment.")
        else:
            await ctx.send(f"❌ Discord error: {e}")
    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def toprssheal(ctx, *args):
    allowed_channels = {1378735765827358791, 1383515877793595435}
    if ctx.channel.id not in allowed_channels:
        await ctx.send(f"❌ Commands are only allowed in <#{1378735765827358791}>.")
        return

    season = DEFAULT_SEASON
    top_n = 10

    def to_int(val):
        try:
            return int(str(val).replace(',', '').replace('-', '').strip())
        except:
            return 0

    def fmt_abbr(n: int) -> str:
        sign = "-" if n < 0 else ""
        x = abs(n)
        if x >= 1_000_000_000:
            v, s = x / 1_000_000_000, "b"
        elif x >= 1_000_000:
            v, s = x / 1_000_000, "m"
        elif x >= 1_000:
            v, s = x / 1_000, "k"
        else:
            return f"{sign}{x}"
        txt = f"{v:.1f}".rstrip("0").rstrip(".")
        return f"{sign}{txt}{s}"

    def truncate(s: str, max_len: int = 64) -> str:
        return s if len(s) <= max_len else s[:max_len-1] + "…"

    # parse args (any order: number + season key)
    try:
        for arg in args:
            a = str(arg).strip().lower()
            if a.isdigit():
                top_n = max(1, min(50, int(a)))
            else:
                if a in SEASON_SHEETS:
                    season = a
                else:
                    await ctx.send(f"❌ Invalid season '{arg}'. Available: {', '.join(SEASON_SHEETS.keys())}")
                    return

        sheet_name = SEASON_SHEETS.get(season)
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Available: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = client.open(sheet_name).worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = tabs[-1]
        previous = tabs[-2]

        data_latest = latest.get_all_values()
        data_prev   = previous.get_all_values()
        headers = data_latest[0]

        id_index = headers.index("lord_id")
        name_index = 1
        alliance_index = 3
        power_idx = 12  # M

        gold_idx = 31  # AF
        wood_idx = 32  # AG
        ore_idx  = 33  # AH
        mana_idx = 34  # AI

        # build prev map
        prev_map = {}
        for row in data_prev[1:]:
            if len(row) > mana_idx:
                rid = (row[id_index] or "").strip()
                if rid:
                    prev_map[rid] = (
                        to_int(row[gold_idx]),
                        to_int(row[wood_idx]),
                        to_int(row[ore_idx]),
                        to_int(row[mana_idx]),
                    )

        # compute deltas
        gains = []
        for row in data_latest[1:]:
            if len(row) <= mana_idx:
                continue
            rid = (row[id_index] or "").strip()
            if not rid or rid not in prev_map:
                continue
            if to_int(row[power_idx]) < 25_000_000:
                continue

            name = (row[name_index] if len(row) > name_index else "?").strip()
            alliance = (row[alliance_index] if len(row) > alliance_index else "").strip()
            display = truncate(f"[{alliance}] {name}".strip())

            g0, w0, o0, m0 = prev_map[rid]
            g1 = to_int(row[gold_idx]); w1 = to_int(row[wood_idx]); o1 = to_int(row[ore_idx]); m1 = to_int(row[mana_idx])
            gold = g1 - g0; wood = w1 - w0; ore = o1 - o0; mana = m1 - m0
            total = gold + wood + ore + mana

            gains.append((display, total, gold, wood, ore, mana))

        gains.sort(key=lambda x: x[1], reverse=True)
        if not gains:
            await ctx.send(
                f"📊 **Top {top_n} RSS Spent** (includes heals + training) (≥25M Power)\n"
                f"`{previous.title}` → `{latest.title}`:\n_No eligible players found._"
            )
            return

        # Build two-line rows (robust to Unicode widths)
        header = (
            f"📊 **Top {top_n} RSS Spent** (includes heals + training) (≥25M Power)\n"
            f"`{previous.title}` → `{latest.title}`\n"
            f"Legend: 🪙 Gold · 🪵 Wood · ⛏️ Ore · 💧 Mana · 💸 Total\n"
        )
        SEP = "  —  "  # nice readable gap between name and numbers

        rows = []
        for i, (name, total, gold, wood, ore, mana) in enumerate(gains[:top_n], start=1):
            line = (
                f"{i}. {name}{SEP}"
                f"💸 {fmt_abbr(total)}  ·  "
                f"🪙 {fmt_abbr(gold)}  ·  "
                f"🪵 {fmt_abbr(wood)}  ·  "
                f"⛏️ {fmt_abbr(ore)}  ·  "
                f"💧 {fmt_abbr(mana)}"
            )
            rows.append(line)
            
        # chunked send
        chunk = header
        for block in rows:
            if len(chunk) + len(block) + 2 > 2000:
                await ctx.send(chunk.rstrip())
                chunk = "(cont.)\n"
            chunk += block + "\n"
        if chunk.strip():
            await ctx.send(chunk.rstrip())

    except discord.HTTPException as e:
        if getattr(e, "code", None) == 50035 or getattr(e, "status", None) == 400:
            await ctx.send("⚠️ Character limit reached — result was too long for Discord (2000 chars). Try a smaller range.")
        elif getattr(e, "status", None) == 429:
            await ctx.send("⏳ Rate limited. Try again in a moment.")
        else:
            await ctx.send(f"❌ Discord error: {e}")
    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def topmanaspent(ctx, *args):
    """
    Show Top N mana spent (delta between the last two tabs).
    Usage:
      !topmana           -> Top 10 for DEFAULT_SEASON
      !topmana 25        -> Top 25
      !topmana sos5      -> Top 10 for 'sos5'
      !topmana 20 sos5   -> Top 20 for 'sos5'
    Filters: only players present in BOTH tabs, Power ≥ 25M.
    """
    allowed_channels = {1378735765827358791, 1383515877793595435}
    if ctx.channel.id not in allowed_channels:
        await ctx.send(f"❌ Commands are only allowed in <#{1378735765827358791}>.")
        return

    season = DEFAULT_SEASON
    top_n = 10

    def to_int(val):
        # EU/US tolerant: strip dots, commas, spaces; '-' and '' -> 0
        try:
            s = str(val).replace(".", "").replace(",", "").replace(" ", "").strip()
            if s in ("", "-"):
                return 0
            return int(s)
        except:
            return 0

    def fmt_abbr(n: int) -> str:
        sign = "-" if n < 0 else ""
        x = abs(n)
        if x >= 1_000_000_000:
            v, s = x / 1_000_000_000, "b"
        elif x >= 1_000_000:
            v, s = x / 1_000_000, "m"
        elif x >= 1_000:
            v, s = x / 1_000, "k"
        else:
            return f"{sign}{x}"
        txt = f"{v:.1f}".rstrip("0").rstrip(".")
        return f"{sign}{txt}{s}"

    def truncate(s: str, max_len: int = 64) -> str:
        return s if len(s) <= max_len else s[:max_len-1] + "…"

    # parse args (any order: number + season key)
    try:
        for arg in args:
            a = str(arg).strip().lower()
            if a.isdigit():
                top_n = max(1, min(50, int(a)))
            else:
                if a in SEASON_SHEETS:
                    season = a
                else:
                    await ctx.send(f"❌ Invalid season '{arg}'. Available: {', '.join(SEASON_SHEETS.keys())}")
                    return

        sheet_name = SEASON_SHEETS.get(season)
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Available: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = client.open(sheet_name).worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = tabs[-1]
        previous = tabs[-2]

        data_latest = latest.get_all_values()
        data_prev   = previous.get_all_values()
        headers = data_latest[0]

        # required indices (0-based)
        id_index       = headers.index("lord_id")
        name_index     = 1
        alliance_index = 3
        power_idx      = 12  # M
        mana_idx       = 34  # AI

        # build prev map
        prev_map = {}
        for row in data_prev[1:]:
            if len(row) > mana_idx:
                rid = (row[id_index] or "").strip()
                if rid:
                    prev_map[rid] = to_int(row[mana_idx])

        # compute mana deltas
        entries = []
        for row in data_latest[1:]:
            if len(row) <= mana_idx:
                continue

            rid = (row[id_index] or "").strip()
            if not rid or rid not in prev_map:
                continue

            power = to_int(row[power_idx])
            if power < 25_000_000:
                continue

            name = (row[name_index] if len(row) > name_index else "?").strip()
            alliance = (row[alliance_index] if len(row) > alliance_index else "").strip()
            display = truncate(f"[{alliance}] {name}".strip())

            m_prev = prev_map[rid]
            m_curr = to_int(row[mana_idx])
            mana_spent = max(0, m_curr - m_prev)  # clamp corrections

            if mana_spent > 0:
                entries.append((display, mana_spent))

        if not entries:
            await ctx.send(
                f"💧 **Top {top_n} Mana Spent** (≥25M Power)\n"
                f"`{previous.title}` → `{latest.title}`:\n_No eligible players found._"
            )
            return

        entries.sort(key=lambda x: x[1], reverse=True)

        # Build output
        header = (
            f"💧 **Top {top_n} Mana Spent** (≥25M Power)\n"
            f"`{previous.title}` → `{latest.title}`\n"
        )

        lines = []
        for i, (name, mana) in enumerate(entries[:top_n], start=1):
            lines.append(f"{i}. {name}  —  💧 {fmt_abbr(mana)}")

        # chunked send
        chunk = header
        for line in lines:
            if len(chunk) + len(line) + 2 > 2000:
                await ctx.send(chunk.rstrip())
                chunk = "(cont.)\n"
            chunk += line + "\n"
        if chunk.strip():
            await ctx.send(chunk.rstrip())

    except discord.HTTPException as e:
        if getattr(e, "code", None) == 50035 or getattr(e, "status", None) == 400:
            await ctx.send("⚠️ Message too long for Discord. Try a smaller N.")
        elif getattr(e, "status", None) == 429:
            await ctx.send("⏳ Rate limited. Try again in a moment.")
        else:
            await ctx.send(f"❌ Discord error: {e}")
    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def kills(ctx, lord_id: str, season: str = DEFAULT_SEASON):
    try:
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season)
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Options: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = client.open(sheet_name).worksheets()
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

@bot.command()
async def topkills(ctx, top_n: int = 10, season: str = DEFAULT_SEASON):
    allowed_channels = {1378735765827358791, 1383515877793595435}
    if ctx.channel.id not in allowed_channels:
        await ctx.send(f"❌ Commands are only allowed in <#{1378735765827358791}>.")
        return

    try:
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season)
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Available: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = client.open(sheet_name).worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = tabs[-1]
        previous = tabs[-2]
        data_latest = latest.get_all_values()
        data_prev = previous.get_all_values()
        headers = data_latest[0]

        id_index = headers.index("lord_id")
        name_index = 1  # Column B
        alliance_index = 3  # Column D
        power_index = 12  # Column M
        kills_index = 9   # Column J

        def to_int(val):
            try: return int(val.replace(",", "").replace("-", "").strip())
            except: return 0

        # Build map from previous sheet
        prev_map = {
            row[id_index].strip(): to_int(row[kills_index])
            for row in data_prev[1:]
            if len(row) > kills_index and row[id_index].strip()
        }

        gains = []
        for row in data_latest[1:]:
            if len(row) <= kills_index:
                continue

            raw_id = row[id_index].strip()
            if not raw_id or raw_id not in prev_map:
                continue

            power = to_int(row[power_index])
            if power < 25_000_000:
                continue

            name = row[name_index].strip()
            alliance = row[alliance_index].strip()
            kills_now = to_int(row[kills_index])
            kills_then = prev_map[raw_id]
            gain = kills_now - kills_then

            full_name = f"[{alliance}] {name}"
            gains.append((full_name, gain))

        gains.sort(key=lambda x: x[1], reverse=True)

        lines = [
            f"{i+1}. `{name}` — ⚔️ +{gain:,}"
            for i, (name, gain) in enumerate(gains[:top_n])
        ]

        await ctx.send("**🏆 Top Kill Gains:**\n" + "\n".join(lines))

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def lowdeads(ctx, *args):
    """
    Lowest dead gains between the last two tabs.

    Usage examples:
      !lowdeads                         -> Bottom 10 overall (≥50M power)
      !lowdeads 25                     -> Bottom 25 overall
      !lowdeads sos5                   -> Bottom 10 for season 'sos5'
      !lowdeads sos5 30                -> Bottom 30 for 'sos5'
      !lowdeads mfd 50                 -> Bottom 50 for MFD on Server 77
      !lowdeads mfd sos5 30            -> MFD+S77, season 'sos5', bottom 30
      !lowdeads all 50                 -> Remove MFD filter and show bottom 50
    """
    allowed_channels = {1378735765827358791, 1383515877793595435}
    if ctx.channel.id not in allowed_channels:
        await ctx.send(f"❌ Commands are only allowed in <#{1378735765827358791}>.")
        return

    # Defaults
    top_n = 10
    season = DEFAULT_SEASON
    filter_mfd = False       # [MFD*] AND server == 77
    MIN_POWER = 50_000_000   # >= 50M only

    # ---- Parse args (any order) ----
    for arg in args:
        a = str(arg).strip().lower()
        if a.isdigit():
            top_n = max(1, min(100, int(a)))
            continue
        if a in ("mfd", "mfd77"):
            filter_mfd = True
            continue
        if a in ("all", "*"):
            filter_mfd = False
            continue
        if a in SEASON_SHEETS:
            season = a
            continue
        await ctx.send(
            f"❌ Invalid argument '{arg}'. Seasons: {', '.join(SEASON_SHEETS.keys())} | Filters: 'mfd', 'all'."
        )
        return

    try:
        sheet_name = SEASON_SHEETS.get(season.lower())
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Available: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = client.open(sheet_name).worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = tabs[-1]
        previous = tabs[-2]

        data_latest = latest.get_all_values()
        data_prev   = previous.get_all_values()
        if not data_latest or not data_prev:
            await ctx.send("❌ Sheet data is empty.")
            return

        headers = data_latest[0]

        # Column indices
        id_index      = headers.index("lord_id")        if "lord_id" in headers        else 0
        name_index    = 1
        alliance_idx  = 3
        server_idx    = headers.index("home_server")    if "home_server" in headers    else 5
        power_idx     = 12   # M
        dead_idx      = 17   # R

        def to_int(val):
            try:
                return int(str(val).replace(",", "").replace("-", "").strip())
            except:
                return 0

        def is_mfd(tag: str) -> bool:
            return bool(tag) and tag.strip().upper().startswith("MFD")

        # Build prev map (id -> deads then)
        prev_map = {}
        for row in data_prev[1:]:
            if len(row) > max(dead_idx, id_index):
                rid = (row[id_index] or "").strip()
                if rid:
                    prev_map[rid] = to_int(row[dead_idx])

        # Collect gains for IDs present in BOTH sheets, ≥50M, optional MFD+S77
        rows = []
        for row in data_latest[1:]:
            if len(row) <= max(dead_idx, power_idx, alliance_idx, server_idx, id_index):
                continue

            rid = (row[id_index] or "").strip()
            if not rid or rid not in prev_map:
                continue

            power = to_int(row[power_idx])
            if power < MIN_POWER:
                continue

            tag = (row[alliance_idx] or "").strip()
            if filter_mfd:
                server_val = str(row[server_idx] or "").strip()
                if not is_mfd(tag) or server_val != "77":
                    continue

            dead_then = prev_map.get(rid, 0)
            dead_now  = to_int(row[dead_idx])
            gain = dead_now - dead_then
            if gain < 0:
                gain = 0  # guard against corrections

            name = (row[name_index] or "?").strip()
            display = f"[{tag}] {name}"
            rows.append((display, gain))

        if not rows:
            scope = "MFD (S77)" if filter_mfd else "All"
            await ctx.send(
                f"**🔻 Lowest {top_n} Dead Gains — {scope} (≥50M Power)**\n"
                f"`{previous.title}` → `{latest.title}`:\n_No eligible players found._"
            )
            return

        # Sort ASC by gain (lowest first), then by name for stability
        rows.sort(key=lambda x: (x[1], x[0]))
        bottom = rows[:top_n]

        # Build lines
        lines = [f"{i+1}. `{name}` — 💀 +{gain:,}" for i, (name, gain) in enumerate(bottom)]

        # Header + chunked send
        scope = "MFD (S77)" if filter_mfd else "All"
        header = (
            f"**🔻 Lowest {top_n} Dead Gains — {scope} (≥50M Power)**\n"
            f"`{previous.title}` → `{latest.title}`:\n"
        )

        chunk = header
        chunks = []
        for line in lines:
            if len(chunk) + len(line) + 1 > 2000:
                chunks.append(chunk.rstrip())
                chunk = "(cont.)\n"
            chunk += line + "\n"
        if chunk.strip():
            chunks.append(chunk.rstrip())

        for ch in chunks:
            try:
                await ctx.send(ch)
            except discord.HTTPException as e:
                if getattr(e, "code", None) == 50035 or getattr(e, "status", None) == 400:
                    await ctx.send("⚠️ Character limit reached — result was too long for Discord (2000 chars). Try a smaller N.")
                    return
                if getattr(e, "status", None) == 429:
                    await ctx.send("⏳ Rate limited. Try again in a moment.")
                    return
                await ctx.send(f"❌ Discord error: {e}")
                return

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def lowmerits(ctx, *args):
    """
    Lowest merit gains between the last two tabs (IDs must be in both).
    Uses merits in column 12 and power in column 13 (1-based).
    Supports MFD (S77) filter. Requires power >= 50M.
    """
    allowed_channels = {1378735765827358791, 1383515877793595435}
    if ctx.channel.id not in allowed_channels:
        await ctx.send(f"❌ Commands are only allowed in <#{1378735765827358791}>.")
        return

    # Defaults
    top_n = 10
    season = DEFAULT_SEASON
    filter_mfd = False
    MIN_POWER = 50_000_000

    # Parse args
    for arg in args:
        a = str(arg).strip().lower()
        if a.isdigit():
            top_n = max(1, min(100, int(a)))
        elif a in ("mfd", "mfd77"):
            filter_mfd = True
        elif a in ("all", "*"):
            filter_mfd = False
        elif a in SEASON_SHEETS:
            season = a
        else:
            await ctx.send(f"❌ Invalid argument '{arg}'. Seasons: {', '.join(SEASON_SHEETS.keys())} | Filters: 'mfd', 'all'.")
            return

    try:
        sheet_name = SEASON_SHEETS.get(season.lower())
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Available: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = client.open(sheet_name).worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = tabs[-1]
        previous = tabs[-2]
        data_latest = latest.get_all_values()
        data_prev   = previous.get_all_values()
        if not data_latest or not data_prev:
            await ctx.send("❌ Sheet data is empty.")
            return

        headers = data_latest[0]
        hmap = {h.strip().lower(): i for i, h in enumerate(headers)}

        # Fixed positions you specified (1-based -> 0-based), with safe fallback to header if present
        id_index     = hmap.get("lord_id", 0)         # A by default
        name_index   = 1                               # B
        alliance_idx = 3                               # D
        server_idx   = hmap.get("home_server", 5)      # F
        merits_idx   = 11                              # column 12 (1-based)
        power_idx    = 12                              # column 13 (1-based)

        # robust int parser: keep digits only (handles 21.734.811, 21,734,811, spaces, NBSP)
        def to_int(val):
            s = str(val).replace("\u00A0", "").strip()
            digits = "".join(ch for ch in s if ch.isdigit())
            try:
                return int(digits) if digits else 0
            except:
                return 0

        def is_mfd(tag: str) -> bool:
            return bool(tag) and tag.strip().upper().startswith("MFD")

        # prev map (id -> merits then)
        prev_map = {}
        for row in data_prev[1:]:
            if len(row) > max(merits_idx, id_index):
                rid = (row[id_index] or "").strip()
                if rid:
                    prev_map[rid] = to_int(row[merits_idx])

        # gather (IDs in both, >=50M, optional MFD S77)
        rows = []
        for row in data_latest[1:]:
            if len(row) <= max(merits_idx, power_idx, alliance_idx, server_idx, id_index):
                continue
            rid = (row[id_index] or "").strip()
            if not rid or rid not in prev_map:
                continue

            power = to_int(row[power_idx])
            if power < MIN_POWER:
                continue

            tag = (row[alliance_idx] or "").strip()
            if filter_mfd:
                server_val = str(row[server_idx] or "").strip()
                if not is_mfd(tag) or server_val != "77":
                    continue

            m_then = prev_map.get(rid, 0)
            m_now  = to_int(row[merits_idx])
            gain = m_now - m_then
            if gain < 0:
                gain = 0  # clamp corrections

            name = (row[name_index] or "?").strip()
            display = f"[{tag}] {name}".strip()
            rows.append((display, gain))

        if not rows:
            scope = "MFD (S77)" if filter_mfd else "All"
            await ctx.send(f"**🔻 Lowest {top_n} Merits Gained — {scope} (≥50M Power)**\n`{previous.title}` → `{latest.title}`:\n_No eligible players found._")
            return

        # sort ascending by gain (lowest first), then name for stability
        rows.sort(key=lambda x: (x[1], x[0]))
        bottom = rows[:top_n]

        lines = [f"{i+1}. `{name}` — 🧠 +{gain:,}" for i, (name, gain) in enumerate(bottom)]

        scope = "MFD (S77)" if filter_mfd else "All"
        header = f"**🔻 Lowest {top_n} Merits Gained — {scope} (≥50M Power)**\n`{previous.title}` → `{latest.title}`:\n"

        chunk = header
        chunks = []
        for line in lines:
            if len(chunk) + len(line) + 1 > 2000:
                chunks.append(chunk.rstrip())
                chunk = "(cont.)\n"
            chunk += line + "\n"
        if chunk.strip():
            chunks.append(chunk.rstrip())

        for ch in chunks:
            try:
                await ctx.send(ch)
            except discord.HTTPException as e:
                if getattr(e, "code", None) == 50035 or getattr(e, "status", None) == 400:
                    await ctx.send("⚠️ Character limit reached — result was too long (2000 chars). Try a smaller N.")
                    return
                if getattr(e, "status", None) == 429:
                    await ctx.send("⏳ Rate limited. Try again in a moment.")
                    return
                await ctx.send(f"❌ Discord error: {e}")
                return

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def topdeads(ctx, *args):
    """
    Usage examples:
      !topdeads                         -> Top 10 overall, default season
      !topdeads 25                     -> Top 25 overall
      !topdeads sos5                   -> Top 10 for season 'sos5'
      !topdeads sos5 25                -> Top 25 for season 'sos5'
      !topdeads mfd 50                 -> Top 50 for MFD on Server 77 (your alliance)
      !topdeads mfd sos5 30            -> MFD+S77, season 'sos5', top 30
      !topdeads all 50                 -> Explicitly remove MFD filter and show top 50
    """
    allowed_channels = {1378735765827358791, 1383515877793595435}
    if ctx.channel.id not in allowed_channels:
        await ctx.send(f"❌ Commands are only allowed in <#{1378735765827358791}>.")
        return

    # Defaults
    top_n = 10
    season = DEFAULT_SEASON
    filter_mfd = False  # toggle for [MFD*] + server 77

    # --- Parse args in any order ---
    # digits -> top_n
    # season key -> season
    # 'mfd' -> filter to MFD on server 77
    # 'all' or '*' -> remove MFD filter explicitly
    for arg in args:
        a = str(arg).strip().lower()
        if a.isdigit():
            top_n = max(1, min(100, int(a)))  # clamp a bit
            continue
        if a in ("mfd", "mfd77"):
            filter_mfd = True
            continue
        if a in ("all", "*"):
            filter_mfd = False
            continue
        # season?
        if a in SEASON_SHEETS:
            season = a
            continue
        # Unknown token -> treat as invalid season token for clarity
        await ctx.send(f"❌ Invalid argument '{arg}'. Seasons: {', '.join(SEASON_SHEETS.keys())} | Filters: 'mfd', 'all'.")
        return

    try:
        sheet_name = SEASON_SHEETS.get(season.lower())
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Available: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = client.open(sheet_name).worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = tabs[-1]
        previous = tabs[-2]

        data_latest = latest.get_all_values()
        data_prev = previous.get_all_values()
        if not data_latest or not data_prev:
            await ctx.send("❌ Sheet data is empty.")
            return

        headers = data_latest[0]

        # Column indices (prefer header lookups where possible)
        id_index = headers.index("lord_id")      if "lord_id"      in headers else 0
        name_index = 1                           # Column B (Name)
        alliance_index = 3                       # Column D (Alliance/tag)
        power_index = 12                         # Column M (Power)
        dead_index = 17                          # Column R (Deads total)
        server_idx = headers.index("home_server") if "home_server" in headers else 5  # Column F fallback

        def to_int(val):
            try:
                return int(str(val).replace(",", "").replace("-", "").strip())
            except:
                return 0

        def is_mfd(tag: str) -> bool:
            if not tag:
                return False
            return tag.strip().upper().startswith("MFD")

        # Build previous map: lord_id -> deads_then
        prev_map = {}
        for row in data_prev[1:]:
            if len(row) > dead_index and len(row) > id_index:
                raw_id = (row[id_index] or "").strip()
                if raw_id:
                    prev_map[raw_id] = to_int(row[dead_index])

        # Collect gains (only players present in both sheets, ≥25M power, optional MFD+S77 filter)
        results = []
        for row in data_latest[1:]:
            if len(row) <= max(dead_index, power_index, alliance_index, server_idx, id_index):
                continue

            raw_id = (row[id_index] or "").strip()
            if not raw_id or raw_id not in prev_map:
                continue

            power = to_int(row[power_index])
            if power < 25_000_000:
                continue

            alliance = (row[alliance_index] or "").strip()
            if filter_mfd:
                server_val = (row[server_idx] or "").strip()
                if not is_mfd(alliance) or str(server_val) != "77":
                    continue

            dead_now = to_int(row[dead_index])
            dead_then = prev_map.get(raw_id, 0)
            gain = dead_now - dead_then
            if gain < 0:
                # Guard against sheet corrections; treat negatives as zero gain
                gain = 0

            name = (row[name_index] or "?").strip()
            full_name = f"[{alliance}] {name}"
            results.append((full_name, gain))

        if not results:
            scope = "MFD (S77)" if filter_mfd else "All"
            await ctx.send(f"**🏆 Top {top_n} Dead Units Gained — {scope}**\n`{previous.title}` → `{latest.title}`:\n_No eligible players found (≥25M power and present in both sheets)._")
            return

        # Sort and slice
        results.sort(key=lambda x: x[1], reverse=True)
        top_rows = results[:top_n]

        # Build lines
        lines = [f"{i+1}. `{name}` — 💀 +{gain:,}" for i, (name, gain) in enumerate(top_rows)]

        # Header + chunked send (<=2000 chars)
        scope = "MFD (S77)" if filter_mfd else "All"
        header = f"**🏆 Top {top_n} Dead Units Gained — {scope}**\n`{previous.title}` → `{latest.title}`:\n"

        chunk = header
        chunks = []
        for line in lines:
            if len(chunk) + len(line) + 1 > 2000:
                chunks.append(chunk.rstrip())
                chunk = "(cont.)\n"
            chunk += line + "\n"
        if chunk.strip():
            chunks.append(chunk.rstrip())

        # Send chunks with friendly errors
        for ch in chunks:
            try:
                await ctx.send(ch)
            except discord.HTTPException as e:
                if getattr(e, "code", None) == 50035 or getattr(e, "status", None) == 400:
                    await ctx.send("⚠️ Character limit reached — result was too long for Discord (2000 chars). Try a smaller N.")
                    return
                if getattr(e, "status", None) == 429:
                    await ctx.send("⏳ Rate limited. Try again in a moment.")
                    return
                await ctx.send(f"❌ Discord error: {e}")
                return

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def topmerits(ctx, *args):
    """
    Usage examples:
      !topmerits                          -> Top 10 overall (default season)
      !topmerits 25                      -> Top 25 overall
      !topmerits sos5                    -> Top 10 for season 'sos5'
      !topmerits sos5 25                 -> Top 25 for season 'sos5'
      !topmerits mfd 50                  -> Top 50 for MFD on Server 77
      !topmerits mfd sos5 30             -> MFD+S77, season 'sos5', top 30
      !topmerits all 50                  -> Remove MFD filter explicitly
    """
    allowed_channels = {1378735765827358791, 1383515877793595435}
    if ctx.channel.id not in allowed_channels:
        await ctx.send(f"❌ Commands are only allowed in <#{1378735765827358791}>.")
        return

    # Defaults
    top_n = 10
    season = DEFAULT_SEASON
    filter_mfd = False  # [MFD*] + Server 77

    # Parse args (any order)
    for arg in args:
        a = str(arg).strip().lower()
        if a.isdigit():
            top_n = max(1, min(100, int(a)))
            continue
        if a in ("mfd", "mfd77"):
            filter_mfd = True
            continue
        if a in ("all", "*"):
            filter_mfd = False
            continue
        if a in SEASON_SHEETS:
            season = a
            continue
        await ctx.send(f"❌ Invalid argument '{arg}'. Seasons: {', '.join(SEASON_SHEETS.keys())} | Filters: 'mfd', 'all'.")
        return

    try:
        sheet_name = SEASON_SHEETS.get(season.lower())
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Available: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = client.open(sheet_name).worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = tabs[-1]
        previous = tabs[-2]

        data_latest = latest.get_all_values()
        data_prev = previous.get_all_values()
        if not data_latest or not data_prev:
            await ctx.send("❌ Sheet data is empty.")
            return

        headers = data_latest[0]

        # Indices (prefer header lookups)
        def hidx(name, fallback=None):
            return headers.index(name) if name in headers else fallback

        id_index      = hidx("lord_id", 0)
        name_index    = 1                   # B
        alliance_idx  = 3                   # D
        power_idx     = 12                  # M
        server_idx    = hidx("home_server", 5)
        merits_idx    = hidx("merits (only 50m+ power)", 11)  # near K/L fallback

        def to_int(val):
            try:
                return int(str(val).replace(",", "").replace("-", "").strip())
            except:
                return 0

        def is_mfd(tag: str) -> bool:
            return bool(tag) and tag.strip().upper().startswith("MFD")

        # Build previous map: lord_id -> merits_then
        prev_map = {}
        for row in data_prev[1:]:
            if len(row) > max(merits_idx, id_index):
                raw_id = (row[id_index] or "").strip()
                if raw_id:
                    prev_map[raw_id] = to_int(row[merits_idx])

        # Collect gains (both sheets, ≥50M power, optional MFD+S77)
        results = []
        for row in data_latest[1:]:
            if len(row) <= max(merits_idx, power_idx, alliance_idx, server_idx, id_index):
                continue

            raw_id = (row[id_index] or "").strip()
            if not raw_id or raw_id not in prev_map:
                continue

            power = to_int(row[power_idx])
            if power < 50_000_000:
                continue

            alliance = (row[alliance_idx] or "").strip()
            if filter_mfd:
                server_val = (row[server_idx] or "").strip()
                if not is_mfd(alliance) or str(server_val) != "77":
                    continue

            merits_now  = to_int(row[merits_idx])
            merits_prev = prev_map.get(raw_id, 0)
            gain = merits_now - merits_prev
            if gain < 0:
                gain = 0  # guard against corrections

            name = (row[name_index] or "?").strip()
            full_name = f"[{alliance}] {name}".strip()
            results.append((full_name, gain))

        if not results:
            scope = "MFD (S77)" if filter_mfd else "All"
            await ctx.send(f"**🏅 Top {top_n} Merits Gained — {scope}**\n`{previous.title}` → `{latest.title}`:\n_No eligible players found (≥50M power and present in both sheets)._")
            return

        # Sort + slice
        results.sort(key=lambda x: x[1], reverse=True)
        top_rows = results[:top_n]

        # Build lines
        lines = [f"{i+1}. `{name}` — 🧠 +{gain:,}" for i, (name, gain) in enumerate(top_rows)]

        # Chunked send
        scope = "MFD (S77)" if filter_mfd else "All"
        header = f"**🏅 Top {top_n} Merits Gained — {scope}**\n`{previous.title}` → `{latest.title}`:\n"

        chunk = header
        chunks = []
        for line in lines:
            if len(chunk) + len(line) + 1 > 2000:
                chunks.append(chunk.rstrip())
                chunk = "(cont.)\n"
            chunk += line + "\n"
        if chunk.strip():
            chunks.append(chunk.rstrip())

        for ch in chunks:
            try:
                await ctx.send(ch)
            except discord.HTTPException as e:
                if getattr(e, "code", None) == 50035 or getattr(e, "status", None) == 400:
                    await ctx.send("⚠️ Character limit reached — result was too long for Discord (2000 chars). Try a smaller N.")
                    return
                if getattr(e, "status", None) == 429:
                    await ctx.send("⏳ Rate limited. Try again in a moment.")
                    return
                await ctx.send(f"❌ Discord error: {e}")
                return

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def progress(ctx, lord_id: str, season: str = DEFAULT_SEASON):
    try:
        season = season.lower()
        is_default_season = (season == DEFAULT_SEASON)
        sheet_name = SEASON_SHEETS.get(season)
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Options: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = client.open(sheet_name).worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = tabs[-1]
        previous = tabs[-2]

        data_latest = latest.get_all_values()
        data_prev = previous.get_all_values()
        headers = data_latest[0]

        def col_idx(col): return headers.index(col)

        id_idx = col_idx("lord_id")
        name_idx = 1
        alliance_idx = 3
        power_idx = headers.index("highest_power")
        kills_idx = headers.index("units_killed")
        dead_idx = headers.index("units_dead")
        healed_idx = headers.index("units_healed")
        gold_idx = headers.index("gold_spent")
        wood_idx = headers.index("wood_spent")
        ore_idx = headers.index("stone_spent")
        mana_idx = headers.index("mana_spent")
        t5_idx = headers.index("killcount_t5")
        t4_idx = headers.index("killcount_t4")
        t3_idx = headers.index("killcount_t3")
        t2_idx = headers.index("killcount_t2")
        t1_idx = headers.index("killcount_t1")
        gold_gathered_idx = headers.index("gold")
        wood_gathered_idx = headers.index("wood")
        ore_gathered_idx = headers.index("ore")
        mana_gathered_idx = headers.index("mana")
        home_server_idx = headers.index("home_server")
        merit_idx = headers.index("merits")  # L

        def idx_any(*names):
            for n in names:
                if n in headers:
                    return headers.index(n)
            raise ValueError(f"Missing column; tried: {names}")
        
        def to_int(v):
            try:
                return int(v.replace(",", "").strip()) if v not in ("-", "") else 0
            except:
                return 0

        def find_row(data):
            for row in data[1:]:
                if row[id_idx] == lord_id:
                    return row
            return None

        row_latest = find_row(data_latest)
        row_prev = find_row(data_prev)

        if not row_latest or not row_prev:
            await ctx.send("❌ Lord ID not found in both sheets.")
            return

        name = row_latest[name_idx]
        alliance = row_latest[alliance_idx]
        power_gain = to_int(row_latest[power_idx]) - to_int(row_prev[power_idx])
        power_latest = to_int(row_latest[power_idx])
        merit_latest = to_int(row_latest[merit_idx])
        merit_ratio = (merit_latest / to_int(row_latest[power_idx]) * 100) if to_int(row_latest[power_idx]) > 0 else 0
        kills_gain = to_int(row_latest[kills_idx]) - to_int(row_prev[kills_idx])
        dead_gain = to_int(row_latest[dead_idx]) - to_int(row_prev[dead_idx])
        healed_gain = to_int(row_latest[healed_idx]) - to_int(row_prev[healed_idx])
        gold = to_int(row_latest[gold_idx]) - to_int(row_prev[gold_idx])
        wood = to_int(row_latest[wood_idx]) - to_int(row_prev[wood_idx])
        ore = to_int(row_latest[ore_idx]) - to_int(row_prev[ore_idx])
        mana = to_int(row_latest[mana_idx]) - to_int(row_prev[mana_idx])
        total_rss = gold + wood + ore + mana
        gold_gathered = to_int(row_latest[gold_gathered_idx]) - to_int(row_prev[gold_gathered_idx])
        wood_gathered = to_int(row_latest[wood_gathered_idx]) - to_int(row_prev[wood_gathered_idx])
        ore_gathered = to_int(row_latest[ore_gathered_idx]) - to_int(row_prev[ore_gathered_idx])
        mana_gathered = to_int(row_latest[mana_gathered_idx]) - to_int(row_prev[mana_gathered_idx])
        total_gathered = gold_gathered + wood_gathered + ore_gathered + mana_gathered

        # Create lookup from previous sheet
        prev_map = {row[id_idx]: row for row in data_prev[1:] if len(row) > mana_idx and row[id_idx].strip()}

        def get_merit_ratio_rank():
            player_server = str(row_latest[home_server_idx]).strip()
            ratios = []
            for row in data_latest[1:]:
                if len(row) <= max(merit_idx, power_idx, home_server_idx):
                    continue
                if str(row[home_server_idx]).strip() != player_server:
                    continue
                p_power = to_int(row[power_idx])
                if p_power <= 0:
                    continue
                p_merit = to_int(row[merit_idx])
                p_ratio = (p_merit / p_power) * 100
                ratios.append((row[id_idx], p_ratio))

            ratios.sort(key=lambda x: x[1], reverse=True)
            for rank, (lid, _) in enumerate(ratios, 1):
                if lid == lord_id:
                    return rank
            return None
        
        rank_merit_ratio = get_merit_ratio_rank()
        
        def get_rank(col_index):
            player_row = next((r for r in data_latest[1:] if r[id_idx].strip() == lord_id), None)
            if not player_row or len(player_row) <= home_server_idx:
                return None

            player_server = str(player_row[home_server_idx]).strip()
            if not player_server:
                return None

            gains = []
            for row in data_latest[1:]:
                if len(row) <= col_index or len(row) <= home_server_idx:
                    continue
                if str(row[home_server_idx]).strip() != player_server:
                    continue

                lid = row[id_idx].strip()
                prev_row = prev_map.get(lid)
                if not prev_row:
                    continue

                val = to_int(row[col_index]) - to_int(prev_row[col_index])
                gains.append((lid, val))

            gains.sort(key=lambda x: x[1], reverse=True)

            for rank, (lid, _) in enumerate(gains, 1):
                if lid == lord_id:
                    return rank

            return None

        rank_power = get_rank(power_idx)
        rank_kills = get_rank(kills_idx)
        rank_dead = get_rank(dead_idx)
        rank_healed = get_rank(healed_idx)
        rank_merit = get_rank(merit_idx)

        t5_total = to_int(row_latest[t5_idx])
        t4_total = to_int(row_latest[t4_idx])
        t3_total = to_int(row_latest[t3_idx])
        t2_total = to_int(row_latest[t2_idx])
        t1_total = to_int(row_latest[t1_idx])

        t5_gain = t5_total - to_int(row_prev[t5_idx])
        t4_gain = t4_total - to_int(row_prev[t4_idx])
        t3_gain = t3_total - to_int(row_prev[t3_idx])
        t2_gain = t2_total - to_int(row_prev[t2_idx])
        t1_gain = t1_total - to_int(row_prev[t1_idx])

        embed = discord.Embed(title=f"📈 Progress Report for [{alliance}] {name} for season `{season.upper()}`", color=discord.Color.green())
        embed.add_field(name="🟩 Power", value=f"{power_latest:,} (+{power_gain:,})" + (f" (#{rank_power})" if rank_power else ""), inline=False)
        embed.add_field(name="🧠 Merits", value=f"{merit_latest:,} ({merit_ratio:.2f}%)" + (f" (#{rank_merit_ratio})" if rank_merit_ratio else ""), inline=False)
        embed.add_field(name="⚔️ Kills", value=f"+{kills_gain:,}" + (f" (#{rank_kills})" if rank_kills else ""), inline=True)
        embed.add_field(name="💀 Deads", value=f"+{dead_gain:,}" + (f" (#{rank_dead})" if rank_dead else ""), inline=True)
        embed.add_field(name="❤️ Healed", value=f"+{healed_gain:,}" + (f" (#{rank_healed})" if rank_healed else ""), inline=True)
        embed.add_field(
            name="• Kill Breakdown",
            value=(
                f"T5: {t5_total:,} (+{t5_gain:,})\n"
                f"T4: {t4_total:,} (+{t4_gain:,})\n"
                f"T3: {t3_total:,} (+{t3_gain:,})\n"
                f"T2: {t2_total:,} (+{t2_gain:,})\n"
                f"T1: {t1_total:,} (+{t1_gain:,})"
            ),
            inline=True
        )
        embed.add_field(
            name="📦 RSS Spent",
            value=(
                f"🪙 Gold: {gold:,}\n"
                f"🪵 Wood: {wood:,}\n"
                f"⛏️ Ore: {ore:,}\n"
                f"💧 Mana: {mana:,}\n"
                f"📦 Total: {total_rss:,}"
            ),
            inline=False
        )
        embed.add_field(
            name="🧑‍🌾 RSS Gathered",
            value=(
                f"🪙 Gold: {gold_gathered:,}\n"
                f"🪵 Wood: {wood_gathered:,}\n"
                f"⛏️ Ore: {ore_gathered:,}\n"
                f"💧 Mana: {mana_gathered:,}\n"
                f"📦 **Total**: {total_gathered:,}"
            ),
            inline=False
        )
        if is_default_season:
            embed.set_footer(
                text=(
                    f"📅 Timespan: {previous.title} → {latest.title}\n"
                    "To view stats from the previous season, add 'sos2' at the end of the command.\n"
                    "Example: !progress 123456 sos2"
                )
            )
        else:
            embed.set_footer(text=f"📅 Timespan: {previous.title} → {latest.title}")

        await ctx.send(embed=embed)

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def lowperformer(ctx, threshold: float = 12.0, season: str = DEFAULT_SEASON):
    """
    Show lowest performers for MFD (S77) using the same scoring as topperformer,
    but filtered to merit_ratio < threshold and sorted by score ascending.

    Score weights: 40% merits, 40% deaths, 20% heals.
    Death target = 0.4% of power:
      - below target: quadratic penalty
      - above target: reward (capped)
    Only players present in BOTH tabs, power ≥ 50M, server == 77.
    """
    allowed_channels = {1378735765827358791, 1383515877793595435}
    if ctx.channel.id not in allowed_channels:
        await ctx.send(f"❌ Commands are only allowed in <#{1378735765827358791}>.")
        return

    try:
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season)
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Options: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = client.open(sheet_name).worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = tabs[-1]
        previous = tabs[-2]
        data_latest = latest.get_all_values()
        data_prev   = previous.get_all_values()
        headers = data_latest[0]

        def col_to_index(col):
            col = col.upper()
            idx = 0
            for ch in col:
                idx = idx * 26 + (ord(ch) - ord('A') + 1)
            return idx - 1

        id_idx    = headers.index("lord_id")
        name_idx  = 1
        serv_idx  = col_to_index("F")   # home_server
        power_idx = col_to_index("M")
        merit_idx = col_to_index("L")
        kills_idx = col_to_index("J")
        dead_idx  = col_to_index("R")
        heal_idx  = col_to_index("S")
        helps_idx = col_to_index("AE")

        def to_int(val):
            try:
                val = str(val).replace(",", "").strip()
                if val == "-" or not val:
                    return 0
                return int(val)
            except:
                return 0

        # map of previous rows (only ids present)
        prev_map = {row[id_idx]: row for row in data_prev[1:] if len(row) > helps_idx and (row[id_idx] or "").strip()}

        MIN_POWER     = 50_000_000
        DEAD_TARGET   = 0.4   # percent of power
        WEIGHT_MERIT  = 0.40
        WEIGHT_DEAD   = 0.40
        WEIGHT_HEAL   = 0.20

        # collect candidates (present in both, S77, power ≥ 50M, merit_ratio < threshold)
        people = []
        for row in data_latest[1:]:
            if len(row) <= helps_idx: 
                continue
            lid = (row[id_idx] or "").strip()
            if not lid or lid not in prev_map:
                continue
            if (row[serv_idx] or "").strip() != "77":
                continue

            power = to_int(row[power_idx])
            if power < MIN_POWER:
                continue

            merit = to_int(row[merit_idx])
            merit_ratio = (merit / power) * 100 if power > 0 else 0.0
            if merit_ratio >= threshold:
                continue  # we're focusing on low performers (below target)

            prev = prev_map[lid]
            kills_gain  = to_int(row[kills_idx]) - to_int(prev[kills_idx])
            dead_gain   = to_int(row[dead_idx])  - to_int(prev[dead_idx])
            healed_gain = to_int(row[heal_idx])  - to_int(prev[heal_idx])
            helps_gain  = to_int(row[helps_idx]) - to_int(prev[helps_idx])

            # clamp negatives from corrections
            kills_gain  = max(0, kills_gain)
            dead_gain   = max(0, dead_gain)
            healed_gain = max(0, healed_gain)
            helps_gain  = max(0, helps_gain)

            dead_ratio = (dead_gain / power) * 100 if power > 0 else 0.0

            people.append({
                "lid": lid, "name": row[name_idx], "power": power,
                "merit": merit, "merit_ratio": merit_ratio,
                "dead_gain": dead_gain, "dead_ratio": dead_ratio,
                "healed_gain": healed_gain, "kills_gain": kills_gain, "helps_gain": helps_gain
            })

        if not people:
            await ctx.send(f"✅ No low performers found under {threshold:.2f}% merit ratio (S77, ≥50M).")
            return

        # --- min-max normalizers over this low-performer cohort ---
        def minmax(vals):
            lo = min(vals); hi = max(vals)
            if hi == lo:
                return lambda _x: 1.0
            span = hi - lo
            return lambda x: (x - lo) / span

        norm_merit = minmax([p["merit_ratio"]  for p in people])   # lower is worse; min-max will reflect that
        norm_dead  = minmax([p["dead_gain"]    for p in people])
        norm_heal  = minmax([p["healed_gain"]  for p in people])

        # same death ratio factor as topperformer
        def death_ratio_factor(ratio_pct: float) -> float:
            if DEAD_TARGET <= 0:
                return 1.0
            r = ratio_pct / DEAD_TARGET
            if r < 1.0:
                return r * r
            bonus = 1.0 + 0.75 * (r - 1.0)
            return min(bonus, 2.0)

        # compute composite score (then sort ASC)
        for p in people:
            merit_comp = norm_merit(p["merit_ratio"])
            dead_base  = norm_dead(p["dead_gain"])
            d_factor   = death_ratio_factor(p["dead_ratio"])
            dead_comp  = min(dead_base * d_factor, 1.0)
            heal_comp  = norm_heal(p["healed_gain"])

            score = (WEIGHT_MERIT * merit_comp) + (WEIGHT_DEAD * dead_comp) + (WEIGHT_HEAL * heal_comp)
            p["score"]    = round(score * 100, 2)
            p["d_factor"] = round(d_factor, 2)

        # worst (lowest score) first; tie-break by merit ratio ascending
        people.sort(key=lambda x: (x["score"], x["merit_ratio"]))

        header = (
            f"🥉 **Low Performers — MFD (S77)**\n"
            f"Scope: power ≥ 50M, **merit ratio < {threshold:.2f}%** • weights 40% merits / 40% deaths / 20% heals\n"
            f"Death target: **{DEAD_TARGET:.2f}%** of power (below = quadratic penalty, above = reward)\n"
            f"`{previous.title}` → `{latest.title}`\n\n"
        )

        chunks, cur = [], header
        for rank, p in enumerate(people, start=1):
            line = (
                f"**#{rank}** — **{p['name']}** (`{p['lid']}`)\n"
                f"🔢 Score: **{p['score']:.2f}**  |  🧠 Merits: {p['merit']:,} (**{p['merit_ratio']:.2f}%** of power)\n"
                f"💀 Deads: +{p['dead_gain']:,} (**{p['dead_ratio']:.2f}%** of power, factor×{p['d_factor']})  •  "
                f"❤️ Heals: +{p['healed_gain']:,}  •  ⚔️ Kills: +{p['kills_gain']:,}  •  🤝 Helps: +{p['helps_gain']:,}\n"
            )
            if len(cur) + len(line) + 1 > 2000:
                chunks.append(cur.rstrip())
                cur = ""
            cur += line + "\n"
        if cur.strip():
            chunks.append(cur.rstrip())

        for ch in chunks:
            await ctx.send(embed=discord.Embed(description=ch, color=discord.Color.red()))

    except discord.HTTPException as e:
        if getattr(e, "code", None) == 50035 or getattr(e, "status", None) == 400:
            await ctx.send("⚠️ Character limit reached — try lowering the threshold or split the query.")
        else:
            await ctx.send(f"❌ Discord error: {e}")
    except Exception as e:
        await ctx.send(f"❌ Error: {e}")
        
@bot.command()
async def topperformer(ctx, threshold: float = 12.0, season: str = DEFAULT_SEASON):
    """
    Rank MFD (S77) players by composite score:
      - Eligibility: power ≥ 50M AND merit_ratio ≥ threshold (default 12%), present in both tabs
      - Score = 40% merits + 40% deaths + 20% heals
      - Death target = 0.4% of power:
          * ratio < target  -> heavy penalty with quadratic scale
          * ratio >= target -> reward (up to +75% boost), capped
    """
    allowed_channels = {1378735765827358791, 1383515877793595435}
    if ctx.channel.id not in allowed_channels:
        await ctx.send(f"❌ Commands are only allowed in <#{1378735765827358791}>.")
        return

    try:
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season)
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Options: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = client.open(sheet_name).worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = tabs[-1]
        previous = tabs[-2]
        data_latest = latest.get_all_values()
        data_prev   = previous.get_all_values()
        headers = data_latest[0]

        def col_to_index(col):
            col = col.upper()
            idx = 0
            for ch in col:
                idx = idx * 26 + (ord(ch) - ord('A') + 1)
            return idx - 1

        id_idx    = headers.index("lord_id")
        name_idx  = 1
        serv_idx  = col_to_index("F")   # home_server
        power_idx = col_to_index("M")
        merit_idx = col_to_index("L")
        kills_idx = col_to_index("J")
        dead_idx  = col_to_index("R")
        heal_idx  = col_to_index("S")
        helps_idx = col_to_index("AE")

        def to_int(val):
            try:
                val = str(val).replace(",", "").strip()
                if val == "-" or not val:
                    return 0
                return int(val)
            except:
                return 0

        # previous rows map (only ids that exist)
        prev_map = {row[id_idx]: row for row in data_prev[1:] if len(row) > helps_idx and (row[id_idx] or "").strip()}

        MIN_POWER = 50_000_000
        DEAD_TARGET = 0.4  # percent of power
        WEIGHT_MERIT = 0.40
        WEIGHT_DEAD  = 0.40
        WEIGHT_HEAL  = 0.20

        # collect raw metrics (eligible only)
        players = []
        for row in data_latest[1:]:
            if len(row) <= helps_idx:
                continue
            lid = (row[id_idx] or "").strip()
            if not lid or lid not in prev_map:
                continue
            if (row[serv_idx] or "").strip() != "77":
                continue

            power = to_int(row[power_idx])
            if power < MIN_POWER:
                continue

            merit = to_int(row[merit_idx])
            merit_ratio = (merit / power) * 100 if power > 0 else 0.0
            if merit_ratio < threshold:
                continue

            prev = prev_map[lid]
            kills_gain  = to_int(row[kills_idx]) - to_int(prev[kills_idx])
            dead_gain   = to_int(row[dead_idx])  - to_int(prev[dead_idx])
            healed_gain = to_int(row[heal_idx])  - to_int(prev[heal_idx])
            helps_gain  = to_int(row[helps_idx]) - to_int(prev[helps_idx])

            # guard against negative corrections
            dead_gain   = max(0, dead_gain)
            healed_gain = max(0, healed_gain)
            kills_gain  = max(0, kills_gain)
            helps_gain  = max(0, helps_gain)

            dead_ratio = (dead_gain / power) * 100 if power > 0 else 0.0

            players.append({
                "lid": lid, "name": row[name_idx], "power": power,
                "merit": merit, "merit_ratio": merit_ratio,
                "dead_gain": dead_gain, "dead_ratio": dead_ratio,
                "healed_gain": healed_gain, "kills_gain": kills_gain, "helps_gain": helps_gain
            })

        if not players:
            await ctx.send(f"✅ No eligible players in server 77 at ≥{threshold:.2f}% merit ratio and ≥50M power.")
            return

        # --- min-max normalizers over the cohort ---
        def minmax(vals):
            lo = min(vals); hi = max(vals)
            if hi == lo:
                return lambda _x: 1.0  # everyone equal
            span = hi - lo
            return lambda x: (x - lo) / span

        norm_merit = minmax([p["merit_ratio"]  for p in players])
        norm_dead  = minmax([p["dead_gain"]    for p in players])
        norm_heal  = minmax([p["healed_gain"]  for p in players])

        # death ratio scale: heavy penalty below target (square), reward above (up to +75%), cap overall component to 1.0
        def death_ratio_factor(ratio_pct: float) -> float:
            if DEAD_TARGET <= 0:
                return 1.0
            r = ratio_pct / DEAD_TARGET
            if r < 1.0:
                return r * r          # quadratic penalty: 0.3 -> 0.09
            bonus = 1.0 + 0.75 * (r - 1.0)  # reward slope 0.75
            return min(bonus, 2.0)  # cap factor

        for p in players:
            m_comp = norm_merit(p["merit_ratio"])

            d_base = norm_dead(p["dead_gain"])
            d_factor = death_ratio_factor(p["dead_ratio"])
            d_comp = min(d_base * d_factor, 1.0)  # keep component in [0,1]

            h_comp = norm_heal(p["healed_gain"])

            score = (WEIGHT_MERIT * m_comp) + (WEIGHT_DEAD * d_comp) + (WEIGHT_HEAL * h_comp)
            p["score"] = round(score * 100, 2)
            p["d_factor"] = round(d_factor, 2)

        # Sort by score desc, tie-break merit ratio desc
        players.sort(key=lambda x: (x["score"], x["merit_ratio"]), reverse=True)

        header = (
            f"🏅 **Top Performers — MFD (S77)**\n"
            f"Filters: power ≥ 50M, merit ratio ≥ {threshold:.2f}% • weights 40% merits / 40% deaths / 20% heals\n"
            f"Death target: **{DEAD_TARGET:.2f}%** of power (below = quadratic penalty, above = reward)\n"
            f"`{previous.title}` → `{latest.title}`\n\n"
        )

        chunks, cur = [], header
        for rank, p in enumerate(players, start=1):
            line = (
                f"**#{rank}** — **{p['name']}** (`{p['lid']}`)\n"
                f"🔢 Score: **{p['score']:.2f}**  |  🧠 Merits: {p['merit']:,} (**{p['merit_ratio']:.2f}%** of power)\n"
                f"💀 Deads: +{p['dead_gain']:,} (**{p['dead_ratio']:.2f}%** of power, factor×{p['d_factor']})  •  "
                f"❤️ Heals: +{p['healed_gain']:,}  •  ⚔️ Kills: +{p['kills_gain']:,}  •  🤝 Helps: +{p['helps_gain']:,}\n"
            )
            if len(cur) + len(line) + 1 > 2000:
                chunks.append(cur.rstrip())
                cur = ""
            cur += line + "\n"
        if cur.strip():
            chunks.append(cur.rstrip())

        for ch in chunks:
            await ctx.send(embed=discord.Embed(description=ch, color=discord.Color.gold()))

    except discord.HTTPException as e:
        if getattr(e, "code", None) == 50035 or getattr(e, "status", None) == 400:
            await ctx.send("⚠️ Character limit reached — tighten filters (e.g., raise threshold).")
        else:
            await ctx.send(f"❌ Discord error: {e}")
    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def farms(ctx, season: str = DEFAULT_SEASON):
    try:
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season)
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Options: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = client.open(sheet_name).worksheets()
        latest = tabs[-1]
        data = latest.get_all_values()
        headers = data[0]

        def idx(col): return headers.index(col)
        id_idx = idx("lord_id")
        name_idx = 1
        power_idx = headers.index("power")

        results = []

        for row in data[1:]:
            if len(row) <= power_idx:
                continue
            try:
                power = int(row[power_idx].replace(",", "").strip()) if row[power_idx] not in ("", "-") else 0
                if 15_000_000 <= power <= 30_000_000:
                    name = row[name_idx].strip()
                    lid = row[id_idx].strip()
                    results.append((name, lid, power))
            except:
                continue

        if not results:
            await ctx.send("✅ No accounts found between 15M and 25M power.")
            return

        # Send in chunks if needed
        message = "**🌽 Accounts between 15M and 25M Power:**\n```"
        message += f"{'Name':<25} {'ID':<12} {'Power':<15}\n"
        message += f"{'-'*25} {'-'*12} {'-'*15}\n"

        for name, lid, power in results:
            message += f"{name:<25} {lid:<12} {power:<15,}\n"

        message += "```"
        await ctx.send(message)

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

from discord.ext import commands
import discord

import discord
from discord.ext import commands

@bot.command()
async def matchups2(ctx, sheet: str = "test"):
    """
    KVK pair stats from two tabs (baseline=tabs[-2], current=tabs[-1]).
    Uses ONLY accounts present in BOTH tabs by lord_id.
    Skips players who swapped server between baseline and current (configurable).
    Kills are derived from T5..T1; tier gains clamped >=0 (configurable).
    Pairs: (225 vs 176), (60 vs 249), (49 vs 363).
    """
    allowed_channels = {1378735765827358791, 1383515877793595435}
    if ctx.channel.id not in allowed_channels:
        await ctx.send("❌ Command not allowed here.")
        return

    # --- knobs ---
    SKIP_SERVER_SWAPS = True
    CLAMP_TIER_DELTAS_NONNEG = True

    try:
        sheet_name = SEASON_SHEETS.get(sheet.lower(), sheet)

        SERVER_MAP = {
            "375": "NxW", "82": "CFRA", "62": "FG", "515": "FW-Y",
            "3": "RK", "77": "MFD"
        }
        MATCHUPS = [("375", "3"), ("77", "515"), ("82", "62")]

        tabs = client.open(sheet_name).worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare. Put baseline + current.")
            return
        current, baseline = tabs[-1], tabs[-2]

        cur_vals, base_vals = current.get_all_values(), baseline.get_all_values()
        if not cur_vals or not base_vals:
            await ctx.send("❌ One of the worksheets is empty.")
            return

        headers = cur_vals[0]
        hmap = {h.strip().lower(): i for i, h in enumerate(headers)}

        def col(*aliases, required=True, fallback=None):
            for a in aliases:
                key = a.strip().lower()
                if key in hmap:
                    return hmap[key]
            if required:
                raise ValueError(f"Missing column: one of {aliases}")
            return fallback

        # EU/US-safe int parse (handles 21.734.811 / 21,734,811 / spaces / "-" / "")
        def to_int(v):
            try:
                s = str(v).replace(".", "").replace(",", "").replace(" ", "").strip()
                if s in ("", "-"): return 0
                return int(s)
            except:
                return 0

        def fmt_gain(v): return f"+{v:,}" if v > 0 else f"{v:,}"

        # Strict columns (no blind fallbacks)
        id_idx     = col("lord_id")
        server_idx = col("home_server", "server", "home server")
        dead_idx   = col("units_dead", "dead", "deads")
        heal_idx   = col("units_healed", "healed", "heals")
        gold_idx   = col("gold_spent", "gold spent")
        wood_idx   = col("wood_spent", "wood spent")
        ore_idx    = col("stone_spent", "ore_spent", "ore spent", "stone spent")
        mana_idx   = col("mana_spent", "mana spent")

        # NEW: merits (accept common aliases you use elsewhere)
        merits_idx = col(
            "merits",
            "merit",
            "merits (only 50m+ power)",
            required=True
        )

        # Tier kills (accept common aliases)
        t5_idx = col("t5_kills", "t5 kills", "t5_points", "t5 points", "killcount_t5")
        t4_idx = col("t4_kills", "t4 kills", "t4_points", "t4 points", "killcount_t4")
        t3_idx = col("t3_kills", "t3 kills", "t3_points", "t3 points", "killcount_t3")
        t2_idx = col("t2_kills", "t2 kills", "t2_points", "t2 points", "killcount_t2")
        t1_idx = col("t1_kills", "t1 kills", "t1_points", "t1 points", "killcount_t1")

        max_needed_idx = max(mana_idx, t1_idx, merits_idx)

        # Build maps and ID intersection
        base_map = {}
        for r in base_vals[1:]:
            if len(r) <= max_needed_idx: continue
            lid = (r[id_idx] or "").strip()
            if lid: base_map[lid] = r

        cur_map = {}
        for r in cur_vals[1:]:
            if len(r) <= max_needed_idx: continue
            lid = (r[id_idx] or "").strip()
            if lid: cur_map[lid] = r

        common_ids = set(base_map) & set(cur_map)

        stat_map = {sid: {
            "kills": 0, "kills_gain": 0,
            "dead": 0,  "dead_gain": 0,
            "healed": 0,"healed_gain": 0,
            "gold": 0, "wood": 0, "ore": 0, "mana": 0,
            "merits": 0, "merits_gain": 0,   # <-- ADDED
            "t5": 0, "t5_gain": 0,
            "t4": 0, "t4_gain": 0,
            "t3": 0, "t3_gain": 0,
            "t2": 0, "t2_gain": 0,
            "t1": 0, "t1_gain": 0,
        } for sid in SERVER_MAP}

        for lid in common_ids:
            r = cur_map[lid]
            b = base_map[lid]

            # normalize server to digits
            sid_now_raw = (r[server_idx] or "").strip()
            sid_now = "".join(ch for ch in sid_now_raw if ch.isdigit())

            sid_prev_raw = (b[server_idx] or "").strip()
            sid_prev = "".join(ch for ch in sid_prev_raw if ch.isdigit())

            if sid_now not in SERVER_MAP:
                continue
            if SKIP_SERVER_SWAPS and sid_now != sid_prev:
                # ignore players who moved servers between scans
                continue

            # current totals
            dead  = to_int(r[dead_idx]);   heal  = to_int(r[heal_idx])
            gold  = to_int(r[gold_idx]);   wood  = to_int(r[wood_idx])
            ore   = to_int(r[ore_idx]);    mana  = to_int(r[mana_idx])
            merits = to_int(r[merits_idx])                 # <-- ADDED
            t5 = to_int(r[t5_idx]); t4 = to_int(r[t4_idx]); t3 = to_int(r[t3_idx]); t2 = to_int(r[t2_idx]); t1 = to_int(r[t1_idx])

            # baseline totals
            dead0  = to_int(b[dead_idx]);  heal0  = to_int(b[heal_idx])
            gold0  = to_int(b[gold_idx]);  wood0  = to_int(b[wood_idx])
            ore0   = to_int(b[ore_idx]);   mana0  = to_int(b[mana_idx])
            merits0 = to_int(b[merits_idx])            # <-- ADDED
            t50 = to_int(b[t5_idx]); t40 = to_int(b[t4_idx]); t30 = to_int(b[t3_idx]); t20 = to_int(b[t2_idx]); t10 = to_int(b[t1_idx])

            # deltas
            d_dead  = dead  - dead0
            d_heal  = heal  - heal0
            d_gold  = gold  - gold0
            d_wood  = wood  - wood0
            d_ore   = ore   - ore0
            d_mana  = mana  - mana0
            d_merits = merits - merits0                 # <-- ADDED
            d_t5 = t5 - t50; d_t4 = t4 - t40; d_t3 = t3 - t30; d_t2 = t2 - t20; d_t1 = t1 - t10

            if CLAMP_TIER_DELTAS_NONNEG:
                if d_t5 < 0: d_t5 = 0
                if d_t4 < 0: d_t4 = 0
                if d_t3 < 0: d_t3 = 0
                if d_t2 < 0: d_t2 = 0
                if d_t1 < 0: d_t1 = 0
            # NOTE: we do NOT clamp merits/resources/deads/heals — those can go negative if data moved backwards.

            s = stat_map[sid_now]
            # totals
            s["dead"]   += dead
            s["healed"] += heal
            s["merits"] += merits          # <-- ADDED
            s["t5"]     += t5; s["t4"] += t4; s["t3"] += t3; s["t2"] += t2; s["t1"] += t1
            # gains
            s["dead_gain"]    += d_dead
            s["healed_gain"]  += d_heal
            s["gold"]         += d_gold
            s["wood"]         += d_wood
            s["ore"]          += d_ore
            s["mana"]         += d_mana
            s["merits_gain"]  += d_merits   # <-- ADDED
            s["t5_gain"]      += d_t5
            s["t4_gain"]      += d_t4
            s["t3_gain"]      += d_t3
            s["t2_gain"]      += d_t2
            s["t1_gain"]      += d_t1

        # derive kills from tiers
        for sid, s in stat_map.items():
            s["kills"] = s["t5"] + s["t4"] + s["t3"] + s["t2"] + s["t1"]
            s["kills_gain"] = s["t5_gain"] + s["t4_gain"] + s["t3_gain"] + s["t2_gain"] + s["t1_gain"]

        def format_side(name, stats):
            return (
                f"{name}\n"
                f"\n"
                f"▶ Combat Stats\n"
                f"⚔️ Kills:  {stats['kills']:,} ({fmt_gain(stats['kills_gain'])})\n"
                f"💀 Deads:  {stats['dead']:,} ({fmt_gain(stats['dead_gain'])})\n"
                f"❤️ Heals:  {stats['healed']:,} ({fmt_gain(stats['healed_gain'])})\n"
                f"🏅 Merits: {stats['merits']:,} ({fmt_gain(stats['merits_gain'])})\n"  # <-- ADDED
                f"\n"
                f"▶ Kill Breakdown\n"
                f"🟨 T5: {stats['t5']:,} ({fmt_gain(stats['t5_gain'])})\n"
                f"🟪 T4: {stats['t4']:,} ({fmt_gain(stats['t4_gain'])})\n"
                f"🟦 T3: {stats['t3']:,} ({fmt_gain(stats['t3_gain'])})\n"
                f"🟩 T2: {stats['t2']:,} ({fmt_gain(stats['t2_gain'])})\n"
                f"⬜ T1: {stats['t1']:,} ({fmt_gain(stats['t1_gain'])})\n"
                f"\n"
                f"▶ Resources Spent (Δ)\n"
                f"💰 Gold:  {stats['gold']:,}\n"
                f"🪵 Wood:  {stats['wood']:,}\n"
                f"⛏️ Ore:   {stats['ore']:,}\n"
                f"💧 Mana:  {stats['mana']:,}\n"
            )

        title = f"📊 War Matchups ({baseline.title} → {current.title})"
        for a, b in MATCHUPS:
            name_a = f"🔵 {SERVER_MAP[a]} (S{a})"
            name_b = f"🔴 {SERVER_MAP[b]} (S{b})"
            embed = discord.Embed(
                title=f"{title} — {SERVER_MAP[a]} vs {SERVER_MAP[b]}",
                description=f"```{name_a} vs {name_b}\n\n{format_side(name_a, stat_map[a])}\n━━━━━━━━━━━━━━\n\n{format_side(name_b, stat_map[b])}```",
                color=0x00AEEF
            )
            await ctx.send(embed=embed)

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def matchups3 (ctx, season: str = DEFAULT_SEASON):
    allowed_channels = {1378735765827358791, 1383515877793595435}
    if ctx.channel.id not in allowed_channels:
        await ctx.send("❌ Command not allowed here.")
        return

    try:
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season, season)

        tabs = client.open(sheet_name).worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = tabs[-1]
        previous = tabs[-2]
        data_latest = latest.get_all_values()
        data_prev   = previous.get_all_values()
        headers = data_latest[0]

        # strict header lookup
        hmap = {h.strip().lower(): i for i, h in enumerate(headers)}
        def col(*names):
            for n in names:
                k = n.strip().lower()
                if k in hmap: return hmap[k]
            raise ValueError(f"Missing column (tried: {names})")

        def to_int(val):
            try:
                s = str(val).replace(',', '').replace(' ', '').strip()
                if s in ("", "-"): return 0
                return int(s)
            except:
                return 0

        def fmt_gain(n): return f"+{n:,}" if n > 0 else f"{n:,}"
        def title(prev_name, latest_name): return f"📊 War Matchups ({prev_name} → {latest_name})"
        def emoji(server):
            return {"99":"🔴 ","283":"🔴 ","77":"🔴 ","110":"🔵 ","183":"🔵 ","92":"🔵 "}.get(server,"")

        SERVER_MAP = {"110":"RoG","92":"wAo","99":"BTX","283":"RFF","183":"A2G","77":"MFD"}
        matchups = [("183","99"), ("77","110"), ("92","283")]

        # indices
        id_idx     = col("lord_id")
        server_idx = col("home_server","server","home server")
        dead_idx   = col("units_dead","deads")
        heal_idx   = col("units_healed","heals")
        gold_idx   = col("gold_spent","gold spent")
        wood_idx   = col("wood_spent","wood spent")
        ore_idx    = col("stone_spent","ore_spent","ore spent","stone spent")
        mana_idx   = col("mana_spent","mana spent")

        # tiers
        t5_idx = col("killcount_t5","t5 kills")
        t4_idx = col("killcount_t4","t4 kills")
        t3_idx = col("killcount_t3","t3 kills")
        t2_idx = col("killcount_t2","t2 kills")
        t1_idx = col("killcount_t1","t1 kills")

        # build maps
        base_map = {}
        for r in data_prev[1:]:
            if len(r) <= mana_idx: continue
            lid = (r[id_idx] or "").strip()
            if lid: base_map[lid] = r

        cur_map = {}
        for r in data_latest[1:]:
            if len(r) <= mana_idx: continue
            lid = (r[id_idx] or "").strip()
            if lid: cur_map[lid] = r

        # only IDs in both AND same server both times
        common_ids = set(base_map) & set(cur_map)

        def norm_server(v):
            s = (v or "").strip()
            digits = "".join(ch for ch in s if ch.isdigit())
            return digits

        stat_map = {s: {
            "kills":0,"kills_gain":0,
            "dead":0,"dead_gain":0,
            "healed":0,"healed_gain":0,
            "gold":0,"wood":0,"ore":0,"mana":0,
            "t5":0,"t5_gain":0,"t4":0,"t4_gain":0,"t3":0,"t3_gain":0,"t2":0,"t2_gain":0,"t1":0,"t1_gain":0,
        } for s in SERVER_MAP}

        for lid in common_ids:
            prev_row = base_map[lid]
            row      = cur_map[lid]

            sid_prev = norm_server(prev_row[server_idx])
            sid_now  = norm_server(row[server_idx])

            # ignore server swaps
            if sid_prev != sid_now: 
                continue
            if sid_now not in SERVER_MAP:
                continue

            # current totals
            dead = to_int(row[dead_idx]);   heal = to_int(row[heal_idx])
            gold = to_int(row[gold_idx]);   wood = to_int(row[wood_idx])
            ore  = to_int(row[ore_idx]);    mana = to_int(row[mana_idx])
            t5 = to_int(row[t5_idx]); t4 = to_int(row[t4_idx]); t3 = to_int(row[t3_idx]); t2 = to_int(row[t2_idx]); t1 = to_int(row[t1_idx])

            # previous totals
            dead0 = to_int(prev_row[dead_idx]);   heal0 = to_int(prev_row[heal_idx])
            gold0 = to_int(prev_row[gold_idx]);   wood0 = to_int(prev_row[wood_idx])
            ore0  = to_int(prev_row[ore_idx]);    mana0 = to_int(prev_row[mana_idx])
            t50 = to_int(prev_row[t5_idx]); t40 = to_int(prev_row[t4_idx]); t30 = to_int(prev_row[t3_idx]); t20 = to_int(prev_row[t2_idx]); t10 = to_int(prev_row[t1_idx])

            s = stat_map[sid_now]
            # totals
            s["dead"] += dead; s["healed"] += heal
            s["t5"] += t5; s["t4"] += t4; s["t3"] += t3; s["t2"] += t2; s["t1"] += t1
            # deltas
            s["dead_gain"]   += (dead - dead0)
            s["healed_gain"] += (heal - heal0)
            s["gold"]        += (gold - gold0)
            s["wood"]        += (wood - wood0)
            s["ore"]         += (ore  - ore0)
            s["mana"]        += (mana - mana0)
            s["t5_gain"]     += (t5 - t50)
            s["t4_gain"]     += (t4 - t40)
            s["t3_gain"]     += (t3 - t30)
            s["t2_gain"]     += (t2 - t20)
            s["t1_gain"]     += (t1 - t10)

        # derive kills from tiers
        for sid, s in stat_map.items():
            s["kills"] = s["t5"] + s["t4"] + s["t3"] + s["t2"] + s["t1"]
            s["kills_gain"] = s["t5_gain"] + s["t4_gain"] + s["t3_gain"] + s["t2_gain"] + s["t1_gain"]

        def format_side(name, stats):
            return (
                f"{name}\n\n"
                f"▶ Combat Stats\n"
                f"⚔️ Kills:  {stats['kills']:,} ({fmt_gain(stats['kills_gain'])})\n"
                f"💀 Deads:  {stats['dead']:,} ({fmt_gain(stats['dead_gain'])})\n"
                f"❤️ Heals:  {stats['healed']:,} ({fmt_gain(stats['healed_gain'])})\n\n"
                f"▶ Kill Breakdown\n"
                f"🟥 T5: {stats['t5']:,} ({fmt_gain(stats['t5_gain'])})\n"
                f"🟦 T4: {stats['t4']:,} ({fmt_gain(stats['t4_gain'])})\n"
                f"🟩 T3: {stats['t3']:,} ({fmt_gain(stats['t3_gain'])})\n"
                f"🟨 T2: {stats['t2']:,} ({fmt_gain(stats['t2_gain'])})\n"
                f"⬜ T1: {stats['t1']:,} ({fmt_gain(stats['t1_gain'])})\n\n"
                f"▶ Resources Spent (Δ)\n"
                f"💰 Gold:  {stats['gold']:,}\n"
                f"🪵 Wood:  {stats['wood']:,}\n"
                f"⛏️ Ore:   {stats['ore']:,}\n"
                f"💧 Mana:  {stats['mana']:,}\n"
            )

        ttl = title(previous.title, latest.title)
        for a, b in matchups:
            name_a = f"{emoji(a)}{SERVER_MAP[a]}"
            name_b = f"{emoji(b)}{SERVER_MAP[b]}"
            block = (
                f"{name_a} vs {name_b}\n\n"
                f"{format_side(name_a, stat_map[a])}"
                f"\n━━━━━━━━━━━━━━\n\n"
                f"{format_side(name_b, stat_map[b])}"
            )
            await ctx.send(embed=discord.Embed(
                title=f"{ttl} — {SERVER_MAP[a]} vs {SERVER_MAP[b]}",
                description=f"```{block}```",
                color=0x00ffcc
            ))

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def matchups(ctx, season: str = DEFAULT_SEASON):
    allowed_channels = {1378735765827358791, 1383515877793595435}
    if ctx.channel.id not in allowed_channels:
        await ctx.send("❌ Command not allowed here.")
        return

    try:
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season, season)

        tabs = client.open(sheet_name).worksheets()
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
                "375": "🔴 ", "82": "🔴 ", "77": "🔴 ",
                "62": "🔵 ", "515": "🔵 ", "3": "🔵 "
            }.get(server, "")

        SERVER_MAP = {
            "375": "NxW", "82": "CFRA", "62": "FG", "515": "FW-Y",
            "3": "RK", "77": "MFD"
        }
        matchups = [("375", "3"), ("77", "515"), ("82", "62")]

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

@bot.command()
async def bastion(ctx):

    season = "sos2"
    sheet_name = SEASON_SHEETS.get(season)
    if not sheet_name:
        await ctx.send("❌ Sheet not found.")
        return

    tabs = client.open(sheet_name).worksheets()
    if len(tabs) < 2:
        await ctx.send("❌ Not enough sheets to compare.")
        return

    latest = tabs[-1]
    previous = tabs[-2]
    data_latest = latest.get_all_values()
    data_prev = previous.get_all_values()
    headers = data_latest[0]

    def col_idx(name): return headers.index(name)
    def to_int(val):
        try:
            return int(val.replace(",", "").strip()) if val not in ("", "-") else 0
        except:
            return 0

    id_idx = col_idx("lord_id")
    name_idx = 1
    alliance_idx = 3
    server_idx = col_idx("home_server")
    power_idx = col_idx("highest_power")
    dead_idx = col_idx("units_dead")

    prev_map = {row[id_idx]: row for row in data_prev[1:] if len(row) > dead_idx and row[id_idx].strip()}
    entries = []

    for row in data_latest[1:]:
        if len(row) <= max(dead_idx, server_idx):
            continue

        lid = row[id_idx].strip()
        name = row[name_idx].strip()
        server = row[server_idx].strip()
        power = to_int(row[power_idx])
        total_dead = to_int(row[dead_idx])

        if not (25_000_000 <= power <= 55_000_000):
            continue
        if server != "77":
            continue
        if lid not in prev_map:
            continue

        prev_dead = to_int(prev_map[lid][dead_idx])
        dead_gain = total_dead - prev_dead

        entries.append((name, lid, power, dead_gain, total_dead))

    # Sort by power descending
    entries.sort(key=lambda x: x[2], reverse=True)

    header = f"{'Name':<25} {'ID':<12} {'Power':<15} {'Dead Gain':<12} {'Total Dead'}\n"
    header += f"{'-'*25} {'-'*12} {'-'*15} {'-'*12} {'-'*12}"

    lines = [header]
    for name, lid, power, dead_gain, total_dead in entries:
        line = f"{name:<25} {lid:<12} {power:>15,} {dead_gain:>12,} {total_dead:>12,}"
        lines.append(line)

    await send_long_message(ctx, lines)

def format_bastion_output(entries):
    output = []
    header = f"{'Name':<25} {'ID':<12} {'Power':<15} {'Dead Gain':<12} {'Total Dead'}\n"
    header += f"{'-'*25} {'-'*12} {'-'*15} {'-'*12} {'-'*12}"
    output.append(header)
    for name, lid, power, dead_gain, total_dead in entries:
        output.append(f"{name:<25} {lid:<12} {power:>15,} {dead_gain:>12,} {total_dead:>12,}")
    return output

async def send_long_message(ctx, data_lines):
    chunk = ""
    for line in data_lines:
        if len(chunk) + len(line) + 1 > 1990:
            await ctx.send(f"```{chunk}```")
            chunk = ""
        chunk += line + "\n"
    if chunk:
        await ctx.send(f"```{chunk}```")

import os
TOKEN = os.getenv("TOKEN")

# Replace with your actual war-status channel ID
CHANNEL_ID = 1369071691111600168

# Allowed role ID
ALLOWED_ROLE_ID = 1352014667589095624

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
    print(f"✅ Bot is online as {bot.user}")

    # your existing daily digest loop
    if not scheduled_event_check.is_running():
        scheduled_event_check.start()

    # 🔥 start the auto-ping loop (the one that reads the sheet and pings)
    if not event_autoping_loop.is_running():
        event_autoping_loop.start()

    print("[events] event_autoping_loop running")


@bot.command()
@role_check()
async def warred(ctx):
    await ctx.send("✅ Command received: Setting status to 🔴 FULL WAR...\n⚠️ *Channel rename may be delayed due to Discord rate limits.*")
    channel = bot.get_channel(CHANNEL_ID)
    if channel:
        try:
            await channel.edit(name="〘🔴〙war-status-fullwar")
        except discord.errors.HTTPException as e:
            print(f"Rename failed or delayed: {e}")


@bot.command()
@role_check()
async def waryellow(ctx):
    await ctx.send("✅ Command received: Setting status to 🟡 Skirmishes...\n⚠️ *Channel rename may be delayed due to Discord rate limits.*")
    channel = bot.get_channel(CHANNEL_ID)
    if channel:
        try:
            await channel.edit(name="〘🟡〙war-status-skirmishes")
        except discord.errors.HTTPException as e:
            print(f"Rename failed or delayed: {e}")


@bot.command()
@role_check()
async def wargreen(ctx):
    await ctx.send("✅ Command received: Setting status to 🟢 No Fighting...\n⚠️ *Channel rename may be delayed due to Discord rate limits.*")
    channel = bot.get_channel(CHANNEL_ID)
    if channel:
        try:
            await channel.edit(name="〘🟢〙war-status-no-fighting")
        except discord.errors.HTTPException as e:
            print(f"Rename failed or delayed: {e}")


@bot.command()
@role_check()
async def warfarm(ctx):
    await ctx.send("🌾 Status set to *Go Farm Mana*.\nEnemies are too bad, dodged again — smh.\nGo stack that mana, RSS heal is expensive.")
    channel = bot.get_channel(CHANNEL_ID)
    if channel:
        try:
            await channel.edit(name="〘🌾〙war-status-go-farm")
        except discord.errors.HTTPException as e:
            print(f"Rename failed or delayed: {e}")

@bot.command()
async def commands(ctx):
    allowed_channel_id = 1378735765827358791  # allowed channel ID
    if ctx.channel.id != allowed_channel_id:
        await ctx.send(f"❌ Commands are only allowed in <#{allowed_channel_id}>.")
        return

    help_text = """
📜 **MFD Bot – Available Commands**

**📊 Progress & Player Stats**
- `!progress [lord_id] [season]` — Full profile: power, kills, deads, heals, RSS, mana (+gains & rank)
- `!stats [lord_id] [season]` — Quick snapshot: power, kills, heals, deads (+gain & rank)
- `!rssheal [lord_id] [season]` — Resources spent on healing (last two sheets)
- `!kills [lord_id] [season]` — Kill breakdown by troop tier
- `!mana [lord_id] [season]` — Mana gathered (+gain & rank)

**🏆 Leaderboards**
- `!topmana` — Top mana spent (delta)
- `!toprssheal` — Top RSS spent (heals/training)
- `!topheal` — Top units healed
- `!topkills` — Top kill gainers
- `!topdeads` — Top dead units
- `!topmerits [X]` — Top X by merits gain (optional season or alliance filter)
- `!lowmerits [X]` — Bottom X by merits gain (optional season or alliance filter)

**⚔️ Performance & Kick Tools**
- `!lowperformer [threshold] [season]` — Shows low performers by merit ratio & activity
- `!kickcheck` — Kick recommendation list (based on merit% and dead%)
  - Uses flex rule for high merits (≥20%) or abs merits (≥12M)
  - Hard floor: 0.20% deads required for everyone

**🆚 Matchups & Server Stats**
- `!matchups [season]` — Summary of server war stats (kills, deads, merits, RSS)
- `!matchups2 [sheet]` — test matchup format (usually top kvk)

**🗂️ Season Support**
You can append an optional season key like `sos5`, `sos2`, `hk1`, etc. to pull archived data.  
> Example: `!progress 123456 sos2`  
If no season is provided, the bot uses the current season automatically.

"""
    await ctx.send(help_text)

bot.run(TOKEN)
