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
    "sos2_2": "Call of Dragons - SoS2_2",
    "sos4": "Call of Dragons - SoS4",
    "sos3": "NxW - SoS3",
    "sos4": "NxW - SoS4",
    "z2": "NxW - SoS4 - Z2",
    "fz": "NxW - FZ",
}

DEFAULT_SEASON = "sos4"

# Now your bot setup
intents = discord.Intents.default()
intents.guilds = True
intents.messages = True
intents.reactions = True
intents.guild_reactions = True
intents.message_content = True  # Not needed for reactions, but good for commands

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
CONFIRM_CHANNEL_ID = 1369071691111600168  # ID of the channel with the message + reactions
WAR_CHANNEL_ID = 1369071691111600168  # ⬅️ replace with your war channel ID
REACTION_MESSAGE_ID = 1369072129068372008  # ⬅️ replace with your message ID
ALLOWED_COMMAND_CHANNEL_ID = 1378735765827358791

# Emoji → new channel name mapping
WAR_CHANNEL_REACTIONS = {
    "🔴": "🔴｜𝐅𝐔𝐋𝐋𝐖𝐀𝐑",
    "🟢": "🟢｜𝐍𝐎-𝐅𝐈𝐆𝐇𝐓𝐈𝐍𝐆",
    "🟡": "🟡｜𝐒𝐊𝐈𝐑𝐌𝐈𝐒𝐇𝐄𝐒",
    "🧑‍🌾": "🧑‍🌾｜𝐆𝐎-𝐅𝐀𝐑𝐌",
}

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
        
@bot.command()
async def totaldeads(ctx, *args):
    """
    Rank by TOTAL deaths (current value in Column R).
    Default: ALL players (≥25M power) in the default season.
    Add 'NVR' to filter to NVR on Server 375.

    Examples:
      !totaldeads                    -> Top 10, ALL players, default season
      !totaldeads 25                 -> Top 25, ALL players
      !totaldeads sos5               -> Top 10, ALL players, season 'sos5'
      !totaldeads sos5 30            -> Top 30, ALL players, season 'sos5'
      !totaldeads NVR 50             -> Top 50, NVR on Server 375
      !totaldeads all 50             -> Explicitly ALL, Top 50
    """

    async with ctx.typing():
        
        if ctx.channel.id != ALLOWED_COMMAND_CHANNEL_ID:
            await ctx.send(f"❌ Commands are only allowed in <#{ALLOWED_COMMAND_CHANNEL_ID}>.")
            return

    # Defaults
    top_n = 10
    season = DEFAULT_SEASON
    filter_NVR = False            # <-- default is ALL (no NVR filter)
    min_power = 25_000_000

    # Parse args flexibly
    for arg in args:
        a = str(arg).strip().lower()
        if a.isdigit():
            top_n = max(1, min(100, int(a)))
            continue
        if a in ("NVR", "NVR375"):
            filter_NVR = True
            continue
        if a in ("all", "*"):
            filter_NVR = False
            continue
        if a in SEASON_SHEETS:
            season = a
            continue
        await ctx.send(f"❌ Invalid argument '{arg}'. Seasons: {', '.join(SEASON_SHEETS.keys())} | Filters: 'NVR', 'all'.")
        return

    try:
        sheet_name = SEASON_SHEETS.get(season.lower())
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Available: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = await asyncio.to_thread(client.open(sheet_name).worksheets)
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

        def is_NVR(tag: str) -> bool:
            return bool(tag) and tag.strip().upper().startswith("NVR")

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
            if filter_NVR:
                server_val = (row[server_idx] or "").strip()
                if not is_NVR(alliance) or str(server_val) != "375":
                    continue

            dead_now = to_int(row[dead_index])
            name = (row[name_index] or "?").strip()
            full_name = f"[{alliance}] {name}"
            rows.append((full_name, dead_now))

        scope = "NVR (S375)" if filter_NVR else "All"
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
async def mana(ctx, lord_id: str, season: str = DEFAULT_SEASON):
    async with ctx.typing():
        
        if ctx.channel.id != ALLOWED_COMMAND_CHANNEL_ID:
            await ctx.send(f"❌ Commands are only allowed in <#{ALLOWED_COMMAND_CHANNEL_ID}>.")
            return
    try:
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season)
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Options: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = await asyncio.to_thread(client.open(sheet_name).worksheets)
        if len(tabs) < 2:
            await ctx.send("❌ Need at least two snapshots to calculate gain.")
            return

        # CHANGE: Compare very first sheet [0] with very last sheet [-1]
        latest_sheet, oldest_sheet = tabs[-1], tabs[0]
        data_latest = await asyncio.to_thread(latest_sheet.get_all_values)
        data_oldest = oldest_sheet.get_all_values()
        
        headers = data_latest[0]
        
        # Helper: Get index by name to avoid hardcoding errors
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

        # PERFORMANCE: Create a dictionary for the oldest data {lord_id: row_data}
        oldest_lookup = {row[id_idx].strip(): row for row in data_oldest[1:] if len(row) > mana_idx}

        # Find specific player data
        row_latest = next((r for r in data_latest[1:] if len(r) > id_idx and r[id_idx].strip() == lord_id), None)
        row_oldest = oldest_lookup.get(lord_id)

        if not row_latest or not row_oldest:
            await ctx.send("❌ Lord ID not found in both the start and end of this season.")
            return

        # Calculate gains for ALL S375 players to determine rank
        s375_gains = []
        for row in data_latest[1:]:
            l_id = row[id_idx].strip()
            # Ensure they are S375 and exist in the oldest sheet
            if len(row) > server_idx and str(row[server_idx]).strip() == "375":
                old_row = oldest_lookup.get(l_id)
                if old_row:
                    gain = to_int(row[mana_idx]) - to_int(old_row[mana_idx])
                    s375_gains.append((l_id, gain))

        # Sort for ranking
        s375_gains.sort(key=lambda x: x[1], reverse=True)
        rank = next((i+1 for i, (lid, _) in enumerate(s375_gains) if lid == lord_id), None)

        # Player specific stats
        mana_gain = to_int(row_latest[mana_idx]) - to_int(row_oldest[mana_idx])
        name = row_latest[name_idx].strip()
        alliance = row_latest[alliance_idx].strip()

# Calculate Value ($100 per 250M mana)
        # We use round() to keep it a whole number
        mana_value = round((mana_gain / 250_000_000) * 100)

        # Build Response
        embed = discord.Embed(
            title=f"🌿 Mana : {season.upper()}",
            description=f"Total gain from **{oldest_sheet.title}** to **{latest_sheet.title}**",
            color=discord.Color.blue()
        )
        embed.add_field(name="Lord", value=f"[{alliance}] {name}", inline=True)
        
        # Combined field with your specific phrasing
        embed.add_field(
            name="💧 Mana gathered", 
            value=f"Total: **{mana_gain:,}**\n*You gathered mana worth **{mana_value:,}$*** ", 
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
async def topmana(ctx, *args):
    async with ctx.typing():
        
        if ctx.channel.id != ALLOWED_COMMAND_CHANNEL_ID:
            await ctx.send(f"❌ Commands are only allowed in <#{ALLOWED_COMMAND_CHANNEL_ID}>.")
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
    async with ctx.typing():
        
        if ctx.channel.id != ALLOWED_COMMAND_CHANNEL_ID:
            await ctx.send(f"❌ Commands are only allowed in <#{ALLOWED_COMMAND_CHANNEL_ID}>.")
            return

    try:
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season)
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Available: {', '.join(SEASON_SHEETS.keys())}")
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

@bot.command()
async def topkills(ctx, top_n: int = 10, season: str = DEFAULT_SEASON):
    async with ctx.typing():
        
        if ctx.channel.id != ALLOWED_COMMAND_CHANNEL_ID:
            await ctx.send(f"❌ Commands are only allowed in <#{ALLOWED_COMMAND_CHANNEL_ID}>.")
            return

    try:
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season)
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Available: {', '.join(SEASON_SHEETS.keys())}")
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
      !lowdeads NVR 50                 -> Bottom 50 for NVR on Server 375
      !lowdeads NVR sos5 30            -> NVR+S375, season 'sos5', bottom 30
      !lowdeads all 50                 -> Remove NVR filter and show bottom 50
    """
    async with ctx.typing():
        
        if ctx.channel.id != ALLOWED_COMMAND_CHANNEL_ID:
            await ctx.send(f"❌ Commands are only allowed in <#{ALLOWED_COMMAND_CHANNEL_ID}>.")
            return

    # Defaults
    top_n = 10
    season = DEFAULT_SEASON
    filter_NVR = False       # [NVR*] AND server == 77
    MIN_POWER = 50_000_000   # >= 50M only

    # ---- Parse args (any order) ----
    for arg in args:
        a = str(arg).strip().lower()
        if a.isdigit():
            top_n = max(1, min(100, int(a)))
            continue
        if a in ("NVR", "NVR375", "NVR"):
            filter_NVR = True
            continue
        if a in ("all", "*"):
            filter_NVR = False
            continue
        if a in SEASON_SHEETS:
            season = a
            continue
        await ctx.send(
            f"❌ Invalid argument '{arg}'. Seasons: {', '.join(SEASON_SHEETS.keys())} | Filters: 'NVR', 'all'."
        )
        return

    try:
        sheet_name = SEASON_SHEETS.get(season.lower())
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Available: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = await asyncio.to_thread(client.open(sheet_name).worksheets)
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

        # Build prev map (id -> deads then)
        prev_map = {}
        for row in data_prev[1:]:
            if len(row) > max(dead_idx, id_index):
                rid = (row[id_index] or "").strip()
                if rid:
                    prev_map[rid] = to_int(row[dead_idx])

        # Collect gains for IDs present in BOTH sheets, ≥50M, optional NVR+S77
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
            # ... (Inside your data_latest loop)
            if filter_NVR:
                # We only check the server ID, ignoring the alliance tag entirely
                server_val = str(row[server_idx] or "").strip()
                
                # If the server isn't 375, skip this player
                # Note: We use "375" because sheets often store numbers as strings
                if server_val != "375":
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
            sscope = "Server 375 (All Alliances)" if filter_NVR else "All Servers"
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
        scope = "NVR (S375)" if filter_NVR else "All"
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
    Supports NVR (S375) filter. Requires power >= 50M.
    """
    async with ctx.typing():
        
        if ctx.channel.id != ALLOWED_COMMAND_CHANNEL_ID:
            await ctx.send(f"❌ Commands are only allowed in <#{ALLOWED_COMMAND_CHANNEL_ID}>.")
            return

    # Defaults
    top_n = 10
    season = DEFAULT_SEASON
    filter_NVR = False
    MIN_POWER = 50_000_000

    # Parse args
    for arg in args:
        a = str(arg).strip().lower()
        if a.isdigit():
            top_n = max(1, min(100, int(a)))
        elif a in ("NVR", "NVR375", "NVR"):
            filter_NVR = True
        elif a in ("all", "*"):
            filter_NVR = False
        elif a in SEASON_SHEETS:
            season = a
        else:
            await ctx.send(f"❌ Invalid argument '{arg}'. Seasons: {', '.join(SEASON_SHEETS.keys())} | Filters: 'NVR', 'all'.")
            return

    try:
        sheet_name = SEASON_SHEETS.get(season.lower())
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Available: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = await asyncio.to_thread(client.open(sheet_name).worksheets)
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
                
        # prev map (id -> merits then)
        prev_map = {}
        for row in data_prev[1:]:
            if len(row) > max(merits_idx, id_index):
                rid = (row[id_index] or "").strip()
                if rid:
                    prev_map[rid] = to_int(row[merits_idx])

        # gather (IDs in both, >=50M, optional NVR S375)
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
            # ... (Inside your data_latest loop)
            if filter_NVR:
                # We only check the server ID, ignoring the alliance tag entirely
                server_val = str(row[server_idx] or "").strip()
                
                # If the server isn't 375, skip this player
                # Note: We use "375" because sheets often store numbers as strings
                if server_val != "375":
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
            scope = "Server 375 (All Alliances)" if filter_NVR else "All Servers"
            await ctx.send(f"**🔻 Lowest {top_n} Merits Gained — {scope} (≥50M Power)!**\n`{previous.title}` → `{latest.title}`:\n_No eligible players found._")
            return

        # sort ascending by gain (lowest first), then name for stability
        rows.sort(key=lambda x: (x[1], x[0]))
        bottom = rows[:top_n]

        lines = [f"{i+1}. `{name}` — 🧠 +{gain:,}" for i, (name, gain) in enumerate(bottom)]

        scope = "NVR (S375)" if filter_NVR else "All"
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
async def allmana(ctx, season: str = DEFAULT_SEASON):
    """Shows the total mana gathered by the entire alliance and its dollar value."""
    async with ctx.typing():
        
        if ctx.channel.id != ALLOWED_COMMAND_CHANNEL_ID:
            await ctx.send(f"❌ Commands are only allowed in <#{ALLOWED_COMMAND_CHANNEL_ID}>.")
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

@bot.command()
async def topdeads(ctx, *args):
    """
    Usage examples:
      !topdeads                         -> Top 10 overall, default season
      !topdeads 25                     -> Top 25 overall
      !topdeads sos5                   -> Top 10 for season 'sos5'
      !topdeads sos5 25                -> Top 25 for season 'sos5'
      !topdeads NVR 50                 -> Top 50 for NVR on Server 375 (your alliance)
      !topdeads NVR sos5 30            -> NVR+S375, season 'sos5', top 30
      !topdeads all 50                 -> Explicitly remove NVR filter and show top 50
    """
    async with ctx.typing():
        
        if ctx.channel.id != ALLOWED_COMMAND_CHANNEL_ID:
            await ctx.send(f"❌ Commands are only allowed in <#{ALLOWED_COMMAND_CHANNEL_ID}>.")
            return

    # Defaults
    top_n = 10
    season = DEFAULT_SEASON
    filter_NVR = False  # toggle for [NVR*] + server 375

    # --- Parse args in any order ---
    # digits -> top_n
    # season key -> season
    # 'NVR' -> filter to NVR on server 375
    # 'all' or '*' -> remove NVR filter explicitly
    for arg in args:
        a = str(arg).strip().lower()
        if a.isdigit():
            top_n = max(1, min(100, int(a)))  # clamp a bit
            continue
        if a in ("NVR", "NVR375", "nvr"):
            filter_NVR = True
            continue
        if a in ("all", "*"):
            filter_NVR = False
            continue
        # season?
        if a in SEASON_SHEETS:
            season = a
            continue
        # Unknown token -> treat as invalid season token for clarity
        await ctx.send(f"❌ Invalid argument '{arg}'. Seasons: {', '.join(SEASON_SHEETS.keys())} | Filters: 'NVR', 'all'.")
        return

    try:
        sheet_name = SEASON_SHEETS.get(season.lower())
        if not sheet_name:
            await ctx.send(f"❌ Invalid season. Available: {', '.join(SEASON_SHEETS.keys())}")
            return

        tabs = await asyncio.to_thread(client.open(sheet_name).worksheets)
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

        # Build previous map: lord_id -> deads_then
        prev_map = {}
        for row in data_prev[1:]:
            if len(row) > dead_index and len(row) > id_index:
                raw_id = (row[id_index] or "").strip()
                if raw_id:
                    prev_map[raw_id] = to_int(row[dead_index])

        # Collect gains (only players present in both sheets, ≥25M power, optional NVR+S375 filter)
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
           # ... (Inside your data_latest loop)
            if filter_NVR:
                # We only check the server ID, ignoring the alliance tag entirely
                server_val = str(row[server_idx] or "").strip()
                
                # If the server isn't 375, skip this player
                # Note: We use "375" because sheets often store numbers as strings
                if server_val != "375":
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
            scope = "Server 375 (All Alliances)" if filter_NVR else "All Servers"
            await ctx.send(f"**🏆 Top {top_n} Dead Units Gained — {scope}**\n`{previous.title}` → `{latest.title}`:\n_No eligible players found (≥25M power and present in both sheets)._")
            return

        # Sort and slice
        results.sort(key=lambda x: x[1], reverse=True)
        top_rows = results[:top_n]

        # Build lines
        lines = [f"{i+1}. `{name}` — 💀 +{gain:,}" for i, (name, gain) in enumerate(top_rows)]

        # Header + chunked send (<=2000 chars)
        scope = "NVR (S375)" if filter_NVR else "All"
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
        
@bot.command(aliases=['stats'])
async def progress(ctx, lord_id: str, season: str = DEFAULT_SEASON):
    async with ctx.typing():
        
        if ctx.channel.id != ALLOWED_COMMAND_CHANNEL_ID:
            await ctx.send(f"❌ Commands are only allowed in <#{ALLOWED_COMMAND_CHANNEL_ID}>.")
            return
    try:
        season = season.lower()
        is_default_season = (season == DEFAULT_SEASON)
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
            await ctx.send("❌ Lord ID not found in both sheets. That's likely because you recently migrated in and don't show up in the first scan at the start of the season because of that.")
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
        """embed.add_field(
            name="• Kill Breakdown (Farlight removed this stat from scans)",
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
            name="📦 RSS Spent (Farlight removed this stat from scans)",
            value=(
                f"🪙 Gold: {gold:,}\n"
                f"🪵 Wood: {wood:,}\n"
                f"⛏️ Ore: {ore:,}\n"
                f"💧 Mana: {mana:,}\n"
                f"📦 Total: {total_rss:,}"
            ),
            inline=False
        )"""
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
                    "To view stats from the previous season, add 'sos2' or 'sos6' at the end of the command.\n"
                    "Example: !progress 123456 sos6"
                )
            )
        else:
            embed.set_footer(text=f"📅 Timespan: {previous.title} → {latest.title}")

        await ctx.send(embed=embed)

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

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

@bot.command()
async def matchups(ctx, season: str = DEFAULT_SEASON):
    async with ctx.typing():
        
        if ctx.channel.id != ALLOWED_COMMAND_CHANNEL_ID:
            await ctx.send(f"❌ Commands are only allowed in <#{ALLOWED_COMMAND_CHANNEL_ID}>.")
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
                "375": "🔴 ", "17": "🔴 ",
                "110": "🔵 ", "247": "🔵 ",
                "428": "🔴 ", "620": "🔴 ", "345": "🔵 ", "540": "🔵 " 
            }.get(server, "")

        SERVER_MAP = {
            "375": "NVR", "17": "ED", "110": "RoG", "247": "3_3",
            "620": "PGD", "428": "NM!", "345": "345V", "540": "Yaa"
        }

        # Matchups structured as tuples: (Team A tuple, Team B tuple)
        matchups = [
            (("375", "620"), ("345", "540")),          # 1v1
            (("17", "428"), ("110", "247")),           # 1v1
        ]

        # indices
        id_idx     = find_idx("lord_id",        0)
        server_idx = find_idx("home_server",    5)
        kills_idx  = find_idx("units_killed",   9)  # Column J is index 9
        merits_idx = find_idx("merits (only 50m+ power)", 11) 
        dead_idx   = find_idx("units_dead",     17)
        heal_idx   = find_idx("units_healed",   18)

        # max_needed_idx ensures we only process rows that have enough columns
        max_needed_idx = max(heal_idx, kills_idx, merits_idx)

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
            "merits": 0, "merits_gain": 0
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
            kills  = to_int(row[kills_idx])
            dead   = to_int(row[dead_idx])
            heal   = to_int(row[heal_idx])
            merits = to_int(row[merits_idx])

            # previous
            kills_prev  = to_int(prev_row[kills_idx])
            dead_prev   = to_int(prev_row[dead_idx])
            heal_prev   = to_int(prev_row[heal_idx])
            merits_prev = to_int(prev_row[merits_idx])

            s = stat_map[sid]
            
            # totals (restricted to IDs present in both)
            s["kills"]  += kills
            s["dead"]   += dead
            s["healed"] += heal
            s["merits"] += merits
            
            # deltas
            s["kills_gain"]  += (kills  - kills_prev)
            s["dead_gain"]   += (dead   - dead_prev)
            s["healed_gain"] += (heal   - heal_prev)
            s["merits_gain"] += (merits - merits_prev)

        def format_side(name, stats):
            return (
                f"{name}\n"
                f"\n"
                f"▶ Combat Stats\n"
                f"⚔️ Kills:   {stats['kills']:,} ({fmt_gain(stats['kills_gain'])})\n"
                f"💀 Deads:   {stats['dead']:,} ({fmt_gain(stats['dead_gain'])})\n"
                f"❤️ Heals:   {stats['healed']:,} ({fmt_gain(stats['healed_gain'])})\n"
                f"🏅 Merits:  {stats['merits']:,} ({fmt_gain(stats['merits_gain'])})\n"
            )

        def merge_stats(team_servers):
            merged = {
                "kills": 0, "kills_gain": 0,
                "dead": 0, "dead_gain": 0,
                "healed": 0, "healed_gain": 0,
                "merits": 0, "merits_gain": 0
            }
            for server in team_servers:
                for key in merged:
                    merged[key] += stat_map[server][key]
            return merged

        title = format_title_with_dates(previous.title, latest.title)

        for team_a, team_b in matchups:
            # Combine names and emojis for the teams
            name_a = " & ".join([f"{emoji_bracket(s)}{SERVER_MAP[s]}" for s in team_a])
            name_b = " & ".join([f"{emoji_bracket(s)}{SERVER_MAP[s]}" for s in team_b])
            
            # Merge stats for multi-server teams
            stats_a = merge_stats(team_a)
            stats_b = merge_stats(team_b)

            block = (
                f"{name_a} vs {name_b}\n\n"
                f"{format_side(name_a, stats_a)}"
                f"\n━━━━━━━━━━━━━━\n\n"
                f"{format_side(name_b, stats_b)}"
            )

            # Raw names for the embed title
            title_a = " & ".join([SERVER_MAP[s] for s in team_a])
            title_b = " & ".join([SERVER_MAP[s] for s in team_b])

            embed = discord.Embed(
                title=f"{title} — {title_a} vs {title_b}",
                description=f"```{block}```",
                color=0x00ffcc
            )
            await ctx.send(embed=embed)

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

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
    print(f"✅ Bot is online as {bot.user}")

@bot.command(aliases=['help', 'info', 'guide'])
async def commands(ctx):
    async with ctx.typing():
        
        if ctx.channel.id != ALLOWED_COMMAND_CHANNEL_ID:
            await ctx.send(f"❌ Commands are only allowed in <#{ALLOWED_COMMAND_CHANNEL_ID}>.")
            return

    help_text = """
📜 **NVR Bot – Available Commands**
You DONT need the [].

**📊 Progress & Player Stats**
- `!progress [lord_id] [season]` — Full profile: power, kills, deads, heals, mana (+gains & rank)
- `!stats [lord_id] [season]` — Quick snapshot: power, kills, heals, deads (+gain & rank)
- `!kills [lord_id] [season]` — Kill breakdown by troop tier
- `!mana [lord_id] [season]` — Mana gathered (+gain & rank)

**🏆 Leaderboards**
- `!topmana` — Top mana gathered (delta)
- `!topheal` — Top units healed
- `!topkills` — Top kill gainers
- `!topdeads` — Highest dead units
- `!lowdeads` — Lowest dead units
- `!topmerits [X]` — Top X by merits gain (optional season or alliance filter)
- `!lowmerits [X]` — Bottom X by merits gain (optional season or alliance filter)

**🆚 Matchups & Server Stats**
- `!matchups [season]` — Summary of server war stats (kills, deads, merits)

**🗂️ Season Support**
You can append an optional season key like `sos6`, `hk1`, `sos2` etc. to pull archived data.
> Example: `!progress 123456 sos6`  
If no season is provided, the bot uses the current season automatically.

"""
    await ctx.send(help_text)

bot.run(TOKEN)
