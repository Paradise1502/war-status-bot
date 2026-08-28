import gspread
from oauth2client.service_account import ServiceAccountCredentials
from string import ascii_uppercase
import os
import json
import discord
from discord.ext import commands
from discord.ext import tasks
from datetime import datetime, timedelta, UTC, timezone
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
    "sos2_2": "Call of Dragons - SoS2",
    "sos5": "Call of Dragons - SoS5",
    "sos6": "Call of Dragons - SoS6",
    "sos2_3": "Call of Dragons - SoS2_2",
    "sos3": "NxW - SoS3",
    "sos4": "NxW - SoS4",
    "z2": "NxW - SoS4 - Z2",
    "fz": "NxW - FZ",
    "farms": "NVR Farms",
    "sos7": "EvG - SoS7",
    "sos2": "NVR - SoS2",
}

SERVER_375_SHEET = "Call of Dragons - Server 375 Stats"

DEFAULT_SEASON = "sos2"

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
        
        if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
            # This creates a nicely formatted string of clickable channel links for the error message
            channels_mentions = ", ".join([f"<#{channel_id}>" for channel_id in ALLOWED_COMMAND_CHANNEL_ID])
            await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
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

@bot.command()
async def mana(ctx, lord_id: str, season: str = DEFAULT_SEASON):
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
        
        if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
            # This creates a nicely formatted string of clickable channel links for the error message
            channels_mentions = ", ".join([f"<#{channel_id}>" for channel_id in ALLOWED_COMMAND_CHANNEL_ID])
            await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
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
async def groupstats(ctx, season: str = DEFAULT_SEASON):
    allowed_channels = {1378735765827358791, 1383515877793595435, 1236059889411952690}
    
    if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
            # This creates a nicely formatted string of clickable channel links for the error message
            channels_mentions = ", ".join([f"<#{channel_id}>" for channel_id in ALLOWED_COMMAND_CHANNEL_ID])
            await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
            return

    TEAM_ROSTER = {
        "1038031": "Sun",    # ＬＮＶ (664.7M)
        "1209648": "Moon",   # BLIИK (584.0M)
        "2554608": "Moon",   # Joker7even (535.5M)
        "1268188": "Sun",    # DUMPER ᴺⱽᴿ (525.8M)
        "12932749": "Sun",   # ˢˢ SHY (522.0M)
        "2283588": "Moon",   # Mad Mаx (489.1M)
        "1433653": "Moon",   # Godlym ᴺᵒᵛᵃ (458.7M)
        "3668102": "Sun",    # Fluffy Waffles (427.9M)
        "11403527": "Sun",   # Hеrо (419.5M)
        "14685384": "Moon",  # SKAMERON42〤 (353.3M)
        "11288601": "Moon",  # The Hippie72 (327.2M)
        "12909862": "Sun",   # Sir ᵀᴳ (312.4M)
        "2069785": "Sun",    # Yvel (305.6M)
        "9093069": "Moon",   # M3rl1n (302.3M)
        "15165964": "Moon",  # ˢᵖʷˢ ᴀsᴜʀᴀ (301.6M)
        "12993192": "Sun",   # ˢᵖʷˢ ᴮᴸᵁᴱ (299.7M)
        "14534389": "Sun",   # sᴛᴏʀᴍ (297.8M)
        "12564527": "Moon",  # ˢᵖʷˢ S a m i (295.4M)
        "14931101": "Moon",  # ˢˢ ѕку (289.4M)
        "15309669": "Sun",   # andy andy (285.8M)
        "11498431": "Sun",   # ˢᵖʷˢ ᴄʏʙᴇʀ (284.1M)
        "15859464": "Moon",  # Stаrk (280.9M)
        "1157541": "Moon",   # Black Widow (280.8M)
        "14841316": "Sun",   # PUGZZ (279.7M)
        "3452794": "Sun",    # ˢˢ 𝖁𝖎𝖓𝖈𝖊𝖓𝖙 (274.6M)
        "4000088": "Moon",   # xL0K1x (273.7M)
        "15880004": "Moon",  # ˢˢ ᶻᵘˢ (273.6M)
        "14855893": "Sun",   # Master (273.5M)
        "15719441": "Sun",   # Shaggy 99 (269.4M)
        "15017853": "Moon",  # ˢᵖʷˢ ᴡʜɪᴛᴇ (269.2M)
        "8350805": "Moon",   # Lord (268.0M)
        "1327811": "Sun",    # ᴼᴿᴰᵀ ʜᴀx (267.8M)
        "3788189": "Sun",    # ˢˢ ᴮᵁᵀᶜᴴᴱᴿ (266.0M)
        "15416591": "Moon",  # ˢˢ ᵂᴼᴸᵛᴱᴿᴵᴺᴱ (263.1M)
        "12467111": "Moon",  # Lord (262.1M)
        "14860406": "Sun",   # ᴺⱽᴿ ᵀᴴᵁᴺᴰᴱᴿ (258.9M)
        "14986396": "Sun",   # ˢˢ ᴷᴺᴵᴳᴴᵀ (258.4M)
        "1475373": "Moon",   # ᴺⱽᴿ T0P G (256.4M)
        "1426605": "Moon",   # ˢˢ ᴬᴺᵀ (254.9M)
        "16007668": "Sun",   # ˢˢ ᶠᴬᴸᶜᴼᴺ (253.5M)
        "14920281": "Sun",   # ˢˢ ᵀᴴᴼᴿ (253.4M)
        "15168167": "Moon",  # ˢˢ ᴿᴱᴰ (253.2M)
        "15137525": "Moon",  # ˢˢ ᴴᵁᴸᴷ (252.3M)
        "15140100": "Sun",   # ˢˢ ᴮᴸᴬᶜᴷ (252.1M)
        "15138989": "Sun",   # ˢˢ ᴳᴿᴱᴱᴺ (252.0M)
        "15138403": "Moon",  # ˢˢ ᴳᴼᴸᴰ (251.8M)
        "14894521": "Moon",  # ˢˢ ᴮᴸᴬᴰᴱ (251.7M)
        "15137458": "Sun",   # ˢˢ ˢᴵᴸⱽᴱᴿ (251.7M)
        "14891433": "Sun",   # ˢˢ ᴳᴿᴱᵞ (251.7M)
        "14892554": "Moon",  # ˢˢ ᴾᵁᴿᴾᴸᴱ (251.7M)
        "14893533": "Moon",  # ˢˢ ᴮᴿᴼᵂᴺ (251.7M)
        "15406991": "Sun",   # ˢˢ ᴹᴼᴼᴺ (251.7M)
        "15872187": "Sun",   # ˢˢ ᴮᴱᴬˢᵀ (251.6M)
        "14868918": "Moon",  # ˢˢ ᵂᴬˢᴾ (251.6M)
        "14840896": "Moon",  # ˢˢ ᴼᴿᴬᴺᴳᴱ (251.5M)
        "14840981": "Sun",   # ˢˢ ᴮᴬᵀ (251.5M)
        "15039904": "Sun",   # ˢˢ ⱽᴱᴺᴼᴹ (251.4M)
        "15029841": "Moon",  # ˢˢ ᴮᴿᵁᵀᴱ (251.3M)
        "14859151": "Moon",  # ˢˢ ᴰᴬᴿᴷ (251.0M)
        "12907861": "Sun",   # ˢᵖʷˢ ᴀsᴜʀᴀ 2 (247.9M)
        "3569766": "Sun",    # ˢᵖʷˢ sʟʏᴛʜ (247.9M)
        "9076185": "Moon",   # Black (247.6M)
        "1288862": "Moon",   # JAY GAUCHO (246.7M)
        "11699043": "Sun",   # ᴹ² ᴮᴸᵁᴱ (245.5M)
        "13255722": "Sun",   # ˢᵖʷˢ ᴘsʏᴄʜ (245.4M)
        "12451416": "Moon",  # ˢᵖʷˢ ᴇsᴄᴀɴᴏʀ (244.5M)
        "11659353": "Moon",  # ˢᵖʷˢ sᴜɴɴʏ (244.4M)
        "12426797": "Sun",   # ˢᵖʷˢ ɢᴏᴋᴜ (243.6M)
        "15673802": "Sun",   # ˢᵖʷˢ ᴠᴇɢᴇᴛᴀ (242.8M)
        "12386205": "Moon",  # ˢᵖʷˢ ʙʀᴏʟʏ (242.8M)
        "12391559": "Moon",  # ˢᵖʷˢ ᴢᴇɴᴏ (242.7M)
        "12861502": "Sun",   # ˢᵖʷˢ ᴊɪʀᴇɴ (242.3M)
        "11589778": "Sun",   # ˢᵖʷˢ ᴛᴏᴘᴘᴏ (242.1M)
        "15639051": "Moon",  # ˢᵖʷˢ ʙᴇᴇʀᴜs (241.9M)
        "12913373": "Moon",  # ˢᵖʷˢ ᴡʜɪs (241.9M)
        "15203473": "Sun",   # ˢᵖʷˢ ᴄʜᴀᴍᴘᴀ (241.6M)
        "11769711": "Sun",   # ˢᵖʷˢ ᴠᴀᴅᴏs (241.5M)
        "11487055": "Moon",  # ˢᵖʷˢ ɢᴏʜᴀɴ (241.5M)
        "15238376": "Moon",  # ˢᵖʷˢ ᴘɪᴄᴄᴏʟᴏ (241.4M)
        "15253936": "Sun",   # ˢᵖʷˢ ᴋʀɪʟʟɪɴ (241.3M)
        "12239902": "Sun",   # ˢᵖʷˢ ᴛɪᴇɴ (241.3M)
        "11597010": "Moon",  # ˢᵖʷˢ ʏᴀᴍᴄʜᴀ (241.2M)
        "15592594": "Moon",  # ˢᵖʷˢ ʀᴏsʜɪ (241.1M)
        "12600393": "Sun",   # ˢᵖʷˢ ᴄʜɪᴄʜɪ (241.1M)
        "14990715": "Sun",   # ˢᵖʷˢ ʙᴜʟᴍᴀ (240.9M)
        "12581309": "Moon",  # ˢᵖʷˢ ᴠɪᴅᴇʟ (240.9M)
        "14625955": "Moon",  # ˢᵖʷˢ ᴘᴀɴ (240.8M)
        "12049853": "Sun",   # ˢᵖʷˢ ʙʀᴀ (240.7M)
        "15665516": "Sun",   # ˢᵖʷˢ ᴛʀᴜɴᴋs (240.6M)
        "12121490": "Moon",  # ˢᵖʷˢ ɢᴏᴛᴇɴ (240.6M)
        "14249731": "Moon",  # ˢᵖʷˢ ᴍᴀʀʀᴏɴ (240.5M)
        "13756181": "Sun",   # ˢᵖʷˢ ᴜʙᴜ (240.4M)
        "15808179": "Sun",   # ˢᵖʷˢ ᴅᴇɴᴅᴇ (240.3M)
        "15985931": "Moon",  # ˢᵖʷˢ ᴋᴀᴍɪ (240.2M)
        "11434627": "Moon",  # ˢᵖʷˢ ᴋᴏʀɪɴ (240.1M)
        "11516385": "Sun",   # ˢᵖʷˢ ʏᴀᴊɪ (240.0M)
        "11529501": "Sun",   # ˢᵖʷˢ ᴘᴏᴘᴏ (239.9M)
        "1652362": "Moon",   # ˢᵖʷˢ ʙᴀʙᴀ (239.9M)
        "2355170": "Moon",   # ˢᵖʷˢ ᴏᴏʟᴏɴɢ (239.8M)
        "3937721": "Sun",    # ˢᵖʷˢ ᴘᴜᴀʀ (239.8M)
        "8167052": "Sun",    # ˢᵖʷˢ ʟᴀᴜɴᴄʜ (239.7M)
        "8654500": "Moon",   # ˢᵖʷˢ ɢʏᴜ (239.6M)
        "3383792": "Moon",   # ˢᵖʷˢ ᴀʀᴀʟᴇ (239.6M)
        "6409636": "Sun",    # ˢᵖʷˢ ɢᴀᴄʜᴀɴ (239.5M)
        "3884083": "Sun",    # ˢᵖʷˢ sᴇɴʙᴇɪ (239.5M)
        "3238703": "Moon",   # ˢᵖʷˢ ᴍɪᴅᴏʀɪ (239.4M)
        "1358230": "Moon",   # ˢᵖʷˢ ᴛᴜʀʙᴏ (239.4M)
        "1191528": "Sun",    # ˢᵖʷˢ ɴɪᴋᴏ (239.3M)
        "1480794": "Sun",    # ˢᵖʷˢ ᴘᴇᴀsᴜᴋᴇ (239.3M)
        "3911741": "Moon",   # ˢᵖʷˢ ᴛᴀʀᴏ (239.2M)
        "7871135": "Moon",   # ˢᵖʷˢ ᴀᴋᴀɴᴇ (239.2M)
        "921581": "Sun",     # ˢᵖʷˢ ᴛsᴜᴋᴜɴ (239.1M)
        "12049278": "Sun",   # ᴹ² ᵂᴴᴵᵀᴱ (235.6M)
        "1186483": "Moon",   # ᴹ² ᴮᴸᴬᶜᴷ (235.5M)
        "19117667": "Moon",  # ᴹ² ᴳᴼᴸᴰ (235.4M)
        "6554196": "Sun",    # ᴹ² ˢᴵᴸⱽᴱᴿ (235.3M)
        "3446240": "Sun",    # ˢᵖʷˢ ᴮᵁᵀᶜᴴᴱᴿ 2 (228.4M)
        "10870772": "Moon",  # ᴹ² ᴮᴿᴼᵂᴺ (219.8M)
        "9900242": "Moon",   # ᴹ² ᴳᴿᴱᴱᴺ (219.7M)
        "5465713": "Sun",    # ᴹ² ᴿᴱᴰ (219.6M)
        "14697698": "Sun",   # ˢᵖʷˢ ᴇsᴄᴀɴᴏʀ 2 (198.8M)
        "4508150": "Moon",   # ˢᵖʷˢ sᴜɴɴʏ 2 (198.7M)
        "14454676": "Moon",  # ˢᵖʷˢ ɢᴏᴋᴜ 2 (198.6M)
        "8218786": "Sun",    # ˢᵖʷˢ ᴠᴇɢᴇᴛᴀ 2 (198.5M)
        "15888878": "Sun",   # ˢᵖʷˢ ʙʀᴏʟʏ 2 (198.4M)
        "15017853": "Moon",  # ˢᵖʷˢ ᴢᴇɴᴏ 2 (198.3M)
        "7741397": "Moon",   # ˢᵖʷˢ ᴊɪʀᴇɴ 2 (198.2M)
        "1288862": "Sun",    # ˢᵖʷˢ ᴛᴏᴘᴘᴏ 2 (198.1M)
        "12467111": "Sun",   # ˢᵖʷˢ ʙᴇᴇʀᴜs 2 (198.0M)
        "1129896": "Moon",   # ˢᵖʷˢ ᴡʜɪs 2 (197.9M)
        "9561066": "Moon",   # ˢᵖʷˢ ᴄʜᴀᴍᴘᴀ 2 (197.8M)
        "124604": "Sun",     # ˢᵖʷˢ ᴠᴀᴅᴏs 2 (197.7M)
        "5306715": "Sun",    # ˢᵖʷˢ ɢᴏʜᴀɴ 2 (197.6M)
        "767323": "Moon",    # ˢᵖʷˢ ᴘɪᴄᴄᴏʟᴏ 2 (197.5M)
        "14835271": "Moon",  # ˢᵖʷˢ ᴋʀɪʟʟɪɴ 2 (197.4M)
        "1696957": "Sun",    # ˢᵖʷˢ ᴛɪᴇɴ 2 (197.3M)
        "15384392": "Sun",   # ˢᵖʷˢ ʏᴀᴍᴄʜᴀ 2 (197.2M)
        "9011380": "Moon",   # ˢᵖʷˢ ʀᴏsʜɪ 2 (197.1M)
        "10403218": "Moon",  # ˢᵖʷˢ ᴄʜɪᴄʜɪ 2 (197.0M)
        "5949871": "Sun",    # ˢᵖʷˢ ʙᴜʟᴍᴀ 2 (196.9M)
        "3566602": "Sun",    # ˢᵖʷˢ ᴠɪᴅᴇʟ 2 (196.8M)
        "8347543": "Moon",   # ˢᵖʷˢ ᴘᴀɴ 2 (196.7M)
        "3221838": "Moon",   # ˢᵖʷˢ ʙʀᴀ 2 (196.6M)
        "1902775": "Sun",    # ˢᵖʷˢ ᴛʀᴜɴᴋs 2 (196.5M)
        "1292270": "Sun",    # ˢᵖʷˢ ɢᴏᴛᴇɴ 2 (196.4M)
        "9298611": "Moon",   # ˢᵖʷˢ ᴍᴀʀʀᴏɴ 2 (196.3M)
        "15240107": "Moon",  # ˢᵖʷˢ ᴜʙᴜ 2 (196.2M)
        "4942738": "Sun",    # ˢᵖʷˢ ᴅᴇɴᴅᴇ 2 (196.1M)
        "1896011": "Sun",    # ˢᵖʷˢ ᴋᴀᴍɪ 2 (196.0M)
        "3282829": "Moon",   # ˢᵖʷˢ ᴋᴏʀɪɴ 2 (195.9M)
        "4213197": "Moon",   # ˢᵖʷˢ ʏᴀᴊɪ 2 (195.8M)
        "1987541": "Sun",    # ˢᵖʷˢ ᴘᴏᴘᴏ 2 (195.7M)
        "1485262": "Sun",    # ˢᵖʷˢ ʙᴀʙᴀ 2 (195.6M)
        "37360": "Moon",     # ˢᵖʷˢ ᴏᴏʟᴏɴɢ 2 (195.5M)
        "1666865": "Moon",   # ˢᵖʷˢ ᴘᴜᴀʀ 2 (195.4M)
        "16457327": "Sun",   # ˢᵖʷˢ ʟᴀᴜɴᴄʜ 2 (195.3M)
        "5501734": "Sun",    # ˢᵖʷˢ ɢʏᴜ 2 (195.2M)
        "1771679": "Moon",   # ˢᵖʷˢ ᴀʀᴀʟᴇ 2 (195.1M)
        "4781116": "Moon",   # ˢᵖʷˢ ɢᴀᴄʜᴀɴ 2 (195.0M)
        "4475636": "Sun",    # ˢᵖʷˢ sᴇɴʙᴇɪ 2 (194.9M)
        "4188659": "Sun",    # ˢᵖʷˢ ᴍɪᴅᴏʀɪ 2 (194.8M)
        "4352097": "Moon",   # ˢᵖʷˢ ᴛᴜʀʙᴏ 2 (194.7M)
        "4188458": "Moon",   # ˢᵖʷˢ ɴɪᴋᴏ 2 (194.6M)
        "2404030": "Sun",    # ˢᵖʷˢ ᴘᴇᴀsᴜᴋᴇ 2 (194.5M)
        "2604968": "Sun",    # ˢᵖʷˢ ᴛᴀʀᴏ 2 (194.4M)
        "14322410": "Moon",  # ˢᵖʷˢ ᴀᴋᴀɴᴇ 2 (194.3M)
        "9947044": "Moon",   # ˢᵖʷˢ ᴛsᴜᴋᴜɴ 2 (194.2M)
    }
    
    try:
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season, season)
        tabs = client.open(sheet_name).worksheets()
        scan_tabs = [tab for tab in tabs if tab.title.lower() != "roster"]
        
        if len(scan_tabs) < 2:
            await ctx.send("❌ Not enough scan sheets to compare.")
            return

        latest = scan_tabs[-1]
        previous = scan_tabs[-2]
        data_latest = latest.get_all_values()
        data_prev   = previous.get_all_values()
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

        # Indices
        id_idx     = find_idx("lord_id", 0)
        name_idx   = find_idx("name", 1)
        power_idx  = find_idx("highest_power", 2)
        kills_idx  = find_idx("units_killed", 9) 
        merits_idx = find_idx("merits", 11) 
        heal_idx   = find_idx("units_healed", 18)
        dead_idx   = find_idx("units_dead", 17)

        max_needed_idx = max(heal_idx, kills_idx, merits_idx, power_idx, dead_idx)

        prev_map = {
            row[id_idx].strip(): row for row in data_prev[1:]
            if len(row) > max_needed_idx and row[id_idx].strip()
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

            p_gain = {
                "name": row[name_idx],
                "power": to_int(row[power_idx]),
                "kills": to_int(row[kills_idx]) - to_int(prev_row[kills_idx]),
                "deads": to_int(row[dead_idx]) - to_int(prev_row[dead_idx]),
                "heals": to_int(row[heal_idx]) - to_int(prev_row[heal_idx]),
                "merits": to_int(row[merits_idx]) - to_int(prev_row[merits_idx])
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
            
            # CRITICAL FIX: The spaces after the colons have been reduced. 
            # If you add too many spaces here, Discord will push the right column down again.
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
                # Force long names to be shorter to protect the layout width
                display_name = p['name'][:13] + ".." if len(p['name']) > 13 else p['name']
                
                # Removed the word " merits" at the end to save even more space!
                top_str += f"{medals[i]} **{display_name}**\n└ `{fmt(p['merits'])}` Merits\n"

            return f"{emoji} __**GROUP {name.upper()}**__", stats_block, top_str

        # Create Single Embed
        embed = discord.Embed(
            title="📊 Group Stats - Sun vs Moon",
            description=f"**Comparing:** `{previous.title}` ➔ `{latest.title}`\n" + "▬" * 15,
            color=0x2f3136 # Dark "Discord" theme color
        )

        # Sun Group Fields
        title_s, stats_s, top_s = format_group_section("Sun", "☀️", group_data["Sun"])
        embed.add_field(name=title_s, value=stats_s, inline=True)
        embed.add_field(name="⭐ TOP PERFORMERS", value=top_s, inline=True)
        
        # Spacer Field (Forces the next group to the bottom)
        embed.add_field(name="\u200b", value="▬" * 30, inline=False)

        # Moon Group Fields
        title_m, stats_m, top_m = format_group_section("Moon", "🌙", group_data["Moon"])
        embed.add_field(name=title_m, value=stats_m, inline=True)
        embed.add_field(name="⭐ TOP PERFORMERS", value=top_m, inline=True)

        embed.set_footer(text="If you read this, sun sucks.")
        
        # Ensure your datetime import matches this format
        embed.timestamp = datetime.now(UTC) 

        await ctx.send(embed=embed)

    except Exception as e:
        await ctx.send(f"❌ **Error:** {e}")

@bot.command(aliases=['grouplb', 'gl'])
async def groupleaderboard(ctx, season: str = DEFAULT_SEASON):
    allowed_channels = {1378735765827358791, 1383515877793595435, 1236059889411952690}
    
    if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
        channels_mentions = ", ".join([f"<#{channel_id}>" for channel_id in ALLOWED_COMMAND_CHANNEL_ID])
        await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
        return
        
    TEAM_ROSTER = {
        "1038031": "Sun",    # ＬＮＶ (664.7M)
        "1209648": "Moon",   # BLIИK (584.0M)
        "2554608": "Moon",   # Joker7even (535.5M)
        "1268188": "Sun",    # DUMPER ᴺⱽᴿ (525.8M)
        "12932749": "Sun",   # ˢˢ SHY (522.0M)
        "2283588": "Moon",   # Mad Mаx (489.1M)
        "1433653": "Moon",   # Godlym ᴺᵒᵛᵃ (458.7M)
        "3668102": "Sun",    # Fluffy Waffles (427.9M)
        "11403527": "Sun",   # Hеrо (419.5M)
        "14685384": "Moon",  # SKAMERON42〤 (353.3M)
        "11288601": "Moon",  # The Hippie72 (327.2M)
        "12909862": "Sun",   # Sir ᵀᴳ (312.4M)
        "2069785": "Sun",    # Yvel (305.6M)
        "9093069": "Moon",   # M3rl1n (302.3M)
        "15165964": "Moon",  # ˢᵖʷˢ ᴀsᴜʀᴀ (301.6M)
        "12993192": "Sun",   # ˢᵖʷˢ ᴮᴸᵁᴱ (299.7M)
        "14534389": "Sun",   # sᴛᴏʀᴍ (297.8M)
        "12564527": "Moon",  # ˢᵖʷˢ S a m i (295.4M)
        "14931101": "Moon",  # ˢˢ ѕку (289.4M)
        "15309669": "Sun",   # andy andy (285.8M)
        "11498431": "Sun",   # ˢᵖʷˢ ᴄʏʙᴇʀ (284.1M)
        "15859464": "Moon",  # Stаrk (280.9M)
        "1157541": "Moon",   # Black Widow (280.8M)
        "14841316": "Sun",   # PUGZZ (279.7M)
        "3452794": "Sun",    # ˢˢ 𝖁𝖎𝖓𝖈𝖊𝖓𝖙 (274.6M)
        "4000088": "Moon",   # xL0K1x (273.7M)
        "15880004": "Moon",  # ˢˢ ᶻᵘˢ (273.6M)
        "14855893": "Sun",   # Master (273.5M)
        "15719441": "Sun",   # Shaggy 99 (269.4M)
        "15017853": "Moon",  # ˢᵖʷˢ ᴡʜɪᴛᴇ (269.2M)
        "8350805": "Moon",   # Lord (268.0M)
        "1327811": "Sun",    # ᴼᴿᴰᵀ ʜᴀx (267.8M)
        "3788189": "Sun",    # ˢˢ ᴮᵁᵀᶜᴴᴱᴿ (266.0M)
        "15416591": "Moon",  # ˢˢ ᵂᴼᴸᵛᴱᴿᴵᴺᴱ (263.1M)
        "12467111": "Moon",  # Lord (262.1M)
        "14860406": "Sun",   # ᴺⱽᴿ ᵀᴴᵁᴺᴰᴱᴿ (258.9M)
        "14986396": "Sun",   # ˢˢ ᴷᴺᴵᴳᴴᵀ (258.4M)
        "1475373": "Moon",   # ᴺⱽᴿ T0P G (256.4M)
        "1426605": "Moon",   # ˢˢ ᴬᴺᵀ (254.9M)
        "16007668": "Sun",   # ˢˢ ᶠᴬᴸᶜᴼᴺ (253.5M)
        "14920281": "Sun",   # ˢˢ ᵀᴴᴼᴿ (253.4M)
        "15168167": "Moon",  # ˢˢ ᴿᴱᴰ (253.2M)
        "15137525": "Moon",  # ˢˢ ᴴᵁᴸᴷ (252.3M)
        "15140100": "Sun",   # ˢˢ ᴮᴸᴬᶜᴷ (252.1M)
        "15138989": "Sun",   # ˢˢ ᴳᴿᴱᴱᴺ (252.0M)
        "15138403": "Moon",  # ˢˢ ᴳᴼᴸᴰ (251.8M)
        "14894521": "Moon",  # ˢˢ ᴮᴸᴬᴰᴱ (251.7M)
        "15137458": "Sun",   # ˢˢ ˢᴵᴸⱽᴱᴿ (251.7M)
        "14891433": "Sun",   # ˢˢ ᴳᴿᴱᵞ (251.7M)
        "14892554": "Moon",  # ˢˢ ᴾᵁᴿᴾᴸᴱ (251.7M)
        "14893533": "Moon",  # ˢˢ ᴮᴿᴼᵂᴺ (251.7M)
        "15406991": "Sun",   # ˢˢ ᴹᴼᴼᴺ (251.7M)
        "15872187": "Sun",   # ˢˢ ᴮᴱᴬˢᵀ (251.6M)
        "14868918": "Moon",  # ˢˢ ᵂᴬˢᴾ (251.6M)
        "14840896": "Moon",  # ˢˢ ᴼᴿᴬᴺᴳᴱ (251.5M)
        "14840981": "Sun",   # ˢˢ ᴮᴬᵀ (251.5M)
        "15039904": "Sun",   # ˢˢ ⱽᴱᴺᴼᴹ (251.4M)
        "15029841": "Moon",  # ˢˢ ᴮᴿᵁᵀᴱ (251.3M)
        "14859151": "Moon",  # ˢˢ ᴰᴬᴿᴷ (251.0M)
        "12907861": "Sun",   # ˢᵖʷˢ ᴀsᴜʀᴀ 2 (247.9M)
        "3569766": "Sun",    # ˢᵖʷˢ sʟʏᴛʜ (247.9M)
        "9076185": "Moon",   # Black (247.6M)
        "1288862": "Moon",   # JAY GAUCHO (246.7M)
        "11699043": "Sun",   # ᴹ² ᴮᴸᵁᴱ (245.5M)
        "13255722": "Sun",   # ˢᵖʷˢ ᴘsʏᴄʜ (245.4M)
        "12451416": "Moon",  # ˢᵖʷˢ ᴇsᴄᴀɴᴏʀ (244.5M)
        "11659353": "Moon",  # ˢᵖʷˢ sᴜɴɴʏ (244.4M)
        "12426797": "Sun",   # ˢᵖʷˢ ɢᴏᴋᴜ (243.6M)
        "15673802": "Sun",   # ˢᵖʷˢ ᴠᴇɢᴇᴛᴀ (242.8M)
        "12386205": "Moon",  # ˢᵖʷˢ ʙʀᴏʟʏ (242.8M)
        "12391559": "Moon",  # ˢᵖʷˢ ᴢᴇɴᴏ (242.7M)
        "12861502": "Sun",   # ˢᵖʷˢ ᴊɪʀᴇɴ (242.3M)
        "11589778": "Sun",   # ˢᵖʷˢ ᴛᴏᴘᴘᴏ (242.1M)
        "15639051": "Moon",  # ˢᵖʷˢ ʙᴇᴇʀᴜs (241.9M)
        "12913373": "Moon",  # ˢᵖʷˢ ᴡʜɪs (241.9M)
        "15203473": "Sun",   # ˢᵖʷˢ ᴄʜᴀᴍᴘᴀ (241.6M)
        "11769711": "Sun",   # ˢᵖʷˢ ᴠᴀᴅᴏs (241.5M)
        "11487055": "Moon",  # ˢᵖʷˢ ɢᴏʜᴀɴ (241.5M)
        "15238376": "Moon",  # ˢᵖʷˢ ᴘɪᴄᴄᴏʟᴏ (241.4M)
        "15253936": "Sun",   # ˢᵖʷˢ ᴋʀɪʟʟɪɴ (241.3M)
        "12239902": "Sun",   # ˢᵖʷˢ ᴛɪᴇɴ (241.3M)
        "11597010": "Moon",  # ˢᵖʷˢ ʏᴀᴍᴄʜᴀ (241.2M)
        "15592594": "Moon",  # ˢᵖʷˢ ʀᴏsʜɪ (241.1M)
        "12600393": "Sun",   # ˢᵖʷˢ ᴄʜɪᴄʜɪ (241.1M)
        "14990715": "Sun",   # ˢᵖʷˢ ʙᴜʟᴍᴀ (240.9M)
        "12581309": "Moon",  # ˢᵖʷˢ ᴠɪᴅᴇʟ (240.9M)
        "14625955": "Moon",  # ˢᵖʷˢ ᴘᴀɴ (240.8M)
        "12049853": "Sun",   # ˢᵖʷˢ ʙʀᴀ (240.7M)
        "15665516": "Sun",   # ˢᵖʷˢ ᴛʀᴜɴᴋs (240.6M)
        "12121490": "Moon",  # ˢᵖʷˢ ɢᴏᴛᴇɴ (240.6M)
        "14249731": "Moon",  # ˢᵖʷˢ ᴍᴀʀʀᴏɴ (240.5M)
        "13756181": "Sun",   # ˢᵖʷˢ ᴜʙᴜ (240.4M)
        "15808179": "Sun",   # ˢᵖʷˢ ᴅᴇɴᴅᴇ (240.3M)
        "15985931": "Moon",  # ˢᵖʷˢ ᴋᴀᴍɪ (240.2M)
        "11434627": "Moon",  # ˢᵖʷˢ ᴋᴏʀɪɴ (240.1M)
        "11516385": "Sun",   # ˢᵖʷˢ ʏᴀᴊɪ (240.0M)
        "11529501": "Sun",   # ˢᵖʷˢ ᴘᴏᴘᴏ (239.9M)
        "1652362": "Moon",   # ˢᵖʷˢ ʙᴀʙᴀ (239.9M)
        "2355170": "Moon",   # ˢᵖʷˢ ᴏᴏʟᴏɴɢ (239.8M)
        "3937721": "Sun",    # ˢᵖʷˢ ᴘᴜᴀʀ (239.8M)
        "8167052": "Sun",    # ˢᵖʷˢ ʟᴀᴜɴᴄʜ (239.7M)
        "8654500": "Moon",   # ˢᵖʷˢ ɢʏᴜ (239.6M)
        "3383792": "Moon",   # ˢᵖʷˢ ᴀʀᴀʟᴇ (239.6M)
        "6409636": "Sun",    # ˢᵖʷˢ ɢᴀᴄʜᴀɴ (239.5M)
        "3884083": "Sun",    # ˢᵖʷˢ sᴇɴʙᴇɪ (239.5M)
        "3238703": "Moon",   # ˢᵖʷˢ ᴍɪᴅᴏʀɪ (239.4M)
        "1358230": "Moon",   # ˢᵖʷˢ ᴛᴜʀʙᴏ (239.4M)
        "1191528": "Sun",    # ˢᵖʷˢ ɴɪᴋᴏ (239.3M)
        "1480794": "Sun",    # ˢᵖʷˢ ᴘᴇᴀsᴜᴋᴇ (239.3M)
        "3911741": "Moon",   # ˢᵖʷˢ ᴛᴀʀᴏ (239.2M)
        "7871135": "Moon",   # ˢᵖʷˢ ᴀᴋᴀɴᴇ (239.2M)
        "921581": "Sun",     # ˢᵖʷˢ ᴛsᴜᴋᴜɴ (239.1M)
        "12049278": "Sun",   # ᴹ² ᵂᴴᴵᵀᴱ (235.6M)
        "1186483": "Moon",   # ᴹ² ᴮᴸᴬᶜᴷ (235.5M)
        "19117667": "Moon",  # ᴹ² ᴳᴼᴸᴰ (235.4M)
        "6554196": "Sun",    # ᴹ² ˢᴵᴸⱽᴱᴿ (235.3M)
        "3446240": "Sun",    # ˢᵖʷˢ ᴮᵁᵀᶜᴴᴱᴿ 2 (228.4M)
        "10870772": "Moon",  # ᴹ² ᴮᴿᴼᵂᴺ (219.8M)
        "9900242": "Moon",   # ᴹ² ᴳᴿᴱᴱᴺ (219.7M)
        "5465713": "Sun",    # ᴹ² ᴿᴱᴰ (219.6M)
        "14697698": "Sun",   # ˢᵖʷˢ ᴇsᴄᴀɴᴏʀ 2 (198.8M)
        "4508150": "Moon",   # ˢᵖʷˢ sᴜɴɴʏ 2 (198.7M)
        "14454676": "Moon",  # ˢᵖʷˢ ɢᴏᴋᴜ 2 (198.6M)
        "8218786": "Sun",    # ˢᵖʷˢ ᴠᴇɢᴇᴛᴀ 2 (198.5M)
        "15888878": "Sun",   # ˢᵖʷˢ ʙʀᴏʟʏ 2 (198.4M)
        "15017853": "Moon",  # ˢᵖʷˢ ᴢᴇɴᴏ 2 (198.3M)
        "7741397": "Moon",   # ˢᵖʷˢ ᴊɪʀᴇɴ 2 (198.2M)
        "1288862": "Sun",    # ˢᵖʷˢ ᴛᴏᴘᴘᴏ 2 (198.1M)
        "12467111": "Sun",   # ˢᵖʷˢ ʙᴇᴇʀᴜs 2 (198.0M)
        "1129896": "Moon",   # ˢᵖʷˢ ᴡʜɪs 2 (197.9M)
        "9561066": "Moon",   # ˢᵖʷˢ ᴄʜᴀᴍᴘᴀ 2 (197.8M)
        "124604": "Sun",     # ˢᵖʷˢ ᴠᴀᴅᴏs 2 (197.7M)
        "5306715": "Sun",    # ˢᵖʷˢ ɢᴏʜᴀɴ 2 (197.6M)
        "767323": "Moon",    # ˢᵖʷˢ ᴘɪᴄᴄᴏʟᴏ 2 (197.5M)
        "14835271": "Moon",  # ˢᵖʷˢ ᴋʀɪʟʟɪɴ 2 (197.4M)
        "1696957": "Sun",    # ˢᵖʷˢ ᴛɪᴇɴ 2 (197.3M)
        "15384392": "Sun",   # ˢᵖʷˢ ʏᴀᴍᴄʜᴀ 2 (197.2M)
        "9011380": "Moon",   # ˢᵖʷˢ ʀᴏsʜɪ 2 (197.1M)
        "10403218": "Moon",  # ˢᵖʷˢ ᴄʜɪᴄʜɪ 2 (197.0M)
        "5949871": "Sun",    # ˢᵖʷˢ ʙᴜʟᴍᴀ 2 (196.9M)
        "3566602": "Sun",    # ˢᵖʷˢ ᴠɪᴅᴇʟ 2 (196.8M)
        "8347543": "Moon",   # ˢᵖʷˢ ᴘᴀɴ 2 (196.7M)
        "3221838": "Moon",   # ˢᵖʷˢ ʙʀᴀ 2 (196.6M)
        "1902775": "Sun",    # ˢᵖʷˢ ᴛʀᴜɴᴋs 2 (196.5M)
        "1292270": "Sun",    # ˢᵖʷˢ ɢᴏᴛᴇɴ 2 (196.4M)
        "9298611": "Moon",   # ˢᵖʷˢ ᴍᴀʀʀᴏɴ 2 (196.3M)
        "15240107": "Moon",  # ˢᵖʷˢ ᴜʙᴜ 2 (196.2M)
        "4942738": "Sun",    # ˢᵖʷˢ ᴅᴇɴᴅᴇ 2 (196.1M)
        "1896011": "Sun",    # ˢᵖʷˢ ᴋᴀᴍɪ 2 (196.0M)
        "3282829": "Moon",   # ˢᵖʷˢ ᴋᴏʀɪɴ 2 (195.9M)
        "4213197": "Moon",   # ˢᵖʷˢ ʏᴀᴊɪ 2 (195.8M)
        "1987541": "Sun",    # ˢᵖʷˢ ᴘᴏᴘᴏ 2 (195.7M)
        "1485262": "Sun",    # ˢᵖʷˢ ʙᴀʙᴀ 2 (195.6M)
        "37360": "Moon",     # ˢᵖʷˢ ᴏᴏʟᴏɴɢ 2 (195.5M)
        "1666865": "Moon",   # ˢᵖʷˢ ᴘᴜᴀʀ 2 (195.4M)
        "16457327": "Sun",   # ˢᵖʷˢ ʟᴀᴜɴᴄʜ 2 (195.3M)
        "5501734": "Sun",    # ˢᵖʷˢ ɢʏᴜ 2 (195.2M)
        "1771679": "Moon",   # ˢᵖʷˢ ᴀʀᴀʟᴇ 2 (195.1M)
        "4781116": "Moon",   # ˢᵖʷˢ ɢᴀᴄʜᴀɴ 2 (195.0M)
        "4475636": "Sun",    # ˢᵖʷˢ sᴇɴʙᴇɪ 2 (194.9M)
        "4188659": "Sun",    # ˢᵖʷˢ ᴍɪᴅᴏʀɪ 2 (194.8M)
        "4352097": "Moon",   # ˢᵖʷˢ ᴛᴜʀʙᴏ 2 (194.7M)
        "4188458": "Moon",   # ˢᵖʷˢ ɴɪᴋᴏ 2 (194.6M)
        "2404030": "Sun",    # ˢᵖʷˢ ᴘᴇᴀsᴜᴋᴇ 2 (194.5M)
        "2604968": "Sun",    # ˢᵖʷˢ ᴛᴀʀᴏ 2 (194.4M)
        "14322410": "Moon",  # ˢᵖʷˢ ᴀᴋᴀɴᴇ 2 (194.3M)
        "9947044": "Moon",   # ˢᵖʷˢ ᴛsᴜᴋᴜɴ 2 (194.2M)
    }            
    
    try:
        # 1. FETCH SEASON DATA (For Merits & Deads Gains)
        season = season.lower()
        sheet_name = SEASON_SHEETS.get(season, season)
        
        # We need asyncio.to_thread if you are using it elsewhere to prevent blocking
        tabs = client.open(sheet_name).worksheets()
        scan_tabs = [tab for tab in tabs if tab.title.lower() != "roster"]
        
        if len(scan_tabs) < 2:
            await ctx.send("❌ Not enough scan sheets to calculate leaderboard gains.")
            return

        latest = scan_tabs[-1]
        previous = scan_tabs[-2]
        data_latest = latest.get_all_values()
        data_prev   = previous.get_all_values()
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

        id_idx     = find_idx("lord_id", 0)
        name_idx   = find_idx("name", 1)
        merits_idx = find_idx("merits", 11) 
        dead_idx   = find_idx("units_dead", 17)
        max_needed_idx = max(id_idx, name_idx, merits_idx, dead_idx)

        prev_map = {
            row[id_idx].strip(): row for row in data_prev[1:]
            if len(row) > max_needed_idx and row[id_idx].strip()
        }

        # 2. FETCH SERVER 375 DATA (For Infantry Merits)
        sheet_375 = client.open(SERVER_375_SHEET)
        data_375 = sheet_375.sheet1.get_all_values()
        headers_375 = data_375[0]
        
        id_col_375 = headers_375.index("Character ID")
        inf_col_375 = headers_375.index("Infantry Only")
        
        # Create a dictionary mapping Lord ID to their Infantry Merits
        inf_map = {}
        for r in data_375[1:]:
            if len(r) > max(id_col_375, inf_col_375):
                inf_map[str(r[id_col_375]).strip()] = to_int(r[inf_col_375])

        # 3. CALCULATE SCORES
        sun_players = []
        moon_players = []

        for row in data_latest[1:]:
            if len(row) <= max_needed_idx: continue
            
            lid = (row[id_idx] or "").strip()
            group = TEAM_ROSTER.get(lid)
            if not group: continue

            prev_row = prev_map.get(lid)
            if prev_row is None: continue

            # Gains from Season Sheet
            merits_gain = to_int(row[merits_idx]) - to_int(prev_row[merits_idx])
            deads_gain  = to_int(row[dead_idx]) - to_int(prev_row[dead_idx])
            
            # Static Total from 375 Sheet
            inf_val = inf_map.get(lid, 0)
            
            # Scoring Formula: Merits (1x) + Infantry (2x) + Deads (5x)
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

        # Sort both teams highest score to lowest
        sun_players.sort(key=lambda x: x["score"], reverse=True)
        moon_players.sort(key=lambda x: x["score"], reverse=True)

        # UI FORMATTING HELPER
        def fmt(num):
            if num >= 1_000_000_000: return f"{num / 1_000_000_000:.2f}B"
            elif num >= 1_000_000: return f"{num / 1_000_000:.2f}M"
            elif num >= 1_000: return f"{num / 1_000:.1f}K"
            return str(num)

        def build_lb_text(players_list, limit=10):
            if not players_list: return "No data available."
            medals = ["🥇", "🥈", "🥉", "4.", "5.", "6.", "7.", "8.", "9.", "10."]
            lines = []
            
            for i, p in enumerate(players_list[:limit]):
                medal = medals[i] if i < len(medals) else f"{i+1}."
                name = p['name'][:14] + ".." if len(p['name']) > 14 else p['name']
                lines.append(f"{medal} **{name}** — `{fmt(p['score'])}` pts")
                lines.append(f"> M: {fmt(p['merits'])} | Inf: {fmt(p['infantry'])} | D: {fmt(p['deads'])}")
            
            return "\n".join(lines)
        # Calculate Total Points for each team
        sun_total = sum(p["score"] for p in sun_players)
        moon_total = sum(p["score"] for p in moon_players)

        # Create the Embed
        embed = discord.Embed(
            title="🏆 Group Leaderboard - Sun vs Moon",
            description=f"**Comparing:** `{previous.title}` ➔ `{latest.title}`\n"
                        f"*Scoring: Merits (1x) | Infantry (2x) | Deads (5x)*\n" + "▬" * 15,
            color=0x2f3136
        )
        
        embed.add_field(name=f"☀️ TEAM SUN — {fmt(sun_total)} pts", value=build_lb_text(sun_players, 10), inline=True)
        embed.add_field(name=f"🌙 TEAM MOON — {fmt(moon_total)} pts", value=build_lb_text(moon_players, 10), inline=True)
        
        embed.timestamp = datetime.now(UTC) 
        await ctx.send(embed=embed)

    except Exception as e:
        await ctx.send(f"❌ **Error:** {e}")

@bot.command()
async def topheal(ctx, top_n: int = 10, season: str = DEFAULT_SEASON):
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
        
        if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
            # This creates a nicely formatted string of clickable channel links for the error message
            channels_mentions = ", ".join([f"<#{channel_id}>" for channel_id in ALLOWED_COMMAND_CHANNEL_ID])
            await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
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
        
        if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
            # This creates a nicely formatted string of clickable channel links for the error message
            channels_mentions = ", ".join([f"<#{channel_id}>" for channel_id in ALLOWED_COMMAND_CHANNEL_ID])
            await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
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
        
        if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
            # This creates a nicely formatted string of clickable channel links for the error message
            channels_mentions = ", ".join([f"<#{channel_id}>" for channel_id in ALLOWED_COMMAND_CHANNEL_ID])
            await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
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

@bot.command(aliases=['mage1v1', 'duel', 'challenge'])
async def duel_challenge(ctx, season: str = DEFAULT_SEASON):
    """Custom 1v1 Challenge: Tinzy vs Balakas"""
    if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
        channels_mentions = ", ".join([f"<#{c}>" for c in ALLOWED_COMMAND_CHANNEL_ID])
        await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
        return

    async with ctx.typing():
        try:
            # 1. FETCH SEASON DATA (For Merits Gains)
            season = season.lower()
            sheet_name = SEASON_SHEETS.get(season, season)
            
            tabs = await asyncio.to_thread(client.open(sheet_name).worksheets)
            scan_tabs = [tab for tab in tabs if tab.title.lower() != "roster"]
            
            if len(scan_tabs) < 2:
                await ctx.send("❌ Not enough scan sheets to calculate gains.")
                return

            latest = scan_tabs[-1]
            previous = scan_tabs[-2]
            data_latest = latest.get_all_values()
            data_prev   = previous.get_all_values()
            headers = data_latest[0]

            id_idx = headers.index("lord_id") if "lord_id" in headers else 0
            name_idx = 1
            merits_idx = headers.index("merits")

            prev_map = {row[id_idx].strip(): row for row in data_prev[1:] if len(row) > merits_idx}

            # 2. FETCH SERVER 375 DATA (For Infantry)
            sheet_375 = await asyncio.to_thread(client.open, SERVER_375_SHEET)
            data_375 = await asyncio.to_thread(sheet_375.sheet1.get_all_values)
            headers_375 = data_375[0]
            
            id_col_375 = headers_375.index("Character ID")
            inf_col_375 = headers_375.index("Infantry Only")
            
            inf_map = {str(r[id_col_375]).strip(): int(str(r[inf_col_375]).replace(",", "").strip() or 0) for r in data_375[1:] if len(r) > inf_col_375}

            # 3. CONTENDERS SETUP
            contenders = {
                "1240639": {"display": "Tinzy", "emoji": "🔵"},
                "12451416": {"display": "BaLaKaS", "emoji": "🔴"}
            }
            
            results = {}

            # 4. CALCULATE SCORES
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

            # 5. DETERMINE LEADER
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

            # 6. BUILD UI
            embed = discord.Embed(
                title="⚔️ THE MAGE DUEL ⚔️",
                description=f"*Rules: All Merits (1x) | Infantry Merits (2x)*\n\n{leader_text}\n" + "▬" * 15,
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
            
            embed.set_footer(text=f"Comparing: {previous.title} → {latest.title}")
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
        
        if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
            # This creates a nicely formatted string of clickable channel links for the error message
            channels_mentions = ", ".join([f"<#{channel_id}>" for channel_id in ALLOWED_COMMAND_CHANNEL_ID])
            await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
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

async def generate_375_leaderboard(ctx, stat_names, embed_title, is_top=True, limit=10):
    """Helper function to generate Top/Bottom leaderboards for Server 375 with multi-message support."""
    limit = min(max(limit, 1), 100)

    async with ctx.typing():
        if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
            channels_mentions = ", ".join([f"<#{c}>" for c in ALLOWED_COMMAND_CHANNEL_ID])
            await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
            return

        try:
            # 1. Fetch Data
            sheet_375 = await asyncio.to_thread(client.open, SERVER_375_SHEET)
            ws_375 = sheet_375.sheet1
            data_375 = await asyncio.to_thread(ws_375.get_all_values)

            headers = data_375[0]
            name_col = headers.index("Character Name")
            power_col = headers.index("Historical Highest Power")
            
            # Allow single string or list of strings for multiple columns
            if isinstance(stat_names, str):
                stat_names = [stat_names]
            
            stat_cols = [headers.index(name) for name in stat_names if name in headers]

            def to_int_local(v):
                try:
                    return int(str(v).replace(",", "").strip()) if v not in ("-", "") else 0
                except:
                    return 0

            # 2. Filter for Accounts >= 50M Power & sum the requested columns
            valid_players = []
            max_col_needed = max([power_col] + stat_cols)
            
            for row in data_375[1:]:
                if len(row) > max_col_needed:
                    power = to_int_local(row[power_col])
                    if power >= 50000000:
                        # Sum all columns provided (e.g., T4 + T5)
                        val = sum(to_int_local(row[c]) for c in stat_cols)
                        valid_players.append((row[name_col], val))

            # 3. Sort list
            valid_players.sort(key=lambda x: x[1], reverse=is_top)

            # 4. Slice requested amount
            sliced_players = valid_players[:limit]
            
            if not sliced_players:
                await ctx.send("❌ No matching players found.")
                return

            # 5. Chunk players into groups of 50 to fit inside separate messages
            chunk_size = 50
            chunks = [sliced_players[i:i + chunk_size] for i in range(0, len(sliced_players), chunk_size)]

            direction = "Top" if is_top else "Bottom"
            color = discord.Color.gold() if is_top else discord.Color.red()

            # 6. Send each chunk as its own embed message
            for index, chunk in enumerate(chunks):
                start_rank = (index * chunk_size) + 1
                end_rank = start_rank + len(chunk) - 1

                desc = ""
                for i, (p_name, p_val) in enumerate(chunk, start_rank):
                    desc += f"**{i}.** {p_name} — `{p_val:,}`\n"

                chunk_title = f"{embed_title} ({direction} {len(sliced_players)} — #{start_rank} to #{end_rank})"
                
                embed = discord.Embed(title=chunk_title, description=desc, color=color)
                
                if index == len(chunks) - 1:
                    embed.set_footer(text="Filtered for accounts ≥ 50M Highest Power")

                await ctx.send(embed=embed)

        except Exception as e:
            await ctx.send(f"❌ Error loading leaderboard: {e}")

# --- INFANTRY ---
@bot.command(aliases=['topinfantry'])
async def topinf(ctx, amount: int = 10):
    await generate_375_leaderboard(ctx, "Infantry Only", "⚔️ Infantry Merits", is_top=True, limit=amount)

@bot.command(aliases=['lowinfantry'])
async def lowinf(ctx, amount: int = 10):
    await generate_375_leaderboard(ctx, "Infantry Only", "⚔️ Infantry Merits", is_top=False, limit=amount)

# --- CAVALRY ---
@bot.command(aliases=['topcavalry'])
async def topcav(ctx, amount: int = 10):
    await generate_375_leaderboard(ctx, "Cavalry Only", "🐎 Cavalry Merits", is_top=True, limit=amount)

@bot.command(aliases=['lowcavalry'])
async def lowcav(ctx, amount: int = 10):
    await generate_375_leaderboard(ctx, "Cavalry Only", "🐎 Cavalry Merits", is_top=False, limit=amount)

# --- ARCHER ---
@bot.command(aliases=['topmarksman', 'toparchers'])
async def toparcher(ctx, amount: int = 10):
    await generate_375_leaderboard(ctx, "Marksman Only", "🏹 Archer Merits", is_top=True, limit=amount)

@bot.command(aliases=['lowmarksman', 'lowarchers'])
async def lowarcher(ctx, amount: int = 10):
    await generate_375_leaderboard(ctx, "Marksman Only", "🏹 Archer Merits", is_top=False, limit=amount)

# --- MAGE ---
@bot.command(aliases=['topmagic', 'topmages'])
async def topmage(ctx, amount: int = 10):
    await generate_375_leaderboard(ctx, "Magic Only", "🪄 Magic Merits", is_top=True, limit=amount)

@bot.command(aliases=['lowmagic', 'lowmages'])
async def lowmage(ctx, amount: int = 10):
    await generate_375_leaderboard(ctx, "Magic Only", "🪄 Magic Merits", is_top=False, limit=amount)

# --- HEALING ---
@bot.command(aliases=['toprsshealing', 'toprssheals'])
async def toprssheal(ctx, amount: int = 10):
    await generate_375_leaderboard(ctx, ["T4 Healed", "T5 Healed"], "❤️ RSS Healing", is_top=True, limit=amount)

@bot.command(aliases=['lowrsshealing', 'lowrssheals'])
async def lowrssheal(ctx, amount: int = 10):
    await generate_375_leaderboard(ctx, ["T4 Healed", "T5 Healed"], "❤️ RSS Healing", is_top=False, limit=amount)

# --- BUILD TIME ---
@bot.command(aliases=['topbuildtime'])
async def topbuild(ctx, amount: int = 10):
    await generate_375_leaderboard(ctx, "Build Time", "🔨 Build Time", is_top=True, limit=amount)

@bot.command(aliases=['lowbuildtime'])
async def lowbuild(ctx, amount: int = 10):
    await generate_375_leaderboard(ctx, "Build Time", "🔨 Build Time", is_top=False, limit=amount)

# --- DESTRUCTION TIME ---
@bot.command(aliases=['topdestruction', 'topdestruct'])
async def topdest(ctx, amount: int = 10):
    await generate_375_leaderboard(ctx, "Destruction Time", "🧨 Destruction", is_top=True, limit=amount)

@bot.command(aliases=['lowdestruction', 'lowdestruct'])
async def lowdest(ctx, amount: int = 10):
    await generate_375_leaderboard(ctx, "Destruction Time", "🧨 Destruction", is_top=False, limit=amount)

@bot.command(aliases=['stats'])
async def progress(ctx, lord_id: str, season: str = DEFAULT_SEASON):
    async with ctx.typing():
        if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
            # This creates a nicely formatted string of clickable channel links for the error message
            channels_mentions = ", ".join([f"<#{channel_id}>" for channel_id in ALLOWED_COMMAND_CHANNEL_ID])
            await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
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
        player_server = str(row_latest[home_server_idx]).strip() # Define player server for global use
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

        # New helper to rank total cumulative stats (instead of gains)
        def get_total_rank(col_index):
            totals = []
            for row in data_latest[1:]:
                if len(row) <= max(col_index, home_server_idx):
                    continue
                if str(row[home_server_idx]).strip() != player_server:
                    continue
                
                lid_current = str(row[id_idx]).strip()
                if not lid_current:
                    continue
                
                val = to_int(row[col_index])
                totals.append((lid_current, val))
                
            totals.sort(key=lambda x: x[1], reverse=True)
            for rank, (lid_curr, _) in enumerate(totals, 1):
                if lid_curr == str(lord_id):
                    return rank
            return None

        rank_total_merit = get_total_rank(merit_idx)
        
        def get_rank(col_index):
            player_row = next((r for r in data_latest[1:] if r[id_idx].strip() == lord_id), None)
            if not player_row or len(player_row) <= home_server_idx:
                return None

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

        rank_power = get_total_rank(power_idx)
        rank_kills = get_rank(kills_idx)
        rank_dead = get_rank(dead_idx)
        rank_healed = get_rank(healed_idx)
        rank_merit = get_rank(merit_idx)
        rank_mana_gathered = get_rank(mana_gathered_idx)

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
        
        embed.add_field(name="🟩 Highest Power", value=f"{power_latest:,} (+{power_gain:,})" + (f" `(#{rank_power})`" if rank_power else ""), inline=False)
        
        # Row 2 (3 Items)
        embed.add_field(name="🧠 Total Merits", value=f"{merit_latest:,}" + (f" `(#{rank_total_merit})`" if rank_total_merit else ""), inline=True)
        embed.add_field(name="📊 Merit Ratio", value=f"{merit_ratio:.2f}%" + (f" `(#{rank_merit_ratio})`" if rank_merit_ratio else ""), inline=True)
        embed.add_field(name="💧 Mana Gathered", value=f"**+{mana_gathered:,}**" + (f" `(#{rank_mana_gathered})`" if rank_mana_gathered and mana_gathered > 0 else ""), inline=True)

        embed.add_field(name="⚔️ Kills", value=f"+{kills_gain:,}" + (f" `(#{rank_kills})`" if rank_kills else ""), inline=True)
        embed.add_field(name="💀 Deads", value=f"+{dead_gain:,}" + (f" `(#{rank_dead})`" if rank_dead else ""), inline=True)
        embed.add_field(name="❤️ Healed", value=f"+{healed_gain:,}" + (f" `(#{rank_healed})`" if rank_healed else ""), inline=True)
       
       # -------------------------------------------------------------
        # NEW SECTION: Server 375 Exclusive Stats Check (Google Sheets)
        # -------------------------------------------------------------
        if player_server == "375":
            try:
                sheet_375 = await asyncio.to_thread(client.open, SERVER_375_SHEET)
                ws_375 = sheet_375.sheet1
                data_375 = await asyncio.to_thread(ws_375.get_all_values)
                
                headers_375 = data_375[0]
                
                id_col = headers_375.index("Character ID")
                hist_power_col = headers_375.index("Historical Highest Power")
                inf_col = headers_375.index("Infantry Only")
                cav_col = headers_375.index("Cavalry Only")
                arch_col = headers_375.index("Marksman Only")
                magic_col = headers_375.index("Magic Only")
                total_merit_col = headers_375.index("Total Merits")
                enemy_merit_col = headers_375.index("Enemy Merits")
                
                # Split healing columns
                t4_heal_col = headers_375.index("T4 Healed")
                t5_heal_col = headers_375.index("T5 Healed")
                heal_cols = [t4_heal_col, t5_heal_col]
                
                build_col = headers_375.index("Build Time")
                dest_col = headers_375.index("Destruction Time")

                server_375_data = []
                player_row_375 = None
                
                max_needed_375 = max(id_col, hist_power_col, inf_col, cav_col, arch_col, magic_col, t4_heal_col, t5_heal_col, build_col, dest_col)

                for r in data_375[1:]:
                    if len(r) > max_needed_375:
                        r_id = str(r[id_col]).strip()
                        if to_int(r[hist_power_col]) >= 50000000:
                            server_375_data.append(r)
                        
                        if r_id == str(lord_id):
                            player_row_375 = r
                            if to_int(r[hist_power_col]) < 50000000:
                                server_375_data.append(r)
                
                def get_375_rank(col_indices, operation="sum"):
                    if isinstance(col_indices, int):
                        col_indices = [col_indices]
                    
                    if operation == "subtract" and len(col_indices) == 2:
                        sorted_members = sorted(server_375_data, key=lambda x: to_int(x[col_indices[0]]) - to_int(x[col_indices[1]]), reverse=True)
                    else:
                        sorted_members = sorted(server_375_data, key=lambda x: sum(to_int(x[c]) for c in col_indices), reverse=True)
                        
                    for rank, row in enumerate(sorted_members, 1):
                        if str(row[id_col]).strip() == str(lord_id):
                            return rank
                    return None

                if player_row_375:
                    inf_val = to_int(player_row_375[inf_col])
                    cav_val = to_int(player_row_375[cav_col])
                    arch_val = to_int(player_row_375[arch_col])
                    magic_val = to_int(player_row_375[magic_col])
                    total_merits_lifetime = to_int(player_row_375[total_merit_col])
                    enemy_merits_lifetime = to_int(player_row_375[enemy_merit_col])
                    traded_merits_lifetime = max(0, total_merits_lifetime - enemy_merits_lifetime)

                    pvp_ratio = (enemy_merits_lifetime / total_merits_lifetime * 100) if total_merits_lifetime > 0 else 0
                    
                    # Extract T4 and T5 separately instead of adding them
                    t4_heal_val = to_int(player_row_375[t4_heal_col])
                    t5_heal_val = to_int(player_row_375[t5_heal_col])
                    
                    build_val = to_int(player_row_375[build_col])
                    dest_val = to_int(player_row_375[dest_col])

                    embed.add_field(
                        name="Troop Merits (Server Rank)",
                        value=(
                            f"⚔️ **Infantry:** {inf_val:,} `(#{get_375_rank(inf_col)})`\n"
                            f"🐎 **Cavalry:** {cav_val:,} `(#{get_375_rank(cav_col)})`\n"
                            f"🏹 **Archer:** {arch_val:,} `(#{get_375_rank(arch_col)})`\n"
                            f"🪄 **Magic:** {magic_val:,} `(#{get_375_rank(magic_col)})`"
                        ),
                        inline=True
                    )

                    embed.add_field(
                        name="Utility (Server Rank)",
                        value=(
                            f"❤️ **T4 RSS Healing:** {t4_heal_val:,} `(#{get_375_rank(t4_heal_col)})`\n"
                            f"❤️ **T5 RSS Healing:** {t5_heal_val:,} `(#{get_375_rank(t5_heal_col)})`\n"
                            f"🔨 **Build Time:** {build_val:,} `(#{get_375_rank(build_col)})`\n"
                            f"🔨 **Destruction:** {dest_val:,} `(#{get_375_rank(dest_col)})`"
                        ),
                        inline=True
                    )

                    embed.add_field(
                        name="Combat Breakdown (Server Stats)",
                        value=(
                            f"🎯 **Enemy (Real) Merits:** {enemy_merits_lifetime:,} `(#{get_375_rank(enemy_merit_col)})`\n"
                            f"🤝 **Traded Merits:** {traded_merits_lifetime:,} `(#{get_375_rank([total_merit_col, enemy_merit_col], operation='subtract')})`\n"
                        ),
                        inline=False
                    )
            
            except Exception as ex:
                print(f"Failed to load Server 375 stats for {lord_id}: {ex}")
        # -------------------------------------------------------------
        # -------------------------------------------------------------

        if is_default_season:
            embed.set_footer(
                text=(
                    f"📅 Timespan: {previous.title} → {latest.title}\n"
                    "⚠️ Stats may vary slightly from in-game counters due to scan timing.\n"
                    "🔍 View past seasons by appending a season code (e.g., !progress 123456 sos6)."
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
        
        if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
            # This creates a nicely formatted string of clickable channel links for the error message
            channels_mentions = ", ".join([f"<#{channel_id}>" for channel_id in ALLOWED_COMMAND_CHANNEL_ID])
            await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
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
    await bot.load_extension("spydetect")
    await bot.load_extension("dashboard")
    print(f"✅ Bot is online as {bot.user}")

    # Start the UTC channel updater loop
    if not update_utc_channels.is_running():
        update_utc_channels.start()
    
@bot.command(aliases=['help', 'info', 'guide'])
async def commands(ctx):
    async with ctx.typing():
        
        if ctx.channel.id not in ALLOWED_COMMAND_CHANNEL_ID:
            # This creates a nicely formatted string of clickable channel links for the error message
            channels_mentions = ", ".join([f"<#{channel_id}>" for channel_id in ALLOWED_COMMAND_CHANNEL_ID])
            await ctx.send(f"❌ Commands are only allowed in {channels_mentions}.")
            return

        help_text = """
📜 **NVR Bot – Available Commands**
You DONT need the [].

**📊 Progress & Player Stats**
- `!progress [lord_id] [season]` — Full profile: power, kills, deads, heals, mana (+gains & rank)
- `!stats [lord_id] [season]` — Quick snapshot: power, kills, heals, deads (+gain & rank)
- `!kills [lord_id] [season]` — Kill breakdown by troop tier
- `!mana [lord_id] [season]` — Mana gathered (+gain & rank)

**🏆 Leaderboards (Main Season)**
- `!topmana` — Top mana gathered (delta)
- `!topheal` — Top units healed
- `!topkills` — Top kill gainers
- `!topdeads` — Highest dead units
- `!lowdeads` — Lowest dead units
- `!topmerits [X]` — Top X by merits gain (optional season or alliance filter)
- `!lowmerits [X]` — Bottom X by merits gain (optional season or alliance filter)

**👑 Server 375 Leaderboards (≥ 50M Power)**
*Optional limit `[amount]` up to 100 (default: 10). Example: `!topinf 50`*
- `!topinf [N]` / `!lowinf [N]` — Infantry Merits
- `!topcav [N]` / `!lowcav [N]` — Cavalry Merits
- `!toparcher [N]` / `!lowarcher [N]` — Archer Merits
- `!topmage [N]` / `!lowmage [N]` — Magic Merits
- `!toprssheal [N]` / `!lowrssheal [N]` — RSS Healing
- `!topbuild [N]` / `!lowbuild [N]` — Build Time
- `!topdest [N]` / `!lowdest [N]` — Destruction Time

**🆚 Matchups & Server Stats**
- `!matchups [season]` — Summary of server war stats (kills, deads, merits)

**🗂️ Season Support**
You can append an optional season key like `sos4` or `sos2` etc. to pull archived data.
> Example: `!progress 123456 sos4`  
If no season is provided, the bot uses the current season automatically.
"""
        await ctx.send(help_text)

bot.run(TOKEN)
