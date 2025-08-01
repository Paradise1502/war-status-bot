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
    "sos2": "Call of Dragons - SoS2",
    "sos5": "Call of Dragons - SoS5"  # 👈 add this line
}

DEFAULT_SEASON = "sos5"

# Now your bot setup
intents = discord.Intents.default()
intents.guilds = True
intents.messages = True
intents.reactions = True
intents.guild_reactions = True
intents.message_content = True  # Not needed for reactions, but good for commands

bot = commands.Bot(command_prefix="!", intents=intents)

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

    # Default values
    top_n = 10
    season = DEFAULT_SEASON

    # Parse args flexibly
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
        mana_idx = 26  # Column AA
        power_idx = 12  # Column M

        def to_int(val):
            try: return int(val.replace(',', '').replace('-', '').strip())
            except: return 0

        prev_map = {
            row[id_index]: {
                "mana": to_int(row[mana_idx])
            }
            for row in data_prev[1:]
            if len(row) > mana_idx and row[id_index]
        }

        gains = []

        for row in data_latest[1:]:
            if len(row) > max(mana_idx, power_idx):
                lord_id = row[id_index]
                alliance = row[alliance_index].strip() if len(row) > alliance_index else ""
                name = f"[{alliance}] {row[name_index].strip()}"
                if lord_id not in prev_map:
                    continue  # skip if not in previous sheet

                mana_now = to_int(row[mana_idx])
                mana_prev = prev_map[lord_id]["mana"]
                gain = mana_now - mana_prev
                power = to_int(row[power_idx])

                if power >= 25_000_000:
                    gains.append((name, gain))

        gains.sort(key=lambda x: x[1], reverse=True)
        result = "\n".join([f"{i+1}. `{name}` — 💧 +{mana:,}" for i, (name, mana) in enumerate(gains[:top_n])])

        await ctx.send(f"📊 **Top {top_n} Mana Gains** (≥25M Power)\n`{previous.title}` → `{latest.title}`:\n{result}")

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
async def toprssheal(ctx, top_n: int = 10, season: str = DEFAULT_SEASON):
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
        power_idx = 12  # Column M

        gold_idx = 31  # AF
        wood_idx = 32  # AG
        ore_idx  = 33  # AH
        mana_idx = 34  # AI

        def to_int(val):
            try: return int(val.replace(',', '').replace('-', '').strip())
            except: return 0

        # Build previous sheet map
        prev_map = {}
        for row in data_prev[1:]:
            if len(row) > mana_idx:
                raw_id = row[id_index].strip() if row[id_index] else ""
                if raw_id:
                    prev_map[raw_id] = {
                        "gold": to_int(row[gold_idx]),
                        "wood": to_int(row[wood_idx]),
                        "ore": to_int(row[ore_idx]),
                        "mana": to_int(row[mana_idx])
                    }

        gains = []
        for row in data_latest[1:]:
            if len(row) > mana_idx:
                raw_id = row[id_index].strip() if row[id_index] else ""
                if raw_id not in prev_map:
                    continue  # Skip users not in both sheets

                power = to_int(row[power_idx])
                if power < 25_000_000:
                    continue

                name = row[name_index].strip() if len(row) > name_index else "?"
                alliance = row[alliance_index].strip() if len(row) > alliance_index else ""
                full_name = f"[{alliance}] {name}"

                gold = to_int(row[gold_idx]) - prev_map[raw_id]["gold"]
                wood = to_int(row[wood_idx]) - prev_map[raw_id]["wood"]
                ore  = to_int(row[ore_idx])  - prev_map[raw_id]["ore"]
                mana = to_int(row[mana_idx]) - prev_map[raw_id]["mana"]
                total = gold + wood + ore + mana

                gains.append((full_name, total, gold, wood, ore, mana))

        gains.sort(key=lambda x: x[1], reverse=True)
        result = "\n".join([
            f"{i+1}. `{name}` — 💸 +{total:,} (🪙{gold:,} 🪵{wood:,} ⛏️{ore:,} 💧{mana:,})"
            for i, (name, total, gold, wood, ore, mana) in enumerate(gains[:top_n])
        ])

        await ctx.send(f"📊 **Top {top_n} RSS Heal Gains** (≥25M Power)\n`{previous.title}` → `{latest.title}`:\n{result}")

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
async def topdeads(ctx, top_n: int = 10, season: str = DEFAULT_SEASON):
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
        power_index = 12  # Column M
        dead_index = 17   # Column R

        def to_int(val):
            try:
                return int(val.replace(',', '').replace('-', '').strip())
            except:
                return 0

        prev_map = {}
        for row in data_prev[1:]:
            if len(row) > dead_index:
                raw_id = row[id_index].strip()
                if raw_id:
                    prev_map[raw_id] = to_int(row[dead_index])

        results = []
        for row in data_latest[1:]:
            if len(row) > dead_index:
                raw_id = row[id_index].strip()
                if raw_id not in prev_map:
                    continue

                power = to_int(row[power_index])
                if power < 25_000_000:
                    continue

                dead_now = to_int(row[dead_index])
                dead_then = prev_map[raw_id]
                gain = dead_now - dead_then

                name = row[name_index].strip()
                alliance = row[alliance_index].strip()
                full_name = f"[{alliance}] {name}"
                results.append((full_name, gain))

        results.sort(key=lambda x: x[1], reverse=True)
        output = "\n".join([f"{i+1}. `{name}` — 💀 +{gain:,}" for i, (name, gain) in enumerate(results[:top_n])])

        if not output:
            await ctx.send("No valid data found.")
        else:
            await ctx.send(f"**🏆 Top {top_n} Dead Units Gained:**\n{output}")

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
        embed.add_field(name="🟩 Power", value=f"+{power_gain:,}" + (f" (#{rank_power})" if rank_power else ""), inline=False)
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
async def lowperformer(ctx, lord_id: str, season: str = DEFAULT_SEASON):
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

        def col_to_index(col):
            col = col.upper()
            index = 0
            for char in col:
                index = index * 26 + (ord(char) - ord('A') + 1)
            return index - 1

        id_idx = headers.index("lord_id")
        name_idx = 1
        home_server_idx = col_to_index("F")
        power_idx = col_to_index("M")
        merit_idx = col_to_index("L")
        kills_idx = col_to_index("J")
        dead_idx = col_to_index("R")
        healed_idx = col_to_index("S")
        helps_idx = col_to_index("AE")

        def to_int(val):
            try:
                val = val.replace(",", "").strip()
                if val == "-" or not val:
                    return 0
                return int(val)
            except:
                return 0

        prev_map = {row[id_idx]: row for row in data_prev[1:] if len(row) > helps_idx and row[id_idx].strip()}

        raw_list = []
        for row in data_latest[1:]:
            if len(row) <= helps_idx:
                continue

            lid = row[id_idx]
            if lid not in prev_map:
                continue

            server = row[home_server_idx].strip() if len(row) > home_server_idx else ""
            if server != "77":
                continue

            power = to_int(row[power_idx])
            if power < 50_000_000:
                continue

            merit = to_int(row[merit_idx])
            merit_ratio = (merit / power) * 100 if power > 0 else 0
            if merit_ratio >= threshold:
                continue

            name = row[name_idx]
            kills_gain = to_int(row[kills_idx]) - to_int(prev_map[lid][kills_idx])
            dead_gain = to_int(row[dead_idx]) - to_int(prev_map[lid][dead_idx])
            healed_gain = to_int(row[healed_idx]) - to_int(prev_map[lid][healed_idx])
            helps_gain = to_int(row[helps_idx]) - to_int(prev_map[lid][helps_idx])

            raw_list.append({
                "lid": lid,
                "name": name,
                "merit": merit,
                "ratio": merit_ratio,
                "power": power,
                "kills": kills_gain,
                "dead": dead_gain,
                "healed": healed_gain,
                "helps": helps_gain
            })

        if not raw_list:
            await ctx.send(f"✅ No players under {threshold:.2f}% merit-to-power ratio in server 77.")
            return

        # Sort by merit ratio ascending
        raw_list.sort(key=lambda x: x["ratio"])

        header = f"📉 **Low Performers (<{threshold:.2f}% merit-to-power)**\n\n"
        chunks = []
        current_chunk = header

        for entry in raw_list:
            line = (
                f"🆔 `{entry['lid']}` | **{entry['name']}** — 🧠 {entry['merit']:,} merits ({entry['ratio']:.2f}%)\n"
                f"📊 Power: {entry['power']:,}\n"
                f"⚔️ Kills: +{entry['kills']:,} | 💀 Dead: +{entry['dead']:,} | "
                f"❤️ Healed: +{entry['healed']:,} | 🤝 Helps: +{entry['helps']:,}\n"
            )

            if len(current_chunk) + len(line) >= 2000:
                chunks.append(current_chunk)
                current_chunk = ""
            current_chunk += line + "\n"

        if current_chunk.strip():
            chunks.append(current_chunk)

        for chunk in chunks:
            if chunk.strip():
                embed = discord.Embed(description=chunk.strip(), color=discord.Color.red())
                await ctx.send(embed=embed)

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
async def matchups(ctx):
    allowed_channels = {1378735765827358791, 1383515877793595435}
    if ctx.channel.id not in allowed_channels:
        await ctx.send("❌ Command not allowed here.")
        return

    try:
        sheet_name = SEASON_SHEETS.get(DEFAULT_SEASON)
        tabs = client.open(sheet_name).worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = tabs[-1]
        previous = tabs[-2]
        data_latest = latest.get_all_values()
        data_prev = previous.get_all_values()
        headers = data_latest[0]

        def idx(name): return headers.index(name)
        def to_int(val):
            try:
                return int(val.replace(',', '').replace(' ', '').strip()) if val not in ("", "-") else 0
            except:
                return 0

        def format_title_with_dates(prev_name, latest_name):
            return f"📊 War Matchups ({prev_name} → {latest_name})"

        def emoji_bracket(server):
            return {
                "60": "🔴 ", "73": "🔴 ", "77": "🔴 ", "435": "🔴 ",
                "11": "🔵 ", "156": "🔵 ", "99": "🔵 ", "222": "🔵 "
            }.get(server, "")

        SERVER_MAP = {
            "11": "Ex-", "156": "B&R", "99": "BTX", "222": "HOUW",
            "60": "ECHO", "73": "SVR", "77": "MFD", "435": "VW"
        }

        matchups = [("60", "99"), ("77", "156"), ("435", "11"), ("73", "222")]

        stat_map = {s: {
            "kills": 0, "kills_gain": 0,
            "dead": 0, "dead_gain": 0,
            "healed": 0, "healed_gain": 0,
            "gold": 0, "wood": 0, "ore": 0, "mana": 0
        } for s in SERVER_MAP}

        id_idx = idx("lord_id")
        server_idx = idx("home_server")
        kills_idx = idx("units_killed")
        dead_idx = idx("units_dead")
        heal_idx = idx("units_healed")
        gold_idx = idx("gold_spent")
        wood_idx = idx("wood_spent")
        ore_idx = idx("stone_spent")
        mana_idx = idx("mana_spent")

        prev_map = {row[id_idx]: row for row in data_prev[1:] if len(row) > mana_idx}

        for row in data_latest[1:]:
            if len(row) <= mana_idx:
                continue
            sid = row[server_idx]
            if sid not in SERVER_MAP:
                continue

            prev_row = prev_map.get(row[id_idx])
            kills = to_int(row[kills_idx])
            dead = to_int(row[dead_idx])
            heal = to_int(row[heal_idx])
            gold = to_int(row[gold_idx])
            wood = to_int(row[wood_idx])
            ore = to_int(row[ore_idx])
            mana = to_int(row[mana_idx])

            kills_prev = to_int(prev_row[kills_idx]) if prev_row else 0
            dead_prev = to_int(prev_row[dead_idx]) if prev_row else 0
            heal_prev = to_int(prev_row[heal_idx]) if prev_row else 0
            gold_prev = to_int(prev_row[gold_idx]) if prev_row else 0
            wood_prev = to_int(prev_row[wood_idx]) if prev_row else 0
            ore_prev = to_int(prev_row[ore_idx]) if prev_row else 0
            mana_prev = to_int(prev_row[mana_idx]) if prev_row else 0

            s = stat_map[sid]
            s["kills"] += kills
            s["dead"] += dead
            s["healed"] += heal
            s["gold"] += gold - gold_prev
            s["wood"] += wood - wood_prev
            s["ore"] += ore - ore_prev
            s["mana"] += mana - mana_prev
            s["kills_gain"] += kills - kills_prev
            s["dead_gain"] += dead - dead_prev
            s["healed_gain"] += heal - heal_prev

        def format_side(name, stats):
            return (
                f"{name}\n"
                f"\n"
                f"▶ Combat Stats\n"
                f"⚔️ Kills:  {stats['kills']:,} (+{stats['kills_gain']:,})\n"
                f"💀 Deads:  {stats['dead']:,} (+{stats['dead_gain']:,})\n"
                f"❤️ Heals:  {stats['healed']:,} (+{stats['healed_gain']:,})\n"
                f"\n"
                f"▶ Resources Spent\n"
                f"💰 Gold Spent:  {stats['gold']:,}\n"
                f"🪵 Wood Spent:  {stats['wood']:,}\n"
                f"⛏️ Ore Spent:   {stats['ore']:,}\n"
                f"💧 Mana Spent:  {stats['mana']:,}\n"
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
    print(f'✅ Bot is online as {bot.user}')
    scheduled_event_check.start()


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
    channel_id = ctx.channel.id
    allowed_channel_id = 1378735765827358791  # your allowed channel ID

    if channel_id != allowed_channel_id:
        await ctx.send(f"❌ Commands are only allowed in <#{allowed_channel_id}>.")
        return

    help_text = """
📜 **Available Commands:**

**🟣 War Status**
- `!warred` — Set status to 🔴 FULL WAR
- `!waryellow` — Set status to 🟡 Skirmishes
- `!wargreen` — Set status to 🟢 No Fighting
- `!warfarm` — Set status to 🌾 Go Farm Mana

**📊 Player Stats**
- `!rssheal [lord_id] [season]` — Show RSS spent on healing between last two sheets
- `!stats [lord_id] [season]` — Show power, kills, healed, dead stats + gain + MFD rank
- `!kills [lord_id] [season]` — Show kills and troop tier breakdown
- `!mana [lord_id] [season]` — Mana gathered + gain + MFD rank
- `!progress [lord_id] [season]` — Full profile: power, kills, dead, heal, RSS, mana (+gain & rank)

**🏆 Leaderboards**
- `!topmana` — Top mana gatherers
- `!topheal` — Top units healed
- `!toprssheal` — Top RSS heal spenders
- `!topkills` — Top kill gainers
- `!topdeads` — Top dead units

**🗂️ Season Support**
You can optionally add a season tag like `sos2`, `hk1`, etc. to pull archived data.

> Example: `!progress 123456 sos2`  
If no season is given, the bot uses the current season (`sos5`).
"""
    await ctx.send(help_text)

bot.run(TOKEN)
