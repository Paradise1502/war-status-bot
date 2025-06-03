import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import discord
from discord.ext import commands

# Google Sheets Auth
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_json = os.getenv("CREDENTIALS_JSON")
creds_dict = json.loads(creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# Now your bot setup
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

@bot.command()
async def rssheal(ctx, lord_id: str):
    try:
        # Get the two latest sheet tabs
        tabs = client.open("Copy SoS5").worksheets()  # Replace with your sheet name if needed
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

@bot.command()
async def stats(ctx, lord_id: str):
    try:
        # Get the two most recent tabs
        tabs = client.open("Copy SoS5").worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = tabs[-1]
        previous = tabs[-2]

        latest_data = latest.get_all_values()
        previous_data = previous.get_all_values()

        headers = latest_data[0]
        id_index = headers.index("lord_id")
        name_index = 1  # Column B

        # Column indices
        power_idx = 12   # M
        killed_idx = 9   # J
        dead_idx = 17    # R
        healed_idx = 18  # S

        def find_row(data):
            for row in data[1:]:
                if row[id_index] == lord_id:
                    return row
            return None

        row_latest = find_row(latest_data)
        row_prev = find_row(previous_data)

        if not row_latest or not row_prev:
            await ctx.send("❌ Lord ID not found in both sheets.")
            return

        username = row_latest[name_index]

        def to_int(val): return int(val.replace(',', '')) if val else 0

        def diff(idx):
            return to_int(row_latest[idx]) - to_int(row_prev[idx]), to_int(row_latest[idx])

        power_gain, power_total = diff(power_idx)
        killed_gain, killed_total = diff(killed_idx)
        dead_gain, dead_total = diff(dead_idx)
        healed_gain, healed_total = diff(healed_idx)

        await ctx.send(
            f"📊 Stats for `{username}` (`{lord_id}`): `{previous.title}` → `{latest.title}`\n"
            f"🏆 Power: {power_total:,} (+{power_gain:,})\n"
            f"⚔️ Kills: {killed_total:,} (+{killed_gain:,})\n"
            f"☠️ Dead: {dead_total:,} (+{dead_gain:,})\n"
            f"❤️‍🩹 Healed: {healed_total:,} (+{healed_gain:,})"
        )

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def mana(ctx, lord_id: str):
    try:
        tabs = client.open("Copy SoS5").worksheets()
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = tabs[-1]
        previous = tabs[-2]

        latest_data = latest.get_all_values()
        previous_data = previous.get_all_values()

        headers = latest_data[0]
        id_index = headers.index("lord_id")
        name_index = 1  # Column B
        mana_idx = 26   # Column AA

        def find_row(data):
            for row in data[1:]:
                if row[id_index] == lord_id:
                    return row
            return None

        row_latest = find_row(latest_data)
        row_prev = find_row(previous_data)

        if not row_latest or not row_prev:
            await ctx.send("❌ Lord ID not found in both sheets.")
            return

        username = row_latest[name_index]

        def to_int(val): return int(val.replace(',', '')) if val else 0

        mana_latest = to_int(row_latest[mana_idx])
        mana_prev = to_int(row_prev[mana_idx])
        mana_gain = mana_latest - mana_prev

        await ctx.send(
            f"🌿 Mana Gathered for `{username}` (`{lord_id}`): `{previous.title}` → `{latest.title}`\n"
            f"💧 Total: {mana_latest:,}\n"
            f"📈 Gained: +{mana_gain:,}"
        )

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def topmana(ctx, top_n: int = 10):
    try:
        sheets = client.open("Copy SoS5").worksheets()
        if len(sheets) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = sheets[-1]
        previous = sheets[-2]

        data_latest = latest.get_all_values()
        data_prev = previous.get_all_values()
        headers = data_latest[0]

        id_index = headers.index("lord_id")
        name_index = 1
        mana_idx = 26  # Column AA

        def to_int(val):
            try: return int(val.replace(',', '').replace('-', '').strip())
            except: return 0

        prev_map = {row[id_index]: to_int(row[mana_idx]) for row in data_prev[1:] if len(row) > mana_idx}
        gains = []

        for row in data_latest[1:]:
            if len(row) > mana_idx:
                lord_id = row[id_index]
                name = row[name_index]
                mana_now = to_int(row[mana_idx])
                mana_prev = prev_map.get(lord_id, 0)
                gain = mana_now - mana_prev
                gains.append((name, gain))

        gains.sort(key=lambda x: x[1], reverse=True)
        result = "\n".join([f"{i+1}. `{name}` — 💧 +{mana:,}" for i, (name, mana) in enumerate(gains[:top_n])])

        await ctx.send(f"📊 **Top {top_n} Mana Gains** (`{previous.title}` → `{latest.title}`):\n{result}")

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def topheal(ctx, top_n: int = 10):
    try:
        sheets = client.open("Copy SoS5").worksheets()
        if len(sheets) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = sheets[-1]
        previous = sheets[-2]

        data_latest = latest.get_all_values()
        data_prev = previous.get_all_values()
        headers = data_latest[0]

        id_index = headers.index("lord_id")
        name_index = 1
        heal_idx = 18  # Column S

        def to_int(val):
            try: return int(val.replace(',', '').replace('-', '').strip())
            except: return 0

        prev_map = {row[id_index]: to_int(row[heal_idx]) for row in data_prev[1:] if len(row) > heal_idx}
        gains = []

        for row in data_latest[1:]:
            if len(row) > heal_idx:
                lord_id = row[id_index]
                name = row[name_index]
                healed_now = to_int(row[heal_idx])
                healed_prev = prev_map.get(lord_id, 0)
                gain = healed_now - healed_prev
                gains.append((name, gain))

        gains.sort(key=lambda x: x[1], reverse=True)
        result = "\n".join([f"{i+1}. `{name}` — ❤️‍🩹 +{heal:,}" for i, (name, heal) in enumerate(gains[:top_n])])

        await ctx.send(f"📊 **Top {top_n} Healed Gains** (`{previous.title}` → `{latest.title}`):\n{result}")

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

import os
TOKEN = os.getenv("TOKEN")

# Replace with your actual war-status channel ID
CHANNEL_ID = 1369071691111600168

# Allowed role ID
ALLOWED_ROLE_ID = 1258156722871337021

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


bot.run(TOKEN)
