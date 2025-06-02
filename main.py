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
        id_index = headers.index("lord_id")

        # Column indices: AF=31, AG=32, AH=33, AI=34 (zero-indexed)
        gold_idx, wood_idx, ore_idx, mana_idx = 31, 32, 33, 34

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
