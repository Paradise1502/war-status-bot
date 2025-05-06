import discord
from discord.ext import commands

# Enable required intents
intents = discord.Intents.default()
intents.message_content = True

# Set command prefix
bot = commands.Bot(command_prefix='!', intents=intents)

# Replace with your actual bot token
import os
TOKEN = os.getenv("TOKEN")

# Replace with the channel ID of your war-status channel
CHANNEL_ID = 1369071691111600168


@bot.event
async def on_ready():
    print(f'✅ Bot is online as {bot.user}')


@bot.command()
async def warred(ctx):
    await ctx.send(
        "✅ Command received: Setting status to 🔴 FULL WAR...\n⚠️ *Channel rename may be delayed due to Discord rate limits.*"
    )
    channel = bot.get_channel(CHANNEL_ID)
    if channel:
        try:
            await channel.edit(name="〘🔴〙war-status-fullwar")
        except discord.errors.HTTPException as e:
            print(f"Rename failed or delayed: {e}")


@bot.command()
async def waryellow(ctx):
    await ctx.send(
        "✅ Command received: Setting status to 🟡 Skirmishes...\n⚠️ *Channel rename may be delayed due to Discord rate limits.*"
    )
    channel = bot.get_channel(CHANNEL_ID)
    if channel:
        try:
            await channel.edit(name="〘🟡〙war-status-skirmishes")
        except discord.errors.HTTPException as e:
            print(f"Rename failed or delayed: {e}")


@bot.command()
async def wargreen(ctx):
    await ctx.send(
        "✅ Command received: Setting status to 🟢 No Fighting...\n⚠️ *Channel rename may be delayed due to Discord rate limits.*"
    )
    channel = bot.get_channel(CHANNEL_ID)
    if channel:
        try:
            await channel.edit(name="〘🟢〙war-status-no-fighting")
        except discord.errors.HTTPException as e:
            print(f"Rename failed or delayed: {e}")


# Run the bot
bot.run(TOKEN)
