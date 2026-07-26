import discord
from discord.ext import commands

# --- INVISIBLE WATERMARK HELPER FUNCTIONS ---
ZW_ZERO = "\u200B"  # Invisible zero-width space (represents binary 0)
ZW_ONE = "\u200C"   # Invisible zero-width non-joiner (represents binary 1)

def encode_watermark(text: str, user_id: int) -> str:
    """Hides the user's ID at the end of the message as invisible text."""
    binary_id = f"{user_id:064b}"
    zw_payload = "".join(ZW_ONE if bit == "1" else ZW_ZERO for bit in binary_id)
    return f"{text}{zw_payload}"

def decode_watermark(text: str) -> int | None:
    """Reads invisible text and converts it back into a user ID."""
    bits = ["1" if char == ZW_ONE else "0" for char in text if char in (ZW_ZERO, ZW_ONE)]
    if len(bits) < 64:
        return None
    return int("".join(bits[-64:]), 2)


# --- THE COG CLASS ---
class SpyDetector(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    # 1. Command to send watermarked DMs
    @commands.command(name="broadcast")
    @commands.has_permissions(administrator=True)
    async def broadcast(self, ctx, *, message: str):
        await ctx.send("Sending watermarked messages to members...")
        sent, failed = 0, 0

        for member in ctx.guild.members:
            if member.bot:
                continue
            
            # Hide this specific user's ID in the text
            watermarked_msg = encode_watermark(message, member.id)
            
            try:
                await member.send(watermarked_msg)
                sent += 1
            except discord.Forbidden:
                failed += 1 # Member has DMs blocked

        await ctx.send(f"Done! Sent to {sent} members ({failed} failed due to closed DMs).")

    # 2. Command to catch the spy from leaked text
    @commands.command(name="catch")
    @commands.has_permissions(administrator=True)
    async def catch(self, ctx, *, leaked_text: str):
        user_id = decode_watermark(leaked_text)
        
        if not user_id:
            await ctx.send("No watermark found in this text.")
            return

        member = ctx.guild.get_member(user_id) or await self.bot.fetch_user(user_id)
        if member:
            await ctx.send(f"**SPY FOUND!**\nUser: `{member.name}`\nID: `{user_id}`")
        else:
            await ctx.send(f"Found watermark for User ID `{user_id}`, but they left the server.")

# This required function registers the Cog with your main bot
async def setup(bot):
    await bot.add_cog(SpyDetector(bot))
