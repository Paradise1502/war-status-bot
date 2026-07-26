import discord
from discord.ext import commands

ZW_ZERO = "\u200B"
ZW_ONE = "\u200C"

# 1. Define visual variation choices
SYNONYM_GROUPS = [
    ["Operation", "Mission", "Op"],                  # Choice 1
    ["starts at", "begins at", "launches at"],      # Choice 2
    ["bring armor", "bring heavy armor", "use armor"],# Choice 3
    ["Be ready.", "Be prepared.", "Stand by."]       # Choice 4
]

def generate_visual_variation(user_id: int) -> str:
    """Uses the user's ID to deterministically pick a unique combination of words."""
    selected_words = []
    
    for i, group in enumerate(SYNONYM_GROUPS):
        # Pick a word index based on user ID modulo group size
        index = (user_id >> (i * 2)) % len(group)
        selected_words.append(group[index])
        
    # Example template combining selected words
    text = f"{selected_words[0]} {selected_words[1]} 20:00 UTC, {selected_words[2]}. {selected_words[3]}"
    
    # Optional: Toggle capitalization on the final word for extra variation
    if (user_id % 2) == 0:
        text = text.upper()  # Or make specific words uppercase
        
    return text

def encode_watermark(text: str, user_id: int) -> str:
    binary_id = f"{user_id:064b}"
    zw_payload = "".join(ZW_ONE if bit == "1" else ZW_ZERO for bit in binary_id)
    return f"{text}{zw_payload}"

def decode_watermark(text: str) -> int | None:
    bits = ["1" if char == ZW_ONE else "0" for char in text if char in (ZW_ZERO, ZW_ONE)]
    if len(bits) < 64:
        return None
    return int("".join(bits[-64:]), 2)

# --- THE COG CLASS ---
class SpyDetector(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    # 3. Test command (Sends a watermarked message ONLY to you)
    @commands.command(name="testbroadcast")
    @commands.has_permissions(administrator=True)
    async def testbroadcast(self, ctx, *, message: str):
        # Generates watermark using YOUR user ID
        watermarked_msg = encode_watermark(message, ctx.author.id)
        
        try:
            await ctx.author.send(watermarked_msg)
            await ctx.send("Test DM sent to you! Copy that DM and run `!catch <text>` to test.")
        except discord.Forbidden:
            await ctx.send("Failed to DM you. Please check if your DMs are open!")

    # 1. Command to send watermarked DMs
    @commands.command(name="broadcast")
    @commands.has_permissions(administrator=True)
    async def broadcast(self, ctx):
        await ctx.send("Sending unique watermarked/varied messages...")
        sent, failed = 0, 0

        for member in ctx.guild.members:
            if member.bot:
                continue
            
            # Step A: Generate unique visible text (for screenshots)
            visible_text = generate_visual_variation(member.id)
            
            # Step B: Attach invisible unicode watermark (for text copies)
            full_msg = encode_watermark(visible_text, member.id)
            
            try:
                await member.send(full_msg)
                sent += 1
            except discord.Forbidden:
                failed += 1

        await ctx.send(f"Broadcast complete! Sent to {sent} members.")

    # 4. Command to find a spy from a screenshot (using visual variations)
    @commands.command(name="catchscreenshot")
    @commands.has_permissions(administrator=True)
    async def catchscreenshot(self, ctx, *, screenshot_text: str):
        """Matches text from a leaked screenshot against every member's generated variation."""
        await ctx.send("Searching through server members...")
        
        matches = []
        
        # Clean up whitespace/casing for an easier match
        target_text = screenshot_text.strip().lower()

        for member in ctx.guild.members:
            if member.bot:
                continue

            # Re-generate what this specific member's announcement text looked like
            member_expected_text = generate_visual_variation(member.id).strip().lower()

            # Check if the screenshot matches this member's text
            if member_expected_text == target_text:
                matches.append(member)

        if matches:
            found_users = "\n".join([f"- `{m.name}` (ID: `{m.id}`)" for m in matches])
            await ctx.send(f"**MATCH FOUND!**\nThe leaked screenshot belongs to:\n{found_users}")
        else:
            await ctx.send("No exact match found. Make sure you typed/copied the screenshot text exactly as shown.")
    
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
