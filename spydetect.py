import discord
from discord.ext import commands

# 1. Set up default intents
intents = discord.Intents.default()

# 2. Explicitly enable the members intent
intents.members = True 
intents.message_content = True # (You probably already have this for commands to work)

# 3. Pass the intents into your bot variable
bot = commands.Bot(command_prefix="!", intents=intents)

ZW_ZERO = "\u200B"
ZW_ONE = "\u200C"

# 1. Useless filler words for the Tactical Sign-off (7,776 combinations)
SIGN_OFF_GROUPS = [
    ["Notice:", "Attention:", "Directive:", "Orders:", "Update:", "Briefing:"],
    ["Stay alert.", "Be prepared.", "Stand by.", "Hold the line.", "Stay focused.", "Keep watch."],
    ["Watch the markers.", "Follow pings.", "Check alliance chat.", "Wait for orders.", "Listen to R4s.", "Track the target."],
    ["Prep your marches.", "Ready your troops.", "Form up.", "Gather forces.", "Prepare to rally.", "Assemble."],
    ["Move out.", "Advance.", "Deploy.", "Engage.", "Push forward.", "Execute."]
]

def generate_signoff(user_id: int) -> str:
    """Generates a deterministic string of filler words for the bottom of the message."""
    selected_words = []
    
    hash_hex = hashlib.md5(str(user_id).encode()).hexdigest()
    deterministic_num = int(hash_hex, 16)
    
    for i, group in enumerate(SIGN_OFF_GROUPS):
        index = (deterministic_num >> (i * 4)) % len(group)
        selected_words.append(group[index])
        
    return f"{selected_words[0]} {selected_words[1]} {selected_words[2]} {selected_words[3]} {selected_words[4]}"

def generate_visual_variation(user_id: int) -> str:
    selected_words = []
    
    for i, group in enumerate(SYNONYM_GROUPS):
        # Pick word based on bits of the User ID
        index = (user_id >> (i * 2)) % len(group)
        selected_words.append(group[index])
        
    return f"{selected_words[0]} {selected_words[1]} 20:00 UTC, {selected_words[2]}. {selected_words[3]}"

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

    @commands.command(name="broadcast")
    @commands.has_permissions(administrator=True)
    async def broadcast(self, ctx, *, announcement: str): # <--- Added 'announcement' parameter
        await ctx.send("Sending unique watermarked/varied messages...")
        sent, failed = 0, 0

        for member in ctx.guild.members:
            if member.bot:
                continue
            
            # Step A: Generate their unique filler sign-off
            unique_signoff = generate_signoff(member.id)
            
            # Step B: Combine YOUR message with THEIR unique sign-off
            visible_text = f"{announcement}\n\n*{unique_signoff}*"
            
            # Step C: Attach invisible unicode watermark
            full_msg = encode_watermark(visible_text, member.id)
            
            print(f"[OPSEC LOG] DM to {member.name} ({member.id}): {unique_signoff}")
            
            try:
                await member.send(full_msg)
                sent += 1
            except discord.Forbidden:
                failed += 1

        await ctx.send(f"Broadcast complete! Sent to {sent} members.")

    # 5. Targeted test command (Sends to mentioned users only)
    @commands.command(name="testbroadcast")
    @commands.has_permissions(administrator=True)
    async def testbroadcast(self, ctx, members: commands.Greedy[discord.Member], *, announcement: str):
        """Sends custom announcements with sign-offs only to mentioned members.
        Usage: !testbroadcast @Officer1 @Officer2 We are hitting the Bastion at 20:00!
        """
        if not members:
            await ctx.send("Please mention at least one member to test with!\nExample: `!testbroadcast @User1 We are hitting the Bastion!`")
            return

        await ctx.send(f"Sending test messages to {len(members)} member(s)...")
        sent, failed = 0, 0

        for member in members:
            if member.bot:
                continue

            # Step A: Generate their unique filler sign-off
            unique_signoff = generate_signoff(member.id)
            
            # Step B: Combine YOUR custom message with THEIR unique sign-off
            visible_text = f"{announcement}\n\n*{unique_signoff}*"

            # Step C: Attach invisible unicode watermark (for text copies)
            full_msg = encode_watermark(visible_text, member.id)

            # Log to Railway console
            print(f"[TEST OPSEC LOG] DM to {member.name} ({member.id}): {unique_signoff}")

            try:
                await member.send(full_msg)
                sent += 1
            except discord.Forbidden:
                failed += 1
                await ctx.send(f"⚠️ Could not DM `{member.name}` (DMs are closed).")

        await ctx.send(f"Test complete! Sent to {sent} member(s).")
    
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

    # 4. Command to find a spy from a screenshot (using visual variations)
    @commands.command(name="catchscreenshot")
    @commands.has_permissions(administrator=True)
    async def catchscreenshot(self, ctx, *, screenshot_text: str):
        """Matches text from a leaked screenshot against every member's generated variation."""
        await ctx.send("Fetching full server member list & searching...")
        
        if not ctx.guild.chunked:
            await ctx.guild.chunk()

        matches = []
        
        # FIX: Strip out the invisible characters just in case the text was copy-pasted!
        ZW_ZERO = "\u200B"
        ZW_ONE = "\u200C"
        target_text = screenshot_text.replace(ZW_ZERO, "").replace(ZW_ONE, "").strip().lower()

        for member in ctx.guild.members:
            if member.bot:
                continue

            # Inside your catchscreenshot loop, change this one line:
            member_expected_text = generate_signoff(member.id).strip().lower()

            if member_expected_text == target_text:
                matches.append(member)

        if matches:
            found_users = "\n".join([f"- `{m.name}` (ID: `{m.id}`)" for m in matches])
            await ctx.send(f"**MATCH FOUND!**\nThe leaked screenshot belongs to:\n{found_users}")
        else:
            await ctx.send("No exact match found. Double-check for typos or extra punctuation.")
            
# This required function registers the Cog with your main bot
async def setup(bot):
    await bot.add_cog(SpyDetector(bot))
