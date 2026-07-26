import discord
from discord.ext import commands
import hashlib
import re
import io

# 1. Set up default intents
intents = discord.Intents.default()

# 2. Explicitly enable the members intent
intents.members = True 
intents.message_content = True # (You probably already have this for commands to work)

# 3. Pass the intents into your bot variable
bot = commands.Bot(command_prefix="!", intents=intents)

ZW_ZERO = "\u200B"
ZW_ONE = "\u200C"

# Updated filler words (No prefixes, blends perfectly) - 7,776 combinations
SIGN_OFF_GROUPS = [
    ["Stay alert.", "Be prepared.", "Stand by.", "Hold the line.", "Stay focused.", "Keep watch."],
    ["Watch the markers.", "Follow pings.", "Check alliance chat.", "Wait for orders.", "Listen to R4s.", "Track the target."],
    ["Prep your marches.", "Ready your troops.", "Form up.", "Gather forces.", "Prepare to rally.", "Assemble."],
    ["Check your talents.", "Ensure you are buffed.", "Refresh your shields.", "Check your stamina.", "Verify your artifacts.", "Use proper setups."],
    ["Move out.", "Advance.", "Deploy.", "Engage.", "Push forward.", "Execute."]
]

def generate_signoff(user_id: int) -> str:
    """Generates a deterministic string of tactical filler words."""
    selected_words = []
    
    hash_hex = hashlib.md5(str(user_id).encode()).hexdigest()
    deterministic_num = int(hash_hex, 16)
    
    for i, group in enumerate(SIGN_OFF_GROUPS):
        index = (deterministic_num >> (i * 4)) % len(group)
        selected_words.append(group[index])
        
    # Just normal text separated by spaces
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
    async def testbroadcast(self, ctx, members: commands.Greedy[discord.Member], *, announcement: str):
        if not members:
            await ctx.send("Please mention at least one member!")
            return

        await ctx.send(f"Processing test announcement and sending to {len(members)} member(s)...")
        
        # --- THE AUTO-INJECTOR ---
        if "[opsec]" not in announcement.lower():
            sentences = re.split(r'(?<=[.!?])\s+', announcement)
            if len(sentences) > 1:
                mid_point = len(sentences) // 2
                sentences.insert(mid_point, "[opsec]")
                announcement = " ".join(sentences)
            else:
                announcement = f"{announcement} [opsec]"
        # -------------------------

        sent, failed = 0, 0
        
        # --- LOGGING SETUP ---
        log_buffer = io.StringIO()
        log_buffer.write(f"--- TEST BROADCAST LOG ---\n")
        log_buffer.write(f"Target Members: {', '.join([m.name for m in members])}\n")
        log_buffer.write(f"Base Message: {announcement}\n")
        log_buffer.write(f"--------------------------\n\n")

        for member in members:
            if member.bot:
                continue

            unique_signoff = generate_signoff(member.id)
            visible_text = re.sub(r'\[opsec\]', unique_signoff, announcement, flags=re.IGNORECASE)
            full_msg = encode_watermark(visible_text, member.id)

            try:
                await member.send(full_msg)
                sent += 1
                # Log successful send
                log_buffer.write(f"Sent to: {member.name} (ID: {member.id})\nText: {visible_text}\n\n")
            except discord.Forbidden:
                failed += 1
                # Log failed send
                log_buffer.write(f"FAILED: {member.name} (ID: {member.id}) - DMs are disabled.\n\n")

        # --- SENDING THE LOG FILE ---
        log_channel_id = 1527938722987900978
        log_channel = ctx.bot.get_channel(log_channel_id)
        
        if log_channel:
            # Rewind the virtual file so Discord can read it
            log_buffer.seek(0)
            file = discord.File(fp=log_buffer, filename="testbroadcast_log.txt")
            await log_channel.send(
                content=f"**New Test Broadcast Report**\nInitiated by: {ctx.author.mention}\nSuccessfully sent to **{sent}** member(s). Failed: **{failed}**.", 
                file=file
            )
        else:
            await ctx.send(f"⚠️ **Warning:** Could not find log channel `{log_channel_id}`. Make sure the bot has 'View Channel' and 'Send Messages' permissions there.")
            
        log_buffer.close()

        await ctx.send(f"Test complete! Sent to {sent} member(s). Check your log channel.")

    @commands.command(name="broadcast")
    @commands.has_permissions(administrator=True)
    async def broadcast(self, ctx, role: discord.Role, *, announcement: str):
        
        # --- OPSEC SAFEGUARD ---
        # 1. Block @everyone completely
        if role.is_default() or role.name == "@everyone":
            await ctx.send("🚨 **OPSEC ALERT:** Broadcasting to everyone is explicitly blocked to prevent leaks.")
            return
            
        # 2. Whitelist: Only allow specific roles (Edit these to match your server!)
        allowed_roles = ["NVR Member"] 
        if role.name not in allowed_roles:
            await ctx.send(f"🚨 **OPSEC ALERT:** Broadcasts are restricted. You can only send to: `{', '.join(allowed_roles)}`")
            return
        # -----------------------

        await ctx.send(f"Processing announcement and sending uniquely blended messages to **{role.name}**...")
        
        # --- THE AUTO-INJECTOR ---
        # If no tag is found, the bot intelligently inserts one in the middle of the text
        if "[opsec]" not in announcement.lower():
            # Splits the text into a list of sentences based on punctuation (. ! ?)
            sentences = re.split(r'(?<=[.!?])\s+', announcement)
            
            if len(sentences) > 1:
                # Find the middle of the paragraph and insert the tag
                mid_point = len(sentences) // 2
                sentences.insert(mid_point, "[opsec]")
                announcement = " ".join(sentences)
            else:
                # If the announcement is literally just one sentence, append it to the end
                announcement = f"{announcement} [opsec]"
        # -------------------------

        sent, failed = 0, 0

        # --- LOGGING SETUP ---
        # Create an in-memory text file to store the exact messages
        log_buffer = io.StringIO()
        log_buffer.write(f"--- BROADCAST LOG ---\n")
        log_buffer.write(f"Target Role: {role.name}\n")
        log_buffer.write(f"Base Message: {announcement}\n")
        log_buffer.write(f"---------------------\n\n")

        for member in role.members:
            if member.bot:
                continue
            
            unique_signoff = generate_signoff(member.id)
            visible_text = re.sub(r'\[opsec\]', unique_signoff, announcement, flags=re.IGNORECASE)
            full_msg = encode_watermark(visible_text, member.id)
            
            try:
                await member.send(full_msg)
                sent += 1
                # Log successful send
                log_buffer.write(f"Sent to: {member.name} (ID: {member.id})\nText: {visible_text}\n\n")
            except discord.Forbidden:
                failed += 1
                # Log failed send (User has DMs turned off)
                log_buffer.write(f"FAILED: {member.name} (ID: {member.id}) - DMs are disabled.\n\n")

        # --- SENDING THE LOG FILE ---
        log_channel_id = 1527938722987900978
        log_channel = ctx.bot.get_channel(log_channel_id)
        
        if log_channel:
            # Rewind the virtual file to the beginning so Discord can read it
            log_buffer.seek(0)
            file = discord.File(fp=log_buffer, filename=f"broadcast_log_{role.name}.txt")
            await log_channel.send(
                content=f"**New Broadcast Report**\nInitiated by: {ctx.author.mention}\nSuccessfully sent to **{sent}** members. Failed: **{failed}**.", 
                file=file
            )
        else:
            await ctx.send(f"⚠️ **Warning:** Could not find log channel `{log_channel_id}`. Make sure the bot has 'View Channel' and 'Send Messages' permissions there.")
            
        # Clean up the virtual file
        log_buffer.close()

        await ctx.send(f"Broadcast complete! Sent to {sent} members of {role.name}. A full report has been sent to your log channel.")


    @commands.command(name="catchscreenshot")
    @commands.has_permissions(administrator=True)
    async def catchscreenshot(self, ctx, *, screenshot_text: str):
        """Matches text from a leaked screenshot against every member's generated variation."""
        await ctx.send("Fetching full server member list & searching...")
        
        if not ctx.guild.chunked:
            await ctx.guild.chunk()

        # Helper function to strip invisible characters, smart quotes, and punctuation
        def clean_text(raw_text: str) -> str:
            # 1. Remove all hidden zero-width / invisible Unicode characters
            cleaned = re.sub(r'[\u200b-\u200d\ufeff\u200e\u200f\u202a-\u202e]', '', raw_text)
            # 2. Lowercase and fix smart apostrophes/quotes
            cleaned = cleaned.lower().replace("’", "'").replace("“", '"').replace("”", '"')
            # 3. Strip out markdown formatting and punctuation so minor typos don't break matches
            cleaned = re.sub(r'[^a-z0-9\s]', '', cleaned)
            # 4. Collapse extra spaces
            return " ".join(cleaned.split())

        target_cleaned = clean_text(screenshot_text)
        matches = []

        for member in ctx.guild.members:
            if member.bot:
                continue

            # Generate and clean this member's expected sign-off
            member_signoff = generate_signoff(member.id)
            expected_cleaned = clean_text(member_signoff)

            # Check if their cleaned sign-off exists anywhere inside the pasted leak text
            if expected_cleaned in target_cleaned:
                matches.append(member)

        if matches:
            found_users = "\n".join([f"- `{m.name}` (ID: `{m.id}`)" for m in matches])
            await ctx.send(f"**MATCH FOUND!**\nThe leaked screenshot belongs to:\n{found_users}")
        else:
            await ctx.send("No exact match found. Double-check for typos or missing words.")
    
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

        # Helper function to strip invisible characters, smart quotes, and punctuation
        def clean_text(raw_text: str) -> str:
            # 1. Remove all hidden zero-width / invisible Unicode characters
            cleaned = re.sub(r'[\u200b-\u200d\ufeff\u200e\u200f\u202a-\u202e]', '', raw_text)
            # 2. Lowercase and fix smart apostrophes/quotes
            cleaned = cleaned.lower().replace("’", "'").replace("“", '"').replace("”", '"')
            # 3. Strip out markdown formatting and punctuation so minor typos don't break matches
            cleaned = re.sub(r'[^a-z0-9\s]', '', cleaned)
            # 4. Collapse extra spaces
            return " ".join(cleaned.split())

        target_cleaned = clean_text(screenshot_text)
        matches = []

        for member in ctx.guild.members:
            if member.bot:
                continue

            # Generate and clean this member's expected sign-off
            member_signoff = generate_signoff(member.id)
            expected_cleaned = clean_text(member_signoff)

            # Check if their cleaned sign-off exists anywhere inside the pasted leak text
            if expected_cleaned in target_cleaned:
                matches.append(member)

        if matches:
            found_users = "\n".join([f"- `{m.name}` (ID: `{m.id}`)" for m in matches])
            await ctx.send(f"**MATCH FOUND!**\nThe leaked screenshot belongs to:\n{found_users}")
        else:
            await ctx.send("No exact match found. Double-check for typos or missing words.")
            
async def setup(bot):
    await bot.add_cog(SpyDetector(bot))
