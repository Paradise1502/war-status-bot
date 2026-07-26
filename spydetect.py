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
TACTICAL_GROUPS = [
    ["Stay alert.", "Be prepared.", "Stand by.", "Hold the line.", "Stay focused.", "Keep watch.", "Eyes open.", "Stay sharp.", "Be ready.", "Hold positions.", "Watch the map.", "Stay vigilant.", "Maintain discipline.", "Stay online.", "Lock in.", "Keep your eyes peeled.", "Hold steady.", "Prepare for action.", "Stay on standby.", "Remain alert."],
    ["Watch the markers.", "Follow pings.", "Check alliance chat.", "Wait for orders.", "Listen to R4s.", "Track the target.", "Follow commands.", "Check your mail.", "Read the pins.", "Wait for the call.", "Listen to leadership.", "Follow the rally.", "Check the markers.", "Stay grouped.", "Watch for updates.", "Wait for pings.", "Observe the field.", "Follow the leads.", "Check Discord.", "Stay coordinated."],
    ["Move out.", "Advance.", "Deploy.", "Engage.", "Push forward.", "Execute.", "March out.", "Send troops.", "Move in.", "Attack.", "Push now.", "Go go go.", "Strike.", "Push the line.", "Advance troops.", "Move to target.", "Engage the enemy.", "Deploy forces.", "Push the objective.", "Execute orders."]
]
# --- CASUAL POOL (For Social) - 8,000 combinations ---
CASUAL_GROUPS = [
    ["Thanks for everything.", "Appreciate the effort.", "You guys are great.", "Love the energy.", "Keep it up.", "Stay awesome.", "Great job everyone.", "Thanks for playing.", "You all rock.", "Appreciate you all.", "Love this team.", "Keep up the good work.", "Thanks for being here.", "Glad you're here.", "You guys are the best.", "Thanks for the hard work.", "Love to see it.", "Great effort.", "Appreciate the activity.", "Thanks for sticking around."],
    ["Keep growing.", "Don't forget to gather.", "Enjoy the downtime.", "Take it easy.", "Have fun out there.", "Pace yourselves.", "Keep farming.", "Enjoy the game.", "Take a breather.", "Relax for a bit.", "Keep leveling up.", "Focus on growth.", "Enjoy the peace.", "Rest your troops.", "Farm up.", "Keep upgrading.", "Take some time off.", "Enjoy the day.", "Focus on yourselves.", "Keep progressing."],
    ["See you in chat.", "Catch you in VC.", "Ask if you need help.", "Reach out to R4s.", "Keep the vibes high.", "Stay active.", "See you around.", "Catch you later.", "Have a good one.", "Take care.", "Talk soon.", "See you on the map.", "Have a great day.", "Stay positive.", "Keep in touch.", "See you all later.", "Have a good week.", "Stay safe.", "Catch you all soon.", "See you next time."]
]

def generate_signoff(user_id: int, mode: str = "tactical") -> str:
    """Generates a secure 3-phrase string based on the chosen mode."""
    selected_words = []
    
    hash_hex = hashlib.md5(str(user_id).encode()).hexdigest()
    deterministic_num = int(hash_hex, 16)
    
    groups = TACTICAL_GROUPS if mode == "tactical" else CASUAL_GROUPS
    
    for i, group in enumerate(groups):
        # Shift bits safely to ensure independent selections
        index = (deterministic_num >> (i * 8)) % len(group)
        selected_words.append(group[index])
        
    # Now only returns 3 sentences!
    return f"{selected_words[0]} {selected_words[1]} {selected_words[2]}"

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
    @commands.command(name="testwarbroadcast") # <--- Renamed
    @commands.has_permissions(administrator=True)
    async def testwarbroadcast(self, ctx, members: commands.Greedy[discord.Member], *, announcement: str): # <--- Renamed
        
        # ... (Keep the rest of your OPSEC, Auto-Injector, and Logging logic the exact same) ...:
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

    @commands.command(name="warbroadcast") # <--- Renamed
    @commands.has_permissions(administrator=True)
    async def warbroadcast(self, ctx, role: discord.Role, *, announcement: str): # <--- Renamed
        
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

    @commands.command(name="socialbroadcast")
    @commands.has_permissions(administrator=True)
    async def socialbroadcast(self, ctx, role: discord.Role, *, announcement: str):
        if role.is_default() or role.name == "@everyone":
            await ctx.send("🚨 **OPSEC ALERT:** Broadcasting to everyone is explicitly blocked to prevent leaks.")
            return
            
        allowed_roles = ["Fighters", "CoreTeam", "Officers", "Alliance"] 
        if role.name not in allowed_roles:
            await ctx.send(f"🚨 **OPSEC ALERT:** Broadcasts are restricted. You can only send to: `{', '.join(allowed_roles)}`")
            return

        await ctx.send(f"Processing social announcement and sending uniquely blended messages to **{role.name}**...")
        
        if "[opsec]" not in announcement.lower():
            sentences = re.split(r'(?<=[.!?])\s+', announcement)
            if len(sentences) > 1:
                mid_point = len(sentences) // 2
                sentences.insert(mid_point, "[opsec]")
                announcement = " ".join(sentences)
            else:
                announcement = f"{announcement} [opsec]"

        sent, failed = 0, 0
        log_buffer = io.StringIO()
        log_buffer.write(f"--- SOCIAL BROADCAST LOG ---\nTarget Role: {role.name}\nBase Message: {announcement}\n--------------------------\n\n")
        
        for member in role.members:
            if member.bot:
                continue
            
            # This is the key difference: mode="casual"
            unique_signoff = generate_signoff(member.id, mode="casual")
            visible_text = re.sub(r'\[opsec\]', unique_signoff, announcement, flags=re.IGNORECASE)
            full_msg = encode_watermark(visible_text, member.id)
            
            try:
                await member.send(full_msg)
                sent += 1
                log_buffer.write(f"Sent to: {member.name} (ID: {member.id})\nText: {visible_text}\n\n")
            except discord.Forbidden:
                failed += 1
                log_buffer.write(f"FAILED: {member.name} (ID: {member.id}) - DMs disabled.\n\n")

        log_channel_id = 1527938722987900978
        log_channel = ctx.bot.get_channel(log_channel_id)
        if log_channel:
            log_buffer.seek(0)
            file = discord.File(fp=log_buffer, filename=f"social_log_{role.name}.txt")
            await log_channel.send(content=f"**New Social Broadcast Report**\nInitiated by: {ctx.author.mention}\nSuccessfully sent to **{sent}** members. Failed: **{failed}**.", file=file)
        
        log_buffer.close()
        await ctx.send(f"Social broadcast complete! Sent to {sent} members.")

    @commands.command(name="testsocialbroadcast")
    @commands.has_permissions(administrator=True)
    async def testsocialbroadcast(self, ctx, members: commands.Greedy[discord.Member], *, announcement: str):
        if not members:
            await ctx.send("Please mention at least one member!")
            return

        await ctx.send(f"Processing test social announcement...")
        
        if "[opsec]" not in announcement.lower():
            sentences = re.split(r'(?<=[.!?])\s+', announcement)
            if len(sentences) > 1:
                mid_point = len(sentences) // 2
                sentences.insert(mid_point, "[opsec]")
                announcement = " ".join(sentences)
            else:
                announcement = f"{announcement} [opsec]"

        sent, failed = 0, 0
        log_buffer = io.StringIO()
        log_buffer.write(f"--- TEST SOCIAL LOG ---\nTarget Members: {', '.join([m.name for m in members])}\nBase Message: {announcement}\n-----------------------\n\n")

        for member in members:
            if member.bot:
                continue

            # Switch to casual mode for the test
            unique_signoff = generate_signoff(member.id, mode="casual")
            visible_text = re.sub(r'\[opsec\]', unique_signoff, announcement, flags=re.IGNORECASE)
            full_msg = encode_watermark(visible_text, member.id)

            try:
                await member.send(full_msg)
                sent += 1
                log_buffer.write(f"Sent to: {member.name} (ID: {member.id})\nText: {visible_text}\n\n")
            except discord.Forbidden:
                failed += 1

        log_channel_id = 1527938722987900978
        log_channel = ctx.bot.get_channel(log_channel_id)
        if log_channel:
            log_buffer.seek(0)
            file = discord.File(fp=log_buffer, filename="test_social_log.txt")
            await log_channel.send(content=f"**New Test Social Report**\nInitiated by: {ctx.author.mention}", file=file)
            
        log_buffer.close()
        await ctx.send(f"Test complete! Sent to {sent} member(s). Check your log channel.")
    
    @commands.command(name="catchscreenshot", aliases=["catch"])
    @commands.has_permissions(administrator=True)
    async def catchscreenshot(self, ctx, *, screenshot_text: str):
        await ctx.send("Fetching full server member list & searching both war and social databases...")
        
        if not ctx.guild.chunked:
            await ctx.guild.chunk()

        def clean_text(raw_text: str) -> str:
            cleaned = re.sub(r'[\u200b-\u200d\ufeff\u200e\u200f\u202a-\u202e]', '', raw_text)
            cleaned = cleaned.lower().replace("’", "'").replace("“", '"').replace("”", '"')
            cleaned = re.sub(r'[^a-z0-9\s]', '', cleaned)
            return " ".join(cleaned.split())

        target_cleaned = clean_text(screenshot_text)
        matches = []

        for member in ctx.guild.members:
            if member.bot:
                continue

            # Generate both 3-phrase fingerprints for the user
            expected_tactical = clean_text(generate_signoff(member.id, mode="tactical"))
            expected_casual = clean_text(generate_signoff(member.id, mode="casual"))

            # Check if EITHER fingerprint is found in the pasted text
            if expected_tactical in target_cleaned or expected_casual in target_cleaned:
                matches.append(member)

        if matches:
            found_users = "\n".join([f"- `{m.name}` (ID: `{m.id}`)" for m in matches])
            await ctx.send(f"**MATCH FOUND!**\nThe leaked text belongs to:\n{found_users}")
        else:
            await ctx.send("No exact match found. Double-check for typos or missing words.")
            
async def setup(bot):
    await bot.add_cog(SpyDetector(bot))
