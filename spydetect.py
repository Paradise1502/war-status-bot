import discord
from discord.ext import commands
import hashlib
import re
import io
import asyncio
import random

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
# Designed to fit universally into any appreciation, social, or general update message.
CASUAL_GROUPS = [
    [
        "Stay active", "Keep going", "Good work", "Stay strong", "Stay ready",
        "Keep pushing", "Stay sharp", "Well done", "Keep grinding", "Great job",
        "Nice work", "Stay focused", "Keep improving", "Stay motivated", "Keep fighting",
        "Stay united", "Good luck", "Keep winning", "Stay awesome", "Much appreciated"
    ],
    [
        "Thanks everyone", "Much love", "Stay safe", "Take care", "See everyone",
        "Catch everyone", "Talk soon", "Until later", "Enjoy yourselves", "Happy gaming",
        "Keep smiling", "Stay positive", "Good vibes", "Team first", "Always together",
        "Strong alliance", "Keep growing", "Great energy", "Looking forward", "See ya"
    ],
    [
        "Cheers everyone", "Stay tuned", "More soon", "Next update", "Keep checking",
        "See Discord", "Join voice", "Stay online", "Until tomorrow", "Next event",
        "See around", "Thanks again", "Have fun", "Take easy", "Stay awesome",
        "Keep connected", "Victory awaits", "Never rivaled", "See later"
    ]
]

ROW_GROUPS = [
    [
        "ready",
        "prepared",
        "focused",
        "active",
        "online",
        "early",
        "available",
        "organized",
        "alert",
        "steady",
        "committed",
        "locked",
        "waiting",
        "standing",
        "grouped",
        "connected",
        "updated",
        "aware",
        "calm",
        "confident",
        "motivated",
        "coordinated",
        "disciplined",
        "engaged",
        "present",
        "positioned",
        "settled",
        "watching",
        "moving",
        "prepared"
    ],

    [
        "join",
        "check",
        "follow",
        "watch",
        "keep",
        "stay",
        "bring",
        "prepare",
        "support",
        "coordinate",
        "listen",
        "follow",
        "maintain",
        "hold",
        "push",
        "move",
        "assist",
        "protect",
        "secure",
        "control",
        "gather",
        "build",
        "cover",
        "respond",
        "react",
        "review",
        "confirm",
        "organize",
        "continue",
        "remain"
    ],

    [
        "battle",
        "match",
        "event",
        "push",
        "call",
        "team",
        "group",
        "plan",
        "operation",
        "strategy",
        "war",
        "fight",
        "phase",
        "round",
        "objective",
        "attack",
        "defense",
        "movement",
        "rotation",
        "formation",
        "schedule",
        "mission",
        "effort",
        "attempt",
        "challenge",
        "campaign",
        "engagement",
        "session",
        "activity",
        "deployment"
    ]
]

def generate_signoff_phrases(user_id: int, mode: str = "tactical") -> list:
    """Picks 3 completely random phrases from the chosen mode's dictionary."""
    if mode == "tactical":
        groups = TACTICAL_GROUPS
    elif mode == "casual":
        groups = CASUAL_GROUPS
    elif mode == "row":
        groups = ROW_GROUPS
    else:
        groups = TACTICAL_GROUPS
    
    # Pick a random phrase from each of the 3 groups
    selected_words = [random.choice(group) for group in groups]
        
    return selected_words # Returns a list of 3 random phrases

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

    @commands.command(name="warbroadcast")
    @commands.has_permissions(administrator=True)
    async def warbroadcast(self, ctx, role: discord.Role, *, announcement: str):
        if role.is_default() or role.name == "@everyone":
            await ctx.send("🚨 **OPSEC ALERT:** Broadcasting to everyone is explicitly blocked to prevent leaks.")
            return
            
        allowed_roles = ["NVR Member"] 
        if role.name not in allowed_roles:
            await ctx.send(f"🚨 **OPSEC ALERT:** Broadcasts are restricted. You can only send to: `{', '.join(allowed_roles)}`")
            return

        await ctx.send(f"Processing war announcement and sending uniquely blended messages to **{role.name}** (Auto-delete: 24h)...")
        
        if "[opsec]" not in announcement.lower():
            parts = re.split(r'(?<=[.!?])(\s+)', announcement)
            if len(parts) > 2:
                mid_point = (len(parts) // 4) * 2 
                parts.insert(mid_point + 1, " [opsec]")
                announcement = "".join(parts)
            else:
                announcement = f"{announcement} [opsec]"

        sent, failed = 0, 0
        log_buffer = io.StringIO()
        log_buffer.write(f"--- WAR BROADCAST LOG ---\nTarget Role: {role.name}\nBase Message: {announcement}\n--------------------------\n\n")
        
        for member in role.members:
            if member.bot:
                continue
            
            unique_signoff = generate_signoff(member.id, mode="tactical")
            visible_text = re.sub(r'\[opsec\]', unique_signoff, announcement, flags=re.IGNORECASE)
            full_msg = encode_watermark(visible_text, member.id)

            if len(full_msg) > 2000:
                await ctx.send(f"🚨 **ERROR:** Announcement too long! Reached **{len(full_msg)}/2000** characters for {member.name}. Please shorten your text and try again.")
                return 
            
            try:
                sent_msg = await member.send(full_msg)
                sent += 1
                log_buffer.write(f"Sent to: {member.name} (ID: {member.id})\nText: {visible_text}\n\n")

                async def delete_after_delay(message, delay):
                    await asyncio.sleep(delay)
                    try:
                        await message.delete()
                    except discord.HTTPException:
                        pass

                ctx.bot.loop.create_task(delete_after_delay(sent_msg, 86400)) # 24 Hours

            except discord.Forbidden:
                failed += 1
                log_buffer.write(f"FAILED: {member.name} (ID: {member.id}) - DMs disabled.\n\n")

        log_channel_id = 1527938722987900978
        log_channel = ctx.bot.get_channel(log_channel_id)
        if log_channel:
            log_buffer.seek(0)
            file = discord.File(fp=log_buffer, filename=f"war_log_{role.name}.txt")
            await log_channel.send(content=f"**New War Broadcast Report**\nInitiated by: {ctx.author.mention}\nSuccessfully sent to **{sent}** members. Failed: **{failed}**.", file=file)
        
        log_buffer.close()
        await ctx.send(f"War broadcast complete! Sent to {sent} members.")

    @commands.command(name="testwarbroadcast")
    @commands.has_permissions(administrator=True)
    async def testwarbroadcast(self, ctx, members: commands.Greedy[discord.Member], *, announcement: str):
        if not members:
            await ctx.send("Please mention at least one member!")
            return

        await ctx.send(f"Processing test war announcement (with 30s auto-delete timer)...")
        
        if "[opsec]" not in announcement.lower():
            parts = re.split(r'(?<=[.!?])(\s+)', announcement)
            if len(parts) > 2:
                mid_point = (len(parts) // 4) * 2 
                parts.insert(mid_point + 1, " [opsec]")
                announcement = "".join(parts)
            else:
                announcement = f"{announcement} [opsec]"

        sent, failed = 0, 0
        log_buffer = io.StringIO()
        log_buffer.write(f"--- TEST WAR LOG ---\nTarget Members: {', '.join([m.name for m in members])}\nBase Message: {announcement}\n-----------------------\n\n")

        for member in members:
            if member.bot:
                continue

            # 1. Get 3 random phrases
            phrases = generate_signoff_phrases(member.id, mode="tactical") # (Change mode per command: tactical, casual, or row)
            
            # 2. Clean Scatter Injector with proper spacing
            current_announcement = announcement
            
            # If user didn't manually provide custom tags, automatically scatter them
            if "[opsec]" not in current_announcement.lower() and "[opsec1]" not in current_announcement.lower():
                parts = re.split(r'(?<=[.!?])\s*', current_announcement)
                if len(parts) >= 6:
                    p1 = len(parts) // 4
                    p2 = (len(parts) // 4) * 2
                    p3 = (len(parts) // 4) * 3
                    
                    parts[p1] = f"{parts[p1]} {phrases[0]}"
                    parts[p2] = f"{parts[p2]} {phrases[1]}"
                    parts[p3] = f"{parts[p3]} {phrases[2]}"
                    visible_text = "".join(parts)
                else:
                    # Fallback for short text: clean trailing space and append nicely
                    visible_text = f"{current_announcement.strip()} {phrases[0]} {phrases[1]} {phrases[2]}"
            else:
                # Manual replacement support if tags are used
                visible_text = current_announcement.replace("[opsec1]", phrases[0]).replace("[opsec2]", phrases[1]).replace("[opsec3]", phrases[2])
                visible_text = visible_text.replace("[opsec]", f"{phrases[0]} {phrases[1]} {phrases[2]}")

            full_msg = encode_watermark(visible_text, member.id)

            if len(full_msg) > 2000:
                await ctx.send(f"🚨 **ERROR:** Announcement too long! Reached **{len(full_msg)}/2000** characters for {member.name}. Please shorten your text and try again.")
                return 

            try:
                sent_msg = await member.send(full_msg)
                sent += 1
                log_buffer.write(f"Sent to: {member.name} (ID: {member.id})\nText: {visible_text}\n\n")

                async def delete_after_delay(message, delay):
                    await asyncio.sleep(delay)
                    try:
                        await message.delete()
                    except discord.HTTPException:
                        pass

                ctx.bot.loop.create_task(delete_after_delay(sent_msg, 30))

            except discord.Forbidden:
                failed += 1

        log_channel_id = 1527938722987900978
        log_channel = ctx.bot.get_channel(log_channel_id)
        if log_channel:
            log_buffer.seek(0)
            file = discord.File(fp=log_buffer, filename="test_war_log.txt")
            await log_channel.send(content=f"**New Test War Report (Auto-delete: 30s)**\nInitiated by: {ctx.author.mention}", file=file)
            
        log_buffer.close()
        await ctx.send(f"Test complete! Sent to {sent} member(s). DMs will vanish in 30 seconds.")

    @commands.command(name="rowbroadcast")
    @commands.has_permissions(administrator=True)
    async def rowbroadcast(self, ctx, role: discord.Role, *, announcement: str):
        if role.is_default() or role.name == "@everyone":
            await ctx.send("🚨 **OPSEC ALERT:** Broadcasting to everyone is explicitly blocked to prevent leaks.")
            return
            
        allowed_roles = ["Main RoW Team", "RoW Team 2"] 
        if role.name not in allowed_roles:
            await ctx.send(f"🚨 **OPSEC ALERT:** Broadcasts are restricted. You can only send to: `{', '.join(allowed_roles)}`")
            return

        await ctx.send(f"Processing row announcement and sending uniquely blended messages to **{role.name}** (Auto-delete: 24h)...")
        
        if "[opsec]" not in announcement.lower():
            parts = re.split(r'(?<=[.!?])(\s+)', announcement)
            if len(parts) > 2:
                mid_point = (len(parts) // 4) * 2 
                parts.insert(mid_point + 1, " [opsec]")
                announcement = "".join(parts)
            else:
                announcement = f"{announcement} [opsec]"

        sent, failed = 0, 0
        log_buffer = io.StringIO()
        log_buffer.write(f"--- ROW BROADCAST LOG ---\nTarget Role: {role.name}\nBase Message: {announcement}\n--------------------------\n\n")
        
        for member in role.members:
            if member.bot:
                continue
            
            unique_signoff = generate_signoff(member.id, mode="row")
            visible_text = re.sub(r'\[opsec\]', unique_signoff, announcement, flags=re.IGNORECASE)
            full_msg = encode_watermark(visible_text, member.id)

            if len(full_msg) > 2000:
                await ctx.send(f"🚨 **ERROR:** Announcement too long! Reached **{len(full_msg)}/2000** characters for {member.name}.")
                return 
            
            try:
                sent_msg = await member.send(full_msg)
                sent += 1
                log_buffer.write(f"Sent to: {member.name} (ID: {member.id})\nText: {visible_text}\n\n")

                async def delete_after_delay(message, delay):
                    await asyncio.sleep(delay)
                    try:
                        await message.delete()
                    except discord.HTTPException:
                        pass

                ctx.bot.loop.create_task(delete_after_delay(sent_msg, 86400)) # 24 Hours

            except discord.Forbidden:
                failed += 1
                log_buffer.write(f"FAILED: {member.name} (ID: {member.id}) - DMs disabled.\n\n")

        log_channel_id = 1527938722987900978
        log_channel = ctx.bot.get_channel(log_channel_id)
        if log_channel:
            log_buffer.seek(0)
            file = discord.File(fp=log_buffer, filename=f"row_log_{role.name}.txt")
            await log_channel.send(content=f"**New Row Broadcast Report**\nInitiated by: {ctx.author.mention}\nSuccessfully sent to **{sent}** members. Failed: **{failed}**.", file=file)
        
        log_buffer.close()
        await ctx.send(f"Row broadcast complete! Sent to {sent} members.")

    @commands.command(name="testrowbroadcast")
    @commands.has_permissions(administrator=True)
    async def testrowbroadcast(self, ctx, members: commands.Greedy[discord.Member], *, announcement: str):
        if not members:
            await ctx.send("Please mention at least one member!")
            return

        await ctx.send(f"Processing test row announcement (with 30s auto-delete timer)...")
        
        if "[opsec]" not in announcement.lower():
            parts = re.split(r'(?<=[.!?])(\s+)', announcement)
            if len(parts) > 2:
                mid_point = (len(parts) // 4) * 2 
                parts.insert(mid_point + 1, " [opsec]")
                announcement = "".join(parts)
            else:
                announcement = f"{announcement} [opsec]"

        sent, failed = 0, 0
        log_buffer = io.StringIO()
        log_buffer.write(f"--- TEST ROW LOG ---\nTarget Members: {', '.join([m.name for m in members])}\nBase Message: {announcement}\n-----------------------\n\n")

        for member in members:
            if member.bot:
                continue

            unique_signoff = generate_signoff(member.id, mode="row")
            visible_text = re.sub(r'\[opsec\]', unique_signoff, announcement, flags=re.IGNORECASE)
            full_msg = encode_watermark(visible_text, member.id)

            if len(full_msg) > 2000:
                await ctx.send(f"🚨 **ERROR:** Announcement too long! Reached **{len(full_msg)}/2000** characters for {member.name}.")
                return 

            try:
                sent_msg = await member.send(full_msg)
                sent += 1
                log_buffer.write(f"Sent to: {member.name} (ID: {member.id})\nText: {visible_text}\n\n")

                async def delete_after_delay(message, delay):
                    await asyncio.sleep(delay)
                    try:
                        await message.delete()
                    except discord.HTTPException:
                        pass

                ctx.bot.loop.create_task(delete_after_delay(sent_msg, 30))

            except discord.Forbidden:
                failed += 1

        log_channel_id = 1527938722987900978
        log_channel = ctx.bot.get_channel(log_channel_id)
        if log_channel:
            log_buffer.seek(0)
            file = discord.File(fp=log_buffer, filename="test_row_log.txt")
            await log_channel.send(content=f"**New Test Row Report (Auto-delete: 30s)**\nInitiated by: {ctx.author.mention}", file=file)
            
        log_buffer.close()
        await ctx.send(f"Test complete! Sent to {sent} member(s). DMs will vanish in 30 seconds.")

    @commands.command(name="socialbroadcast")
    @commands.has_permissions(administrator=True)
    async def socialbroadcast(self, ctx, role: discord.Role, *, announcement: str):
        if role.is_default() or role.name == "@everyone":
            await ctx.send("🚨 **OPSEC ALERT:** Broadcasting to everyone is explicitly blocked to prevent leaks.")
            return
            
        allowed_roles = ["NVR Member"] 
        if role.name not in allowed_roles:
            await ctx.send(f"🚨 **OPSEC ALERT:** Broadcasts are restricted. You can only send to: `{', '.join(allowed_roles)}`")
            return

        await ctx.send(f"Processing social announcement and sending uniquely blended messages to **{role.name}** (Auto-delete: 24h)...")
        
        if "[opsec]" not in announcement.lower():
            parts = re.split(r'(?<=[.!?])(\s+)', announcement)
            if len(parts) > 2:
                mid_point = (len(parts) // 4) * 2 
                parts.insert(mid_point + 1, " [opsec]")
                announcement = "".join(parts)
            else:
                announcement = f"{announcement} [opsec]"

        sent, failed = 0, 0
        log_buffer = io.StringIO()
        log_buffer.write(f"--- SOCIAL BROADCAST LOG ---\nTarget Role: {role.name}\nBase Message: {announcement}\n--------------------------\n\n")
        
        for member in role.members:
            if member.bot:
                continue
            
            unique_signoff = generate_signoff(member.id, mode="casual")
            visible_text = re.sub(r'\[opsec\]', unique_signoff, announcement, flags=re.IGNORECASE)
            full_msg = encode_watermark(visible_text, member.id)

            if len(full_msg) > 2000:
                await ctx.send(f"🚨 **ERROR:** Announcement too long! Reached **{len(full_msg)}/2000** characters for {member.name}.")
                return 
            
            try:
                sent_msg = await member.send(full_msg)
                sent += 1
                log_buffer.write(f"Sent to: {member.name} (ID: {member.id})\nText: {visible_text}\n\n")

                async def delete_after_delay(message, delay):
                    await asyncio.sleep(delay)
                    try:
                        await message.delete()
                    except discord.HTTPException:
                        pass

                ctx.bot.loop.create_task(delete_after_delay(sent_msg, 86400)) # 24 Hours

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

    @commands.command(name="testsocialbroadcast", aliases=["testsocial"])
    @commands.has_permissions(administrator=True)
    async def testsocialbroadcast(self, ctx, members: commands.Greedy[discord.Member], *, announcement: str):
        if not members:
            await ctx.send("Please mention at least one member!")
            return

        await ctx.send(f"Processing test social announcement (with 30s auto-delete timer)...")
        
        if "[opsec]" not in announcement.lower():
            parts = re.split(r'(?<=[.!?])(\s+)', announcement)
            if len(parts) > 2:
                mid_point = (len(parts) // 4) * 2 
                parts.insert(mid_point + 1, " [opsec]")
                announcement = "".join(parts)
            else:
                announcement = f"{announcement} [opsec]"

        sent, failed = 0, 0
        log_buffer = io.StringIO()
        log_buffer.write(f"--- TEST SOCIAL LOG ---\nTarget Members: {', '.join([m.name for m in members])}\nBase Message: {announcement}\n-----------------------\n\n")

        for member in members:
            if member.bot:
                continue

            unique_signoff = generate_signoff(member.id, mode="casual")
            visible_text = re.sub(r'\[opsec\]', unique_signoff, announcement, flags=re.IGNORECASE)
            full_msg = encode_watermark(visible_text, member.id)

            if len(full_msg) > 2000:
                await ctx.send(f"🚨 **ERROR:** Announcement too long! Reached **{len(full_msg)}/2000** characters for {member.name}.")
                return 

            try:
                sent_msg = await member.send(full_msg)
                sent += 1
                log_buffer.write(f"Sent to: {member.name} (ID: {member.id})\nText: {visible_text}\n\n")

                async def delete_after_delay(message, delay):
                    await asyncio.sleep(delay)
                    try:
                        await message.delete()
                    except discord.HTTPException:
                        pass

                ctx.bot.loop.create_task(delete_after_delay(sent_msg, 30))

            except discord.Forbidden:
                failed += 1

        log_channel_id = 1527938722987900978
        log_channel = ctx.bot.get_channel(log_channel_id)
        if log_channel:
            log_buffer.seek(0)
            file = discord.File(fp=log_buffer, filename="test_social_log.txt")
            await log_channel.send(content=f"**New Test Social Report (Auto-delete: 30s)**\nInitiated by: {ctx.author.mention}", file=file)
            
        log_buffer.close()
        await ctx.send(f"Test complete! Sent to {sent} member(s). DMs will vanish in 30 seconds.")
    
   @commands.command(name="catchscreenshot", aliases=["catch"])
    @commands.has_permissions(administrator=True)
    async def catchscreenshot(self, ctx, *, screenshot_text: str):
        await ctx.send("Fetching full server member list & searching all broadcast databases...")
        
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

            # Generate the 3 randomized phrases for this user across all modes
            tactical_phrases = generate_signoff_phrases(member.id, mode="tactical")
            casual_phrases = generate_signoff_phrases(member.id, mode="casual")
            row_phrases = generate_signoff_phrases(member.id, mode="row")

            # Clean each phrase for accurate comparison
            clean_tactical = [clean_text(p) for p in tactical_phrases]
            clean_casual = [clean_text(p) for p in casual_phrases]
            clean_row = [clean_text(p) for p in row_phrases]

            # Check if ALL 3 phrases of any mode appear anywhere in the leaked screenshot text
            has_tactical = all(p in target_cleaned for p in clean_tactical)
            has_casual = all(p in target_cleaned for p in clean_casual)
            has_row = all(p in target_cleaned for p in clean_row)

            if has_tactical or has_casual or has_row:
                matches.append(member)

        if matches:
            found_users = "\n".join([f"- `{m.name}` (ID: `{m.id}`)" for m in matches])
            await ctx.send(f"**MATCH FOUND!**\nThe leaked text belongs to:\n{found_users}")
        else:
            await ctx.send("No exact match found. Double-check for typos or missing words.")
            
async def setup(bot):
    await bot.add_cog(SpyDetector(bot))
