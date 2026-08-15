import discord
from discord.ext import commands
import hashlib
import re
import io
import asyncio

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
# --- CASUAL POOL (For Social) - 8,000 combinations ---
# Structured to read perfectly as: [Appreciation]. [Encouragement]. [Sign-off].
CASUAL_GROUPS = [
    [
        "Great work everyone", "Appreciate the effort", "Thanks for staying active", 
        "Awesome job today", "Solid push everyone", "Thanks for the dedication", 
        "Great activity lately", "Love the energy here", "Excellent coordination", 
        "Proud of this team", "Good stuff everyone", "Thanks for stepping up", 
        "Appreciate the teamwork", "Amazing turnout", "Great job as always", 
        "Thanks for being ready", "Well played everyone", "Appreciate all of you", 
        "Fantastic work team", "Thanks for your time"
    ],
    [
        "Let's keep this momentum going", "Stay sharp for the next one", "Keep pushing those limits", 
        "Let's maintain this pace", "Keep up the great work", "Stay focused on our goals", 
        "Let's keep growing stronger", "Keep grinding those stats", "Let's stay ahead of the pack", 
        "Keep this activity up", "Let's keep dominating", "Stay ready for more", 
        "Keep improving every day", "Let's hold the line", "Keep your eyes on the prize", 
        "Let's stay united", "Keep the communication up", "Let's prepare for what's next", 
        "Keep showing up like this", "Let's keep winning together"
    ],
    [
        "See you on the battlefield", "More updates to follow", "Catch you all later", 
        "Have a great day", "Enjoy the rest of your week", "Talk to you all soon", 
        "See you in Discord", "Stay safe out there", "Take it easy", 
        "See you at reset", "Rest up for now", "Catch you at the next event", 
        "Enjoy your downtime", "See you in the voice channel", "Have a good one", 
        "Until next time", "Keep an eye on the pins", "See you all tomorrow", 
        "Stay awesome", "Take care everyone"
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

def generate_signoff(user_id: int, mode: str = "tactical") -> str:
    """Generates a secure 3-phrase string based on the chosen mode with natural formatting."""
    selected_words = []
    
    hash_hex = hashlib.md5(str(user_id).encode()).hexdigest()
    deterministic_num = int(hash_hex, 16)
    
    if mode == "tactical":
        groups = TACTICAL_GROUPS
    elif mode == "casual":
        groups = CASUAL_GROUPS
    elif mode == "row":
        groups = ROW_GROUPS
    else:
        groups = TACTICAL_GROUPS # Fallback
    
    for i, group in enumerate(groups):
        index = (deterministic_num >> (i * 8)) % len(group)
        selected_words.append(group[index])
        
    # Format the output so it looks completely natural to human readers
    if mode == "casual":
        # Turns "Stay active Keep going More soon" into "Stay active. Keep going. More soon."
        return f"{selected_words[0]}. {selected_words[1]}. {selected_words[2]}."
        
    elif mode == "row":
        # Turns ["ready", "join", "battle"] into "Be ready, join the battle."
        return f"Be {selected_words[0]}, {selected_words[1]} the {selected_words[2]}."
        
    else:
        # Tactical dictionary already has periods built into the words
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

    @commands.command(name="warbroadcast")
    @commands.has_permissions(administrator=True)
    async def warbroadcast(self, ctx, role: discord.Role, *, announcement: str):
        if role.is_default() or role.name == "@everyone":
            await ctx.send("🚨 **OPSEC ALERT:** Broadcasting to everyone is explicitly blocked to prevent leaks.")
            return
            
        allowed_roles = ["Test"] 
        if role.name not in allowed_roles:
            await ctx.send(f"🚨 **OPSEC ALERT:** Broadcasts are restricted. You can only send to: `{', '.join(allowed_roles)}`")
            return

        await ctx.send(f"Processing war announcement and sending uniquely blended messages to **{role.name}** (Auto-delete: 24h)...")
        
        sent, failed = 0, 0
        log_buffer = io.StringIO()
        log_buffer.write(f"--- WAR BROADCAST LOG ---\nTarget Role: {role.name}\nBase Message: {announcement}\n--------------------------\n\n")
        
        for member in role.members:
            if member.bot:
                continue
            
            unique_signoff = generate_signoff(member.id, mode="tactical")
            
            if "[opsec]" not in announcement.lower():
                paragraphs = [p for p in announcement.split("\n\n") if p.strip()]
                if len(paragraphs) >= 2:
                    mid = len(paragraphs) // 2
                    paragraphs[mid] = f"{paragraphs[mid]}\n{unique_signoff}"
                    visible_text = "\n\n".join(paragraphs)
                else:
                    visible_text = f"{announcement.strip()}\n\n{unique_signoff}"
            else:
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

        sent, failed = 0, 0
        log_buffer = io.StringIO()
        log_buffer.write(f"--- TEST WAR LOG ---\nTarget Members: {', '.join([m.name for m in members])}\nBase Message: {announcement}\n-----------------------\n\n")

        for member in members:
            if member.bot:
                continue

            unique_signoff = generate_signoff(member.id, mode="tactical")
            
            if "[opsec]" not in announcement.lower():
                paragraphs = [p for p in announcement.split("\n\n") if p.strip()]
                if len(paragraphs) >= 2:
                    mid = len(paragraphs) // 2
                    paragraphs[mid] = f"{paragraphs[mid]}\n{unique_signoff}"
                    visible_text = "\n\n".join(paragraphs)
                else:
                    visible_text = f"{announcement.strip()}\n\n{unique_signoff}"
            else:
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
            
        allowed_roles = ["Main RoW Team", "Test"] 
        if role.name not in allowed_roles:
            await ctx.send(f"🚨 **OPSEC ALERT:** Broadcasts are restricted. You can only send to: `{', '.join(allowed_roles)}`")
            return

        await ctx.send(f"Processing row announcement and sending uniquely blended messages to **{role.name}** (Auto-delete: 24h)...")
        
        sent, failed = 0, 0
        log_buffer = io.StringIO()
        log_buffer.write(f"--- ROW BROADCAST LOG ---\nTarget Role: {role.name}\nBase Message: {announcement}\n--------------------------\n\n")
        
        for member in role.members:
            if member.bot:
                continue
            
            unique_signoff = generate_signoff(member.id, mode="row")
            
            if "[opsec]" not in announcement.lower():
                paragraphs = [p for p in announcement.split("\n\n") if p.strip()]
                if len(paragraphs) >= 2:
                    mid = len(paragraphs) // 2
                    paragraphs[mid] = f"{paragraphs[mid]}\n{unique_signoff}"
                    visible_text = "\n\n".join(paragraphs)
                else:
                    visible_text = f"{announcement.strip()}\n\n{unique_signoff}"
            else:
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

        sent, failed = 0, 0
        log_buffer = io.StringIO()
        log_buffer.write(f"--- TEST ROW LOG ---\nTarget Members: {', '.join([m.name for m in members])}\nBase Message: {announcement}\n-----------------------\n\n")

        for member in members:
            if member.bot:
                continue

            unique_signoff = generate_signoff(member.id, mode="row")
            
            if "[opsec]" not in announcement.lower():
                paragraphs = [p for p in announcement.split("\n\n") if p.strip()]
                if len(paragraphs) >= 2:
                    mid = len(paragraphs) // 2
                    paragraphs[mid] = f"{paragraphs[mid]}\n{unique_signoff}"
                    visible_text = "\n\n".join(paragraphs)
                else:
                    visible_text = f"{announcement.strip()}\n\n{unique_signoff}"
            else:
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
            
        allowed_roles = ["Test"] 
        if role.name not in allowed_roles:
            await ctx.send(f"🚨 **OPSEC ALERT:** Broadcasts are restricted. You can only send to: `{', '.join(allowed_roles)}`")
            return

        await ctx.send(f"Processing social announcement and sending uniquely blended messages to **{role.name}** (Auto-delete: 24h)...")
        
        sent, failed = 0, 0
        log_buffer = io.StringIO()
        log_buffer.write(f"--- SOCIAL BROADCAST LOG ---\nTarget Role: {role.name}\nBase Message: {announcement}\n--------------------------\n\n")
        
        for member in role.members:
            if member.bot:
                continue
            
            # 1. Generate unique signoff per member
            unique_signoff = generate_signoff(member.id, mode="casual")
            
            # 2. Clean paragraph-based injection per member
            if "[opsec]" not in announcement.lower():
                paragraphs = [p for p in announcement.split("\n\n") if p.strip()]
                if len(paragraphs) >= 2:
                    mid = len(paragraphs) // 2
                    paragraphs[mid] = f"{paragraphs[mid]}\n{unique_signoff}"
                    visible_text = "\n\n".join(paragraphs)
                else:
                    visible_text = f"{announcement.strip()}\n\n{unique_signoff}"
            else:
                visible_text = re.sub(r'\[opsec\]', unique_signoff, announcement, flags=re.IGNORECASE)

            # 3. Watermark and Send
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

        sent, failed = 0, 0
        log_buffer = io.StringIO()
        log_buffer.write(f"--- TEST SOCIAL LOG ---\nTarget Members: {', '.join([m.name for m in members])}\nBase Message: {announcement}\n-----------------------\n\n")

        for member in members:
            if member.bot:
                continue

            unique_signoff = generate_signoff(member.id, mode="casual")
            
            if "[opsec]" not in announcement.lower():
                paragraphs = [p for p in announcement.split("\n\n") if p.strip()]
                if len(paragraphs) >= 2:
                    mid = len(paragraphs) // 2
                    paragraphs[mid] = f"{paragraphs[mid]}\n{unique_signoff}"
                    visible_text = "\n\n".join(paragraphs)
                else:
                    visible_text = f"{announcement.strip()}\n\n{unique_signoff}"
            else:
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

            # Inside your catchscreenshot loop:
            expected_tactical = clean_text(generate_signoff(member.id, mode="tactical"))
            expected_casual = clean_text(generate_signoff(member.id, mode="casual"))
            expected_row = clean_text(generate_signoff(member.id, mode="row"))

            # Check if ANY of the three fingerprints are found
            if expected_tactical in target_cleaned or expected_casual in target_cleaned or expected_row in target_cleaned:
                matches.append(member)

        if matches:
            found_users = "\n".join([f"- `{m.name}` (ID: `{m.id}`)" for m in matches])
            await ctx.send(f"**MATCH FOUND!**\nThe leaked text belongs to:\n{found_users}")
        else:
            await ctx.send("No exact match found. Double-check for typos or missing words.")
            
async def setup(bot):
    await bot.add_cog(SpyDetector(bot))
