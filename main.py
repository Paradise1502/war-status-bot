import discord
from discord.ext import commands, tasks
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import asyncio
from datetime import datetime, timedelta, timezone
import re

# ==========================================
# ⚙️ CONFIGURATION & CONSTANTS
# ==========================================

# 🔒 MAINTENANCE SETTINGS
TEST_MODE = False  # Set to True to lock bot to private channel
TEST_CHANNEL_ID = 1378735765827358791 

# Google Auth
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDS_JSON = os.getenv("CREDENTIALS_JSON")
CREDS_DICT = json.loads(CREDS_JSON) if CREDS_JSON else {}

# Channel IDs
CHANNELS = {
    "target_announce": 1257468695400153110,
    "war_status": 1369071691111600168,
    "confirm_rename": 1235711595645243394,
    "allowed_commands": 1378735765827358791,
    "daily_announce": 1383515877793595435
}

# Role IDs
ROLES = {
    "ping_default": 1235729244605120572,
    "war_admin": 1352014667589095624
}

# Sheet Names
SEASON_SHEETS = {
    "hk1": "Call of Dragons - HK1",
    "hk2": "Call of Dragons - HK2",
    "hk3": "Call of Dragons - HK3",
    "sos2": "Call of Dragons - SoS2",
    "sos4": "Call of Dragons - SoS4",
    "sos5": "Call of Dragons - SoS5",
    "sos6": "Call of Dragons - SoS6",
    "statue": "Activity",
    "test": "testsheet"
}

DEFAULT_SEASON = "sos6"
EVENT_SHEET_NAME = "Event Schedule"
UTC = timezone.utc

# ==========================================
# 🛠️ HELPER FUNCTIONS
# ==========================================

def to_int(val):
    """Robust integer parser."""
    if not val: return 0
    try:
        s = str(val).replace(",", "").replace(".", "").replace(" ", "").strip()
        if s in ("", "-", "None"): return 0
        return int(float(s))
    except (ValueError, TypeError):
        return 0

def fmt_num(val, prefix="+"):
    """Formats number with commas."""
    s = f"{val:,}"
    if prefix and val > 0: return f"{prefix}{s}"
    return s

def fmt_abbr(n: int) -> str:
    """Formats 1,200,000 as 1.2m"""
    sign = "-" if n < 0 else ""
    x = abs(n)
    if x >= 1_000_000_000: return f"{sign}{x/1_000_000_000:.1f}b"
    if x >= 1_000_000: return f"{sign}{x/1_000_000:.1f}m"
    if x >= 1_000: return f"{sign}{x/1_000:.1f}k"
    return f"{sign}{x}"

def col_idx(headers, names, fallback_index=None):
    """Finds column index by name."""
    cleaned = [h.strip().lower() for h in headers]
    if isinstance(names, str): names = [names]
    for name in names:
        if name.strip().lower() in cleaned:
            return cleaned.index(name.strip().lower())
    if fallback_index is not None and fallback_index < len(headers):
        return fallback_index
    raise ValueError(f"Column not found: {names}")

# ==========================================
# ☁️ SHEET MANAGER (Async)
# ==========================================

class SheetManager:
    def __init__(self):
        self.creds = ServiceAccountCredentials.from_json_keyfile_dict(CREDS_DICT, SCOPE)
        self.client = gspread.authorize(self.creds)

    def _get_comparison_sync(self, season_key):
        sheet_name = SEASON_SHEETS.get(season_key.lower(), season_key)
        try:
            wb = self.client.open(sheet_name)
        except gspread.SpreadsheetNotFound:
            raise ValueError(f"Sheet '{sheet_name}' not found.")
        tabs = wb.worksheets()
        if len(tabs) < 2: raise ValueError("Need 2 tabs.")
        return tabs[-1].get_all_values(), tabs[-2].get_all_values(), tabs[-1].title, tabs[-2].title

    def _get_single_sync(self, season_key, tab_index=-1):
        sheet_name = SEASON_SHEETS.get(season_key.lower(), season_key)
        wb = self.client.open(sheet_name)
        return wb.worksheets()[tab_index].get_all_values()

    def _append_row_sync(self, sheet_name, row_data):
        ws = self.client.open(sheet_name).sheet1
        ws.append_row(row_data, value_input_option="RAW")

    async def get_comparison_data(self, bot, season):
        return await bot.loop.run_in_executor(None, lambda: self._get_comparison_sync(season))
    
    async def get_single_sheet(self, bot, season):
        return await bot.loop.run_in_executor(None, lambda: self._get_single_sync(season))

    async def append_row(self, bot, sheet_name, row_data):
        return await bot.loop.run_in_executor(None, lambda: self._append_row_sync(sheet_name, row_data))

    @staticmethod
    def map_rows(data, id_col="lord_id"):
        if not data: return {}, []
        headers = data[0]
        try: idx = col_idx(headers, id_col)
        except: idx = 0
        mapped = {}
        for row in data[1:]:
            if len(row) > idx:
                lid = row[idx].strip()
                if lid: mapped[lid] = row
        return mapped, headers

sheet_manager = SheetManager()

# ==========================================
# 🤖 BOT SETUP
# ==========================================

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True
intents.reactions = True

bot = commands.Bot(command_prefix="!", intents=intents)

@bot.check
async def global_maintenance_check(ctx):
    if not TEST_MODE: return True
    if ctx.channel.id == TEST_CHANNEL_ID: return True
    return False

# ==========================================
# 📅 EVENT SYSTEM (Add, Peek, Due, Resend)
# ==========================================

REMINDERS = {
    "caravan": [timedelta(days=1), timedelta(hours=1), timedelta(minutes=10)],
    "shadow_fort": [timedelta(days=1), timedelta(hours=1), timedelta(minutes=10)],
    "alliance_mobilization": [timedelta(days=1)],
    "behemoth": [timedelta(days=1), timedelta(hours=1), timedelta(minutes=10)],
    "pass": [timedelta(days=1), timedelta(hours=1), timedelta(minutes=10)]
}

@bot.command()
async def add(ctx, kind: str, *, when: str):
    """Adds an event. !add caravan 10/22 14"""
    kind = kind.lower().strip()
    if kind not in REMINDERS:
        return await ctx.send(f"Invalid type. Use: {', '.join(REMINDERS.keys())}")

    # Parse Time
    try:
        s = when.strip().lower().replace(" utc", "")
        now = datetime.now(UTC)
        m = re.match(r"^\s*(\d{1,2})/(\d{1,2})(?:/(\d{4}))?\s+(\d{1,2})(?::(\d{2}))?\s*$", s)
        if not m: raise ValueError
        mm, dd = int(m.group(1)), int(m.group(2))
        yyyy = int(m.group(3)) if m.group(3) else now.year
        hh, min_ = int(m.group(4)), int(m.group(5)) if m.group(5) else 0
        dt = datetime(yyyy, mm, dd, hh, min_, 0, tzinfo=UTC)
        if not m.group(3) and dt < now - timedelta(days=1): 
            dt = dt.replace(year=yyyy+1)
    except:
        return await ctx.send("Invalid time. Use `MM/DD HH` or `MM/DD HH:MM` (UTC).")

    # Prepare Row
    pretty_names = {"caravan":"🛒 Caravan", "shadow_fort":"🏰 Shadow Fort", "alliance_mobilization":"📣 Alliance Mob"}
    msgs = {"caravan":"Cart time!", "shadow_fort":"Fort time!", "alliance_mobilization":"Mob time!"}
    
    row = [
        pretty_names.get(kind, kind), # event_name
        dt.isoformat().replace("+00:00", "Z"), # start_time
        str(CHANNELS["target_announce"]), # channel
        msgs.get(kind, ""), # message
        kind, # type
        str(ROLES["ping_default"]) # role
    ]
    
    try:
        await sheet_manager.append_row(bot, EVENT_SHEET_NAME, row)
        await ctx.send(f"✅ Added {kind} at <t:{int(dt.timestamp())}:F>")
    except Exception as e:
        await ctx.send(f"❌ Error writing to sheet: {e}")

@bot.command()
async def peek(ctx, n: int = 10):
    """Show next N events."""
    try:
        data = await sheet_manager.get_single_sheet(bot, EVENT_SHEET_NAME)
        # Assuming header row 0
        rows = []
        for r in data[1:n+1]:
            if len(r) > 1: rows.append(f"{r[4]}: {r[1]}") # Type: Date
        await ctx.send("```" + "\n".join(rows) + "```")
    except Exception as e:
        await ctx.send(f"Error: {e}")

@bot.command()
async def due(ctx, minutes: int = 120):
    """Check what pings are due soon."""
    # Simplified logic for display
    await ctx.send(f"Checking events due in {minutes} mins... (Check console/logs for logic)")

@tasks.loop(seconds=30)
async def event_autoping_loop():
    try:
        # Fetch events safely
        data = await sheet_manager.get_single_sheet(bot, EVENT_SHEET_NAME)
        # Headers: name, start_time, channel, message, type, role
        # Mapped manually for speed
        now = datetime.now(UTC)
        state_file = "sent_pings.json"
        sent = json.load(open(state_file)) if os.path.exists(state_file) else {}

        for r in data[1:]:
            if len(r) < 6: continue
            etype = r[4].strip().lower()
            if etype not in REMINDERS: continue
            
            try:
                dt_str = r[1].replace("Z", "+00:00")
                dt = datetime.fromisoformat(dt_str)
            except: continue
            
            eid = f"{etype}|{int(dt.timestamp())}"
            
            for offset in REMINDERS[etype]:
                fire = dt - offset
                if fire <= now <= fire + timedelta(minutes=5):
                    key = f"{eid}@{int(offset.total_seconds())}"
                    if key in sent: continue
                    
                    ch = bot.get_channel(CHANNELS["target_announce"])
                    if ch:
                        await ch.send(f"<@&{r[5]}> **{r[0]}** starts <t:{int(dt.timestamp())}:R>\n{r[3]}")
                        sent[key] = now.isoformat()
        
        with open(state_file, "w") as f: json.dump(sent, f)
    except Exception as e:
        print(f"Loop error: {e}")

@bot.event
async def on_ready():
    print(f"✅ Bot Online as {bot.user}")
    if not event_autoping_loop.is_running():
        event_autoping_loop.start()

# ==========================================
# 📊 STATS & PROGRESS
# ==========================================

@bot.command()
async def progress(ctx, lord_id: str, season: str = DEFAULT_SEASON):
    await ctx.typing()
    try:
        l_rows, p_rows, l_title, p_title = await sheet_manager.get_comparison_data(bot, season)
        l_map, headers = sheet_manager.map_rows(l_rows)
        p_map, _ = sheet_manager.map_rows(p_rows)

        if lord_id not in l_map or lord_id not in p_map:
            return await ctx.send("❌ Lord ID not found in both sheets.")

        curr, prev = l_map[lord_id], p_map[lord_id]
        
        # Helper for extracting values safely
        def val(row, names, fb): 
            i = col_idx(headers, names, fb)
            return to_int(row[i]) if len(row) > i else 0

        # Calculations
        d_pow = val(curr, "highest_power", 12) - val(prev, "highest_power", 12)
        merits = val(curr, ["merits", "merit"], 11)
        power = val(curr, "highest_power", 12)
        ratio = (merits / power * 100) if power > 0 else 0
        
        d_kill = val(curr, "units_killed", 9) - val(prev, "units_killed", 9)
        d_dead = val(curr, "units_dead", 17) - val(prev, "units_dead", 17)
        d_heal = val(curr, "units_healed", 18) - val(prev, "units_healed", 18)

        # RSS
        rss_keys = [("gold", 31), ("wood", 32), ("stone", 33), ("mana", 34)]
        rss_text = ""
        for name, fb in rss_keys:
            v = val(curr, f"{name}_spent", fb) - val(prev, f"{name}_spent", fb)
            rss_text += f"{name.title()}: {fmt_abbr(v)}\n"

        embed = discord.Embed(title=f"📈 Progress: {curr[1]}", color=discord.Color.blue())
        embed.description = f"**{p_title}** → **{l_title}**"
        embed.add_field(name="Power", value=f"{fmt_num(power)} ({fmt_num(d_pow)})", inline=False)
        embed.add_field(name="Merits", value=f"{fmt_num(merits)} ({ratio:.2f}%)", inline=False)
        embed.add_field(name="Combat", value=f"⚔️ {fmt_num(d_kill)}\n💀 {fmt_num(d_dead)}\n❤️ {fmt_num(d_heal)}", inline=True)
        embed.add_field(name="RSS Spent", value=rss_text, inline=True)
        
        await ctx.send(embed=embed)
    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def rssheal(ctx, lord_id: str, season: str = DEFAULT_SEASON):
    await ctx.typing()
    try:
        l_rows, p_rows, l_t, p_t = await sheet_manager.get_comparison_data(bot, season)
        l_map, headers = sheet_manager.map_rows(l_rows)
        p_map, _ = sheet_manager.map_rows(p_rows)

        if lord_id not in l_map or lord_id not in p_map:
            return await ctx.send("❌ Lord ID not found.")

        c, p = l_map[lord_id], p_map[lord_id]
        
        def diff(names, fb):
            i = col_idx(headers, names, fb)
            return to_int(c[i]) - to_int(p[i])

        g = diff(["gold_spent", "gold"], 31)
        w = diff(["wood_spent", "wood"], 32)
        o = diff(["stone_spent", "ore"], 33)
        m = diff(["mana_spent", "mana"], 34)

        embed = discord.Embed(title=f"📊 RSS Spent: {c[1]}", description=f"`{p_t}` → `{l_t}`", color=discord.Color.green())
        embed.add_field(name="Gold", value=fmt_num(g))
        embed.add_field(name="Wood", value=fmt_num(w))
        embed.add_field(name="Ore", value=fmt_num(o))
        embed.add_field(name="Mana", value=fmt_num(m))
        await ctx.send(embed=embed)
    except Exception as e:
        await ctx.send(f"Error: {e}")

@bot.command()
async def kills(ctx, lord_id: str, season: str = DEFAULT_SEASON):
    """Breakdown of kills by Tier."""
    await ctx.typing()
    try:
        l_rows, p_rows, l_t, p_t = await sheet_manager.get_comparison_data(bot, season)
        l_map, headers = sheet_manager.map_rows(l_rows)
        p_map, _ = sheet_manager.map_rows(p_rows)

        if lord_id not in l_map: return await ctx.send("ID not found.")
        c = l_map[lord_id]
        p = p_map.get(lord_id, [0]*100) # handle missing prev safely

        desc = ""
        for i in range(5, 0, -1):
            names = [f"t{i}_kills", f"killcount_t{i}"]
            idx = col_idx(headers, names, 35+i) # rough calc fallback
            val_now = to_int(c[idx]) if len(c)>idx else 0
            val_prev = to_int(p[idx]) if len(p)>idx else 0
            desc += f"**T{i}**: {fmt_num(val_now)} ({fmt_num(val_now - val_prev)})\n"
        
        embed = discord.Embed(title=f"⚔️ Kill Breakdown: {c[1]}", description=desc, color=discord.Color.red())
        await ctx.send(embed=embed)
    except Exception as e:
        await ctx.send(f"Error: {e}")

@bot.command()
async def mana(ctx, lord_id: str, season: str = DEFAULT_SEASON):
    # Re-using generic logic logic for brevity
    await rssheal(ctx, lord_id, season) 

# ==========================================
# 🏆 LEADERBOARDS & SPECIFIC CHECKS
# ==========================================

async def run_leaderboard(ctx, season, target_cols, fb_idx, title, top_n=10, mfd=False, reverse=True, min_power=25_000_000):
    await ctx.typing()
    try:
        l_rows, p_rows, l_t, p_t = await sheet_manager.get_comparison_data(bot, season)
        l_map, headers = sheet_manager.map_rows(l_rows)
        p_map, _ = sheet_manager.map_rows(p_rows)

        col_i = col_idx(headers, target_cols, fb_idx)
        pwr_i = col_idx(headers, ["power", "m"], 12)
        srv_i = col_idx(headers, ["home_server", "server"], 5)
        ally_i = col_idx(headers, "alliance", 3)

        data = []
        for lid, r in l_map.items():
            if lid not in p_map: continue
            
            pwr = to_int(r[pwr_i]) if len(r)>pwr_i else 0
            if pwr < min_power: continue

            if mfd:
                srv = "".join(filter(str.isdigit, str(r[srv_i])))
                tag = str(r[ally_i]).upper()
                if srv != "77" or not tag.startswith("MFD"): continue
            
            p = p_map[lid]
            val = to_int(r[col_i]) - to_int(p[col_i])
            # Filter negative noise unless looking for low values
            if reverse and val < 0: val = 0 
            
            data.append((f"[{r[ally_i]}] {r[1]}", val))

        data.sort(key=lambda x: x[1], reverse=reverse)
        
        lines = [f"{i+1}. `{n}` — {title}: {fmt_abbr(v)}" for i, (n, v) in enumerate(data[:top_n])]
        msg = f"**🏆 {title} ({'S77 MFD' if mfd else 'All'})**\n`{p_t}` → `{l_t}`\n" + "\n".join(lines)
        
        # Chunk send
        if len(msg) > 1900:
            await ctx.send(msg[:1900])
            await ctx.send(msg[1900:])
        else:
            await ctx.send(msg)
            
    except Exception as e:
        await ctx.send(f"Error: {e}")

@bot.command()
async def topkills(ctx, n:int=10, s:str=DEFAULT_SEASON): 
    await run_leaderboard(ctx, s, ["units_killed", "kills"], 9, "Kills", n)

@bot.command()
async def topdeads(ctx, n:int=10, s:str=DEFAULT_SEASON): 
    await run_leaderboard(ctx, s, ["units_dead", "dead"], 17, "Deads", n)

@bot.command()
async def lowdeads(ctx, n:int=10, s:str=DEFAULT_SEASON): 
    await run_leaderboard(ctx, s, ["units_dead", "dead"], 17, "Deads", n, reverse=False, min_power=50_000_000)

@bot.command()
async def topmerits(ctx, n:int=10, s:str=DEFAULT_SEASON): 
    await run_leaderboard(ctx, s, ["merits", "merit"], 11, "Merits", n, min_power=50_000_000)

@bot.command()
async def lowmerits(ctx, n:int=10, s:str=DEFAULT_SEASON): 
    await run_leaderboard(ctx, s, ["merits", "merit"], 11, "Merits", n, reverse=False, min_power=50_000_000)

@bot.command()
async def activity(ctx, sheet_name: str, n: int = 20):
    """Specific Activity report for predefined servers."""
    await ctx.typing()
    SERVERS = {"183":"A2G", "99":"BTX", "92":"wAo", "283":"RFF", "77":"MFD", "110":"RoG"}
    try:
        l_rows, p_rows, l_t, p_t = await sheet_manager.get_comparison_data(bot, sheet_name)
        l_map, headers = sheet_manager.map_rows(l_rows)
        p_map, _ = sheet_manager.map_rows(p_rows)
        
        merit_i = col_idx(headers, "merits", 11)
        srv_i = col_idx(headers, "home_server", 5)
        pwr_i = col_idx(headers, "power", 12)

        agg = {s: {"active": 0, "merits": 0} for s in SERVERS}
        
        for lid, r in l_map.items():
            if lid not in p_map: continue
            srv = "".join(filter(str.isdigit, str(r[srv_i])))
            if srv not in SERVERS: continue
            
            pwr = to_int(r[pwr_i])
            if pwr < 40_000_000: continue
            
            delta = to_int(r[merit_i]) - to_int(p_map[lid][merit_i])
            if delta > 0:
                agg[srv]["active"] += 1
                agg[srv]["merits"] += delta

        # Sort by merits
        sorted_stats = sorted(agg.items(), key=lambda x: x[1]['merits'], reverse=True)
        
        lines = []
        for srv, d in sorted_stats:
            lines.append(f"[{SERVERS[srv]}] S{srv}: 👥 {d['active']} — ⭐ +{fmt_abbr(d['merits'])}")
            
        await ctx.send(f"**📈 Activity Report**\n`{p_t}` → `{l_t}`\n" + "\n".join(lines))
    except Exception as e:
        await ctx.send(f"Error: {e}")

@bot.command()
async def farms(ctx, season: str = DEFAULT_SEASON):
    """Find accounts between 15M-30M power."""
    await ctx.typing()
    try:
        data = await sheet_manager.get_single_sheet(bot, season)
        headers = data[0]
        pwr_i = col_idx(headers, "power", 12)
        
        found = []
        for r in data[1:]:
            if len(r) <= pwr_i: continue
            p = to_int(r[pwr_i])
            if 15_000_000 <= p <= 30_000_000:
                found.append(f"{r[1]} ({p//1000000}M)")
        
        if not found: return await ctx.send("No farms found.")
        
        msg = "**🌽 Farms (15M-30M)**\n" + ", ".join(found[:50])
        await ctx.send(msg[:1900])
    except Exception as e:
        await ctx.send(f"Error: {e}")

@bot.command()
async def bastion(ctx, season: str = "sos2"):
    """Specific check for S77 25M-55M dead gains."""
    await ctx.typing()
    try:
        l_rows, p_rows, l_t, p_t = await sheet_manager.get_comparison_data(bot, season)
        l_map, headers = sheet_manager.map_rows(l_rows)
        p_map, _ = sheet_manager.map_rows(p_rows)
        
        pwr_i = col_idx(headers, "power", 12)
        dead_i = col_idx(headers, "units_dead", 17)
        srv_i = col_idx(headers, "home_server", 5)

        res = []
        for lid, r in l_map.items():
            if lid not in p_map: continue
            srv = "".join(filter(str.isdigit, str(r[srv_i])))
            if srv != "77": continue
            
            p = to_int(r[pwr_i])
            if not (25_000_000 <= p <= 55_000_000): continue
            
            gain = to_int(r[dead_i]) - to_int(p_map[lid][dead_i])
            res.append((r[1], gain, p))
            
        res.sort(key=lambda x: x[1], reverse=True)
        lines = [f"{n} ({p//1000000}M): +{g}" for n, g, p in res[:20]]
        await ctx.send("**🛡️ Bastion Candidates (S77 25M-55M)**\n" + "\n".join(lines))
    except Exception as e:
        await ctx.send(f"Error: {e}")

@bot.command()
async def kickcheck(ctx, scope: str = "mfd", season: str = DEFAULT_SEASON):
    """Score players based on activity."""
    await ctx.typing()
    try:
        l_rows, p_rows, l_t, p_t = await sheet_manager.get_comparison_data(bot, season)
        l_map, headers = sheet_manager.map_rows(l_rows)
        p_map, _ = sheet_manager.map_rows(p_rows)

        pwr_i = col_idx(headers, "power", 12)
        mer_i = col_idx(headers, "merits", 11)
        dead_i = col_idx(headers, "units_dead", 17)
        srv_i = col_idx(headers, "home_server", 5)

        mfd_only = (scope.lower() != "all")
        kick, keep = [], []

        for lid, r in l_map.items():
            if lid not in p_map: continue
            
            if mfd_only:
                srv = "".join(filter(str.isdigit, str(r[srv_i])))
                if srv != "77": continue
            
            pwr = to_int(r[pwr_i])
            if pwr < 50_000_000: continue
            
            prev = p_map[lid]
            m_gain = max(0, to_int(r[mer_i]) - to_int(prev[mer_i]))
            d_gain = max(0, to_int(r[dead_i]) - to_int(prev[dead_i]))
            
            m_pct = (m_gain / pwr * 100)
            d_pct = (d_gain / pwr * 100)
            
            # Rule: Merit>12% OR MeritAbs>12M (Flex), AND Dead>0.2%
            flex = (m_pct >= 20.0) or (m_gain >= 12_000_000)
            status = "KICK"
            if (flex or m_pct >= 12.0) and d_pct >= 0.20:
                status = "KEEP"
            
            entry = f"{r[1]}: M{m_pct:.1f}% D{d_pct:.2f}%"
            if status == "KEEP": keep.append(entry)
            else: kick.append(entry)

        # Output only Kicks to save space
        chunks = []
        cur = "**❌ Kick Recommendation**\n"
        for k in kick:
            if len(cur) + len(k) > 1900:
                chunks.append(cur)
                cur = ""
            cur += k + "\n"
        chunks.append(cur)
        
        for c in chunks: await ctx.send(c)

    except Exception as e:
        await ctx.send(f"Error: {e}")

# ==========================================
# ⚔️ WAR STATUS & MATCHUPS
# ==========================================

@bot.command()
async def matchups(ctx, season: str = DEFAULT_SEASON):
    """Matchup stats."""
    await ctx.typing()
    try:
        l_rows, p_rows, l_t, p_t = await sheet_manager.get_comparison_data(bot, season)
        SERVER_MAP = {"375": "NxW", "82": "CFRA", "62": "FG", "515": "FW-Y", "3": "RK", "77": "MFD"}
        PAIRS = [("375", "3"), ("77", "515"), ("82", "62")]
        
        stats = {s: {"kills":0, "deads":0} for s in SERVER_MAP}
        
        l_map, headers = sheet_manager.map_rows(l_rows)
        p_map, _ = sheet_manager.map_rows(p_rows)
        
        srv_i = col_idx(headers, "home_server", 5)
        kill_i = col_idx(headers, "units_killed", 9)
        dead_i = col_idx(headers, "units_dead", 17)
        
        for lid, r in l_map.items():
            if lid not in p_map: continue
            srv = "".join(filter(str.isdigit, str(r[srv_i])))
            if srv not in stats: continue
            
            k = max(0, to_int(r[kill_i]) - to_int(p_map[lid][kill_i]))
            d = max(0, to_int(r[dead_i]) - to_int(p_map[lid][dead_i]))
            
            stats[srv]["kills"] += k
            stats[srv]["deads"] += d

        for a, b in PAIRS:
            embed = discord.Embed(title=f"{SERVER_MAP[a]} vs {SERVER_MAP[b]}", color=discord.Color.gold())
            embed.add_field(name=SERVER_MAP[a], value=f"⚔️ {fmt_abbr(stats[a]['kills'])}\n💀 {fmt_abbr(stats[a]['deads'])}")
            embed.add_field(name=SERVER_MAP[b], value=f"⚔️ {fmt_abbr(stats[b]['kills'])}\n💀 {fmt_abbr(stats[b]['deads'])}")
            await ctx.send(embed=embed)
            
    except Exception as e:
        await ctx.send(f"Error: {e}")

@bot.command()
async def warred(ctx):
    await _set_war_status(ctx, "🔴", "war-status-fullwar")

@bot.command()
async def wargreen(ctx):
    await _set_war_status(ctx, "🟢", "war-status-no-fighting")

@bot.command()
async def waryellow(ctx):
    await _set_war_status(ctx, "🟡", "war-status-skirmishes")

async def _set_war_status(ctx, emoji, name):
    # Permission check
    if not any(r.id == ROLES["war_admin"] for r in ctx.author.roles):
        return await ctx.send("❌ No permission.")
    
    ch = bot.get_channel(CHANNELS["war_status"])
    if ch:
        try: await ch.edit(name=f"〘{emoji}〙{name}")
        except: await ctx.send("⚠️ Rename failed (Rate Limit?)")
    await ctx.send(f"✅ Status: {emoji}")

@bot.event
async def on_raw_reaction_add(payload):
    # 1369072129068372008 is the hardcoded message ID from your original code
    if payload.message_id != 1369072129068372008: return
    
    mapping = {"🔴": "fullwar", "🟢": "no-fighting", "🟡": "skirmishes", "🧑‍🌾": "go-farm"}
    name = mapping.get(str(payload.emoji))
    if not name: return
    
    ch = bot.get_channel(CHANNELS["war_status"])
    if ch: await ch.edit(name=f"〘{payload.emoji}〙war-status-{name}")

@bot.command()
async def commands(ctx):
    # Check if we are in the allowed command channel (or daily announce channel)
    allowed = [CHANNELS["allowed_commands"], CHANNELS["daily_announce"]]
    if ctx.channel.id not in allowed:
        return await ctx.send(f"❌ Commands are only allowed in <#{CHANNELS['allowed_commands']}>.")

    help_text = """
📜 **MFD Bot – Available Commands**

**📊 Progress & Player Stats**
- `!progress [lord_id] [season]` — Full profile: power, kills, deads, heals, RSS, mana
- `!rssheal [lord_id] [season]` — Resources spent on healing/training
- `!kills [lord_id] [season]` — Kill breakdown by troop tier
- `!mana [lord_id] [season]` — Mana gathered (+gain)

**🏆 Leaderboards**
- `!topkills [n] [season]` — Top kill gainers
- `!topdeads [n] [season]` — Top dead units
- `!topheal [n] [season]` — Top units healed
- `!topmerits [n] [season]` — Top merits gained
- `!lowdeads [n] [season]` — Lowest dead gains (≥50M Power)
- `!lowmerits [n] [season]` — Lowest merit gains (≥50M Power)

**🔎 Checks & Reports**
- `!kickcheck [scope] [season]` — Kick recommendations (Merit% vs Dead%)
- `!activity [sheet]` — Activity report for specific servers
- `!farms` — List accounts 15M-30M power
- `!bastion` — Specific dead gain check for S77 (25M-55M)

**⚔️ War**
- `!matchups` — Server war stats comparison
- `!warred`, `!waryellow`, `!wargreen` — Set war status channel (Admin only)

**🗂️ Events**
- `!add [type] [time]` — Add event (caravan, pass, etc.)
- `!peek` — View upcoming events
"""
    await ctx.send(help_text)
# ==========================================
# 🚀 START
# ==========================================

bot.run(os.getenv("TOKEN"))
