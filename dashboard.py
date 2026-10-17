"""
NVR Alliance Dashboard
======================
Two messages:
  • Dashboard (!setup_dashboard) – overview for everyone, grouped by ability. Alerts are posted here too.
  • Officer panel (!setup_panel) – one button per shell. Tap the shell → tap what happened.

Life cycle of every ability:
  🔴 on cooldown  →  🟡 needs donations  →  (💰 Donated)  →  🟢 ready to use  →  (used)  →  🔴 …
  Build Buff is 🟠 active for its first hour, and switches ⚡ on in the shell builds channel.

What happens automatically:
  • ⏰ heads-up 15 min before something comes off cooldown, then a 🟡 "needs donations" alert
  • Build Buff turns ⚡ on in the shell builds channel and off again when the hour is over
  • Dashboard + officer panel stay at the bottom of their channels
  • Who pressed what is saved (📜 Log) and shown on each shell card
  • Everything is saved on the /data volume, so deploys don't reset timers
"""
import asyncio
import json
import os
import re
import shutil
import time

import discord
from discord.ext import commands, tasks
from discord.ui import Button, Modal, TextInput, View

# ── CONFIGURATION ────────────────────────────────────────────
ALLIANCES = [
    "[NVR!] Main", "[NVR-] Shell", "[NVR2] Shell", "[NVR3] Shell", "[NVR4] Shell",
    "[NVR5] Shell", "[NVR6] Shell", "[MFD] Shell", "[NxW] Shell", "[UHA] Shell",
    "[NOIR] Shell", "[-GG-] Shell",
]

# key → (emoji, name, cooldown in seconds, "used" button text)
ABILITIES = {
    "bastion":    ("🏰", "Bastion",        12 * 3600, "Bastion removed"),
    "build_buff": ("🔨", "Build Buff",     72 * 3600, "Build Buff used"),
    "storm":      ("⚡", "Storm Vanguard", 36 * 3600, "Storm used"),
    "fog":        ("🌫️", "Fog of War",     48 * 3600, "Fog used"),
}
# Which abilities have to be donated to after the cooldown before they can be used again
NEEDS_DONATION = {"bastion": True, "build_buff": True, "storm": True, "fog": True}
BUILD_BUFF_ACTIVE = 1 * 3600         # build buff is active for the first hour of its 72h cooldown

LOG_CHANNEL_ID = 1527938722987900978  # also post every click here (None = off)
ALERT_CHANNEL_ID = None              # where alerts go (None = the dashboard channel)
ALERT_ROLE_ID = None                 # ⬅️ role to ping with alerts, e.g. your R4/officer role (None = no ping)
HEADS_UP_BEFORE = 15 * 60            # ⏰ heads-up this long before a cooldown ends (None = off)
HEADS_UP_DELETE_AFTER = 20 * 60      # heads-up messages delete themselves after 20 min
ALERT_DELETE_AFTER = 3 * 3600        # "needs donations" / "build buff over" alerts delete after 3h (None = keep)
ALERT_BUILD_BUFF_END = True          # announce when a build buff's active hour is over
OFFICER_ROLE_IDS = []                # only these roles may use the panel ([] = anyone who can see it)
STICKY = True                        # keep dashboard + panel as the last message in their channels
STICKY_DELAY = 5                     # seconds of quiet before they move down
HISTORY_KEEP = 500                   # how many log entries to keep

_DATA_DIR = os.getenv("DATA_DIR") or ("/data" if os.path.isdir("/data") else ".")
DATA_FILE = os.path.join(_DATA_DIR, "dashboard_data.json")
_OLD_DATA_FILE = "dashboard_data.json"   # where older versions saved it (lost on every deploy)

DASH_TITLE = "🛡️ NVR ALLIANCE DASHBOARD"
PANEL_TITLE = "🛠️ OFFICER PANEL"


# ── DATA ─────────────────────────────────────────────────────
def load_data():
    if not os.path.exists(DATA_FILE) and os.path.exists(_OLD_DATA_FILE) \
            and os.path.abspath(_OLD_DATA_FILE) != os.path.abspath(DATA_FILE):
        shutil.copy(_OLD_DATA_FILE, DATA_FILE)   # one-time move onto the volume
    data = {}
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            data = {}
    for k in ("dashboard_channel_id", "dashboard_message_id", "panel_channel_id", "panel_message_id"):
        data.setdefault(k, None)
    data.setdefault("undo", {})          # str(user_id) → last action, for Undo
    for k in ("cooldowns", "donated", "alerted", "last"):
        data.setdefault(k, {})
        for a in ALLIANCES:
            data[k].setdefault(a, {})
    data.setdefault("history", [])
    return data


def save_data(data):
    tmp = DATA_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, DATA_FILE)


def tag(alliance):
    m = re.match(r"\[(.+?)\]", alliance)
    return m.group(1) if m else alliance


def by_tag(t):
    return next((a for a in ALLIANCES if tag(a) == t), None)


def _end(db, alliance, key):
    return int(db["cooldowns"].get(alliance, {}).get(key, 0) or 0)


def state(db, alliance, key, now=None):
    """('cooldown'|'active'|'donate'|'ready', timestamp)"""
    now = now or int(time.time())
    end = _end(db, alliance, key)
    if end > now:
        if key == "build_buff":
            active_end = end - ABILITIES["build_buff"][2] + BUILD_BUFF_ACTIVE
            if now < active_end:
                return "active", active_end
        return "cooldown", end
    if not NEEDS_DONATION.get(key, False):
        return "ready", 0
    donated = int(db["donated"].get(alliance, {}).get(key, 0) or 0)
    return ("ready", 0) if donated else ("donate", 0)


def status_text(db, alliance, key):
    s, t = state(db, alliance, key)
    return {
        "ready": "🟢 **Ready to use**",
        "donate": "🟡 **Needs donations**",
        "active": f"🟠 **Active** · ends <t:{t}:R>",
        "cooldown": f"🔴 Off cooldown <t:{t}:R>",
    }[s]


def parse_duration(text):
    """'5h30m', '90m', '2d', '1h 20m', '1:30', '3' (= hours), '0' → seconds. None if unreadable."""
    t = text.strip().lower().replace(" ", "")
    if t in ("0", "ready", "now", "r"):
        return 0
    m = re.fullmatch(r"(\d+):(\d{1,2})", t)
    if m:
        return int(m.group(1)) * 3600 + int(m.group(2)) * 60
    if re.fullmatch(r"\d+([.,]\d+)?", t):
        return int(float(t.replace(",", ".")) * 3600)
    parts = re.findall(r"(\d+(?:[.,]\d+)?)(d|h|m|min|s)", t)
    if not parts or "".join(n + u for n, u in parts) != t:
        return None
    mult = {"d": 86400, "h": 3600, "m": 60, "min": 60, "s": 1}
    return int(sum(float(n.replace(",", ".")) * mult[u] for n, u in parts))


def fmt_duration(seconds):
    h, m = divmod(max(seconds, 0) // 60, 60)
    return f"{h}h{m:02d}m" if h else f"{m}m"


# ── EMBEDS ───────────────────────────────────────────────────
def generate_dashboard_embed(db):
    now = int(time.time())
    embed = discord.Embed(title=DASH_TITLE, color=discord.Color.gold())

    active = [(tag(a), state(db, a, "build_buff", now)[1]) for a in ALLIANCES
              if state(db, a, "build_buff", now)[0] == "active"]
    if active:
        embed.description = ("🔨 **BUILD BUFF ACTIVE:** "
                             + " · ".join(f"`{t}` ends <t:{e}:R>" for t, e in active) + "\n​")
    else:
        embed.description = "*No build buff active right now.*\n​"

    for key, (emoji, name, cd, _) in ABILITIES.items():
        groups = {"ready": [], "donate": []}
        busy = []
        for a in ALLIANCES:
            s, t = state(db, a, key, now)
            if s in groups:
                groups[s].append(f"`{tag(a)}`")
            elif s == "active":
                busy.append((t, f"🟠 `{tag(a)}` active, ends <t:{t}:R>"))
            else:
                busy.append((t, f"🔴 `{tag(a)}` <t:{t}:R>"))
        lines = []
        if groups["ready"]:
            lines.append("🟢 " + " ".join(groups["ready"]))
        if groups["donate"]:
            lines.append("🟡 " + " ".join(groups["donate"]))
        lines += [line for _, line in sorted(busy)]
        embed.add_field(name=f"{emoji} {name} · {cd // 3600}h", value="\n".join(lines)[:1024] or "–",
                        inline=False)

    embed.set_footer(text="🟢 ready to use · 🟡 needs donations · 🔴 cooldown · 🟠 active")
    embed.timestamp = discord.utils.utcnow()
    return embed


def panel_embed():
    e = discord.Embed(
        title=PANEL_TITLE,
        description=("**Tap the shell → tap what happened.** That's it.\n"
                     "💰 **Donated** when a skill is funded · **…used** when it was used.\n"
                     "The dashboard updates for everyone right away.\n\n"
                     "Forgot to press it in time? Open the shell → **✏️ Fix timers**."),
        color=discord.Color.gold(),
    )
    e.set_footer(text="⏰ The dashboard channel gets a heads-up 15 min before something is off cooldown.")
    return e


def _last_line(db, alliance, key):
    last = db["last"].get(alliance, {}).get(key)
    if not last:
        return ""
    return f"\n　╰ *{last['what']} by **{last['by']}** <t:{last['t']}:R>*"


def shell_embed(db, alliance, header=None):
    e = discord.Embed(title=f"🛡️ {alliance}", color=discord.Color.gold())
    lines = [f"{emoji} **{name}** — {status_text(db, alliance, key)}{_last_line(db, alliance, key)}"
             for key, (emoji, name, _, _) in ABILITIES.items()]
    e.description = ((header + "\n\n") if header else "") + "\n".join(lines)
    e.set_footer(text="Row 1: used · Row 2: donated · greyed out = not possible right now")
    return e


def log_embed(db, alliance=None, limit=20):
    entries = [h for h in db["history"] if not alliance or h["a"] == alliance][-limit:]
    title = f"📜 Log · {tag(alliance)}" if alliance else "📜 Log · all shells"
    e = discord.Embed(title=title, color=discord.Color.dark_gold())
    if entries:
        e.description = "\n".join(
            f"<t:{h['t']}:t> **{h['by']}** · {h['what']}" + ("" if alliance else f" · `{tag(h['a'])}`")
            for h in reversed(entries))[:4096]
    else:
        e.description = "*Nothing logged yet.*"
    e.set_footer(text=f"Last {len(entries)} actions · newest first")
    return e


# ── VIEWS ────────────────────────────────────────────────────
class OfficerPanel(View):
    """Persistent panel: one button per shell + overview + log."""

    def __init__(self, cog):
        super().__init__(timeout=None)
        self.cog = cog
        per_row = 4
        for i, a in enumerate(ALLIANCES[: per_row * 4]):
            b = Button(label=tag(a), style=discord.ButtonStyle.primary if "Main" in a
                       else discord.ButtonStyle.secondary,
                       custom_id=f"dash:shell:{tag(a)}", row=i // per_row)
            b.callback = self._open(a)
            self.add_item(b)
        ov = Button(label="Overview", emoji="📋", style=discord.ButtonStyle.success,
                    custom_id="dash:overview", row=4)
        ov.callback = self._overview
        lg = Button(label="Log", emoji="📜", style=discord.ButtonStyle.secondary,
                    custom_id="dash:log", row=4)
        lg.callback = self._log
        self.add_item(ov)
        self.add_item(lg)

    def _open(self, alliance):
        async def cb(interaction: discord.Interaction):
            if not self.cog.allowed(interaction.user):
                return await interaction.response.send_message("❌ Only officers can use this panel.",
                                                               ephemeral=True)
            await interaction.response.send_message(
                embed=shell_embed(self.cog.db, alliance),
                view=ShellCard(self.cog, alliance, interaction.user.id), ephemeral=True)
        return cb

    async def _overview(self, interaction: discord.Interaction):
        await interaction.response.send_message(embed=generate_dashboard_embed(self.cog.db), ephemeral=True)

    async def _log(self, interaction: discord.Interaction):
        await interaction.response.send_message(embed=log_embed(self.cog.db), ephemeral=True)


class ShellCard(View):
    """Private card for one shell: used buttons, donated buttons, undo / fix / log / reset.
    The buttons only carry IDs – clicks are handled by AllianceDashboard.on_interaction,
    so cards keep working even after the bot restarts or redeploys."""

    def __init__(self, cog, alliance, user_id):
        super().__init__(timeout=1)
        t = tag(alliance)
        db = cog.db
        for key, (emoji, name, cd, label) in ABILITIES.items():
            s, _ = state(db, alliance, key)
            self.add_item(Button(
                label=label, emoji=emoji, row=0, custom_id=f"dashc:use:{t}:{key}",
                style=discord.ButtonStyle.success if s == "ready" else discord.ButtonStyle.secondary,
                disabled=s not in ("ready", "donate")))
        for key, (emoji, name, cd, label) in ABILITIES.items():
            s, _ = state(db, alliance, key)
            short = label.rsplit(" ", 1)[0]          # "Bastion", "Build Buff", "Storm", "Fog"
            self.add_item(Button(
                label=f"{short} donated", emoji="💰", row=1, custom_id=f"dashc:don:{t}:{key}",
                style=discord.ButtonStyle.primary if s == "donate" else discord.ButtonStyle.secondary,
                disabled=(s != "donate")))
        last = cog.db["undo"].get(str(user_id))
        self.add_item(Button(label="Undo", emoji="↩️", style=discord.ButtonStyle.secondary, row=2,
                             custom_id=f"dashc:undo:{t}", disabled=not (last and last[0] == alliance)))
        self.add_item(Button(label="Fix timers", emoji="✏️", style=discord.ButtonStyle.secondary, row=2,
                             custom_id=f"dashc:fix:{t}"))
        self.add_item(Button(label="Log", emoji="📜", style=discord.ButtonStyle.secondary, row=2,
                             custom_id=f"dashc:log:{t}"))
        self.add_item(Button(label="Reset shell", emoji="🔄", style=discord.ButtonStyle.danger, row=2,
                             custom_id=f"dashc:reset:{t}"))


def reset_confirm_view(alliance):
    t = tag(alliance)
    v = View(timeout=1)
    v.add_item(Button(label=f"Yes, reset all timers for {t}", emoji="🔄", style=discord.ButtonStyle.danger,
                      custom_id=f"dashc:resetyes:{t}"))
    v.add_item(Button(label="Cancel", style=discord.ButtonStyle.secondary, custom_id=f"dashc:cancel:{t}"))
    return v


class FixTimersModal(Modal):
    """For when someone forgot to press: enter the time LEFT for each ability."""

    def __init__(self, cog, alliance):
        super().__init__(title=f"Fix timers · {tag(alliance)}"[:45])
        self.cog, self.alliance = cog, alliance
        self.fields, self.defaults = {}, {}
        now = int(time.time())
        for key, (emoji, name, cd, _) in ABILITIES.items():
            left = max(_end(cog.db, alliance, key) - now, 0)
            label = f"{name} · time left" + (" (cooldown, max 72h)" if key == "build_buff" else "")
            shown = fmt_duration(left) if left else ""
            field = TextInput(label=label[:45], required=False, max_length=12,
                              placeholder="e.g. 5h30m · 0 or empty = off cooldown",
                              default=shown or None)
            self.fields[key], self.defaults[key] = field, shown
            self.add_item(field)

    async def on_submit(self, interaction: discord.Interaction):
        now = int(time.time())
        changes, errors = [], []
        for key, field in self.fields.items():
            raw = (field.value or "").strip()
            emoji, name, cd, _ = ABILITIES[key]
            if raw == self.defaults[key]:
                continue                      # left as it was
            secs = parse_duration(raw or "0")
            if secs is None:
                errors.append(f"❌ {name}: couldn't read `{raw}` (try `5h30m`)")
                continue
            if secs > cd:
                errors.append(f"❌ {name}: `{raw}` is longer than its {cd // 3600}h cooldown")
                continue
            new_end = now + secs if secs else 0
            what = f"✏️ {name} timer fixed to " + (fmt_duration(secs) if secs else "0")
            await self.cog.record(interaction.user, self.alliance, key, what, cooldown=new_end,
                                  donated=0 if secs else None, refresh=False)
            changes.append(f"{emoji} {name} → " + (f"off cooldown <t:{new_end}:R>" if new_end
                                                    else "off cooldown now"))
        if changes:
            save_data(self.cog.db)
            await self.cog.after_change()
        header = "\n".join((["✏️ **Timers updated:**"] + changes if changes else ["ℹ️ Nothing changed."]) + errors)
        await interaction.response.send_message(
            embed=shell_embed(self.cog.db, self.alliance, header),
            view=ShellCard(self.cog, self.alliance, interaction.user.id), ephemeral=True)


class LegacyPanel(View):
    """Catches clicks on the very old panel (dropdown + buttons) and turns it into the new panel."""

    def __init__(self, cog):
        super().__init__(timeout=None)
        self.cog = cog
        sel = discord.ui.Select(custom_id="select_alliance", placeholder="old panel",
                                options=[discord.SelectOption(label="old", value="old")])
        sel.callback = self._upgrade
        self.add_item(sel)
        for cid in ("btn_bastion", "btn_build", "btn_storm", "btn_fog", "btn_undo", "btn_reset_shell"):
            b = Button(label=cid, custom_id=cid)
            b.callback = self._upgrade
            self.add_item(b)

    async def _upgrade(self, interaction: discord.Interaction):
        await interaction.response.edit_message(embed=panel_embed(), view=OfficerPanel(self.cog))
        self.cog.db["panel_channel_id"] = interaction.channel_id
        self.cog.db["panel_message_id"] = interaction.message.id
        save_data(self.cog.db)
        await interaction.followup.send("✨ The panel was upgraded – tap a shell to continue.", ephemeral=True)


# ── COG ──────────────────────────────────────────────────────
class AllianceDashboard(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = load_data()
        self._sticky_tasks = {}
        self._sticky_lock = asyncio.Lock()
        self._mark_expired_as_alerted()

    async def cog_load(self):
        self.bot.add_view(OfficerPanel(self))   # buttons keep working after restarts
        self.bot.add_view(LegacyPanel(self))
        self.watch.start()

    async def cog_unload(self):
        self.watch.cancel()

    # ── helpers ──
    def allowed(self, member):
        if not OFFICER_ROLE_IDS:
            return True
        if getattr(member, "guild_permissions", None) and member.guild_permissions.administrator:
            return True
        return any(r.id in OFFICER_ROLE_IDS for r in getattr(member, "roles", []))

    def _mark_expired_as_alerted(self):
        """Don't spam alerts for timers that ran out while the bot was offline / before this version."""
        now = int(time.time())
        for a in ALLIANCES:
            al = self.db["alerted"][a]
            for key in ABILITIES:
                end = _end(self.db, a, key)
                if 0 < end <= now:
                    al[key] = end
                if 0 < end <= now + (HEADS_UP_BEFORE or 0):
                    al[key + "_soon"] = end
            end = _end(self.db, a, "build_buff")
            if end and state(self.db, a, "build_buff", now)[0] != "active":
                al["build_buff_active"] = end

    async def log(self, text):
        if LOG_CHANNEL_ID:
            ch = self.bot.get_channel(LOG_CHANNEL_ID)
            if ch:
                try:
                    await ch.send(text, allowed_mentions=discord.AllowedMentions.none())
                except discord.HTTPException:
                    pass

    def _history(self, user, alliance, key, what):
        now = int(time.time())
        name = getattr(user, "display_name", str(user))
        self.db["history"].append({"t": now, "by": name, "a": alliance, "k": key, "what": what})
        del self.db["history"][:-HISTORY_KEEP]
        self.db["last"][alliance][key] = {"t": now, "by": name, "what": what}

    async def record(self, user, alliance, key, what, cooldown=None, donated=None, refresh=True):
        """Change a timer and/or donation flag, remember it for undo, log it."""
        cds, dons = self.db["cooldowns"][alliance], self.db["donated"][alliance]
        self.db["undo"][str(user.id)] = [alliance, key, cds.get(key, 0), dons.get(key, 0),
                                         self.db["last"][alliance].get(key), what]
        if cooldown is not None:
            cds[key] = int(cooldown)
        if donated is not None:
            dons[key] = int(donated)
        self._history(user, alliance, key, what)
        await self.log(f"**{user.display_name}** · {what} · **{alliance}**")
        if refresh:
            save_data(self.db)
            await self.after_change()

    async def undo(self, user):
        last = self.db["undo"].pop(str(user.id), None)
        if not last:
            return "❌ Nothing to undo."
        alliance, key, prev_cd, prev_don, prev_last, what = last
        self.db["cooldowns"][alliance][key] = prev_cd
        self.db["donated"][alliance][key] = prev_don
        self.db["history"].append({"t": int(time.time()), "by": user.display_name, "a": alliance,
                                   "k": key, "what": f"↩️ undid: {what}"})
        if prev_last:
            self.db["last"][alliance][key] = prev_last
        else:
            self.db["last"][alliance].pop(key, None)
        save_data(self.db)
        await self.after_change()
        await self.log(f"**{user.display_name}** · ↩️ undid: {what} · **{alliance}**")
        return f"↩️ Undid: {what}"

    async def reset_shell(self, user, alliance):
        self.db["cooldowns"][alliance] = {}
        self.db["donated"][alliance] = {}
        self.db["undo"].pop(str(user.id), None)
        self._history(user, alliance, "_reset", "🔄 reset all timers")
        save_data(self.db)
        await self.after_change()
        await self.log(f"**{user.display_name}** · 🔄 reset all timers · **{alliance}**")

    async def after_change(self):
        await self.refresh_dashboard()
        await self.sync_shell_buff()

    async def refresh_dashboard(self):
        ch_id, msg_id = self.db.get("dashboard_channel_id"), self.db.get("dashboard_message_id")
        if not (ch_id and msg_id):
            return
        try:
            await self.bot.get_partial_messageable(ch_id).get_partial_message(msg_id).edit(
                embed=generate_dashboard_embed(self.db))
        except discord.NotFound:
            await self._repost("dashboard")
        except discord.HTTPException:
            pass

    async def sync_shell_buff(self):
        """Tell the shell builds channel which shells have an active build buff (⚡ on/off)."""
        hook = getattr(self.bot, "dashboard_build_buff", None)
        if not hook:
            return
        now = int(time.time())
        active = {tag(a): state(self.db, a, "build_buff", now)[1] for a in ALLIANCES
                  if state(self.db, a, "build_buff", now)[0] == "active"}
        try:
            await hook(active)
        except Exception as e:
            print(f"[dashboard] couldn't update the shell builds channel: {e!r}")

    async def _send_alert(self, lines, delete_after):
        ch_id = ALERT_CHANNEL_ID or self.db.get("dashboard_channel_id") or self.db.get("panel_channel_id")
        if not ch_id or not lines:
            return
        ping = f"<@&{ALERT_ROLE_ID}>\n" if ALERT_ROLE_ID else ""
        try:
            await self.bot.get_partial_messageable(ch_id).send(
                ping + "\n".join(lines)[:1900], delete_after=delete_after,
                allowed_mentions=discord.AllowedMentions(
                    roles=[discord.Object(ALERT_ROLE_ID)] if ALERT_ROLE_ID else False))
        except discord.HTTPException:
            pass

    # ── background: heads-up, turn things yellow/green, alerts ──
    @tasks.loop(seconds=30)
    async def watch(self):
        now = int(time.time())
        soon, over = [], []
        for a in ALLIANCES:
            al = self.db["alerted"].setdefault(a, {})
            for key, (emoji, name, _, _) in ABILITIES.items():
                end = _end(self.db, a, key)
                donate = NEEDS_DONATION.get(key, False)
                if HEADS_UP_BEFORE and now < end <= now + HEADS_UP_BEFORE and al.get(key + "_soon") != end:
                    al[key + "_soon"] = end
                    todo = "get donations ready!" if donate else "get ready!"
                    soon.append(f"⏰ {emoji} **{name}** · `{tag(a)}` off cooldown <t:{end}:R> – {todo}")
                if 0 < end <= now and al.get(key) != end:
                    al[key] = end
                    al[key + "_soon"] = end
                    if donate:
                        over.append(f"🟡 {emoji} **{name}** · `{tag(a)}` is off cooldown – **needs donations!**")
                    else:
                        over.append(f"🟢 {emoji} **{name}** · `{tag(a)}` is ready again")
            end = _end(self.db, a, "build_buff")
            if end > now and state(self.db, a, "build_buff", now)[0] == "cooldown" \
                    and al.get("build_buff_active") != end:
                al["build_buff_active"] = end
                if ALERT_BUILD_BUFF_END:
                    over.append(f"🔨 **Build Buff** is over · `{tag(a)}` (next one <t:{end}:R>)")
                await self.sync_shell_buff()
        if not (soon or over):
            return
        save_data(self.db)
        await self.refresh_dashboard()
        await self._send_alert(soon, HEADS_UP_DELETE_AFTER)
        await self._send_alert(over, ALERT_DELETE_AFTER)

    @watch.before_loop
    async def _before_watch(self):
        await self.bot.wait_until_ready()
        await self.sync_shell_buff()

    # ── shell card clicks (handled here so they survive restarts) ──
    async def _card_refresh(self, interaction, alliance, header):
        await interaction.response.edit_message(embed=shell_embed(self.db, alliance, header),
                                                view=ShellCard(self, alliance, interaction.user.id))

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if interaction.type != discord.InteractionType.component:
            return
        cid = (interaction.data or {}).get("custom_id", "")
        if not cid.startswith("dashc:"):
            return
        parts = cid.split(":")
        action, t = parts[1], parts[2]
        key = parts[3] if len(parts) > 3 else None
        alliance = by_tag(t)
        user = interaction.user
        if alliance is None:
            return await interaction.response.send_message("❌ That shell isn't on the dashboard anymore.",
                                                           ephemeral=True)
        if not self.allowed(user):
            return await interaction.response.send_message("❌ Only officers can use this.", ephemeral=True)

        if action == "use" and key in ABILITIES:
            emoji, name, cd, label = ABILITIES[key]
            if state(self.db, alliance, key)[0] not in ("ready", "donate"):
                return await self._card_refresh(
                    interaction, alliance, f"⚠️ **{name}** is already running – someone else just pressed it.")
            ready_at = int(time.time()) + cd
            await self.record(user, alliance, key, f"{emoji} {label}", cooldown=ready_at, donated=0)
            extra = (f"Active for 1h (⚡ is on in the shell builds channel), then on cooldown until "
                     f"<t:{ready_at}:f>." if key == "build_buff" else f"Off cooldown <t:{ready_at}:R>.")
            return await self._card_refresh(interaction, alliance, f"✅ {emoji} **{name}** used. {extra}")

        if action == "don" and key in ABILITIES:
            emoji, name, _, _ = ABILITIES[key]
            if state(self.db, alliance, key)[0] != "donate":
                return await self._card_refresh(interaction, alliance,
                                                f"ℹ️ **{name}** doesn't need donations right now.")
            await self.record(user, alliance, key, f"💰 {name} donated", donated=int(time.time()))
            return await self._card_refresh(interaction, alliance, f"💰 **{name}** donated – it's 🟢 ready to use.")

        if action == "undo":
            return await self._card_refresh(interaction, alliance, await self.undo(user))
        if action == "fix":
            return await interaction.response.send_modal(FixTimersModal(self, alliance))
        if action == "log":
            return await interaction.response.send_message(embed=log_embed(self.db, alliance), ephemeral=True)
        if action == "reset":
            return await interaction.response.edit_message(
                embed=shell_embed(self.db, alliance, f"⚠️ Reset **all** timers for **{t}**?"),
                view=reset_confirm_view(alliance))
        if action == "resetyes":
            await self.reset_shell(user, alliance)
            return await self._card_refresh(
                interaction, alliance, "🔄 All timers reset – everything is off cooldown and 🟡 needs donations.")
        if action == "cancel":
            return await self._card_refresh(interaction, alliance, None)

    # ── sticky dashboard + panel ──
    def _sticky_cfg(self, which):
        if which == "dashboard":
            return "dashboard_channel_id", "dashboard_message_id", lambda: {
                "embed": generate_dashboard_embed(self.db)}
        return "panel_channel_id", "panel_message_id", lambda: {
            "embed": panel_embed(), "view": OfficerPanel(self)}

    async def _repost(self, which):
        ch_key, msg_key, content = self._sticky_cfg(which)
        async with self._sticky_lock:
            ch_id, old_id = self.db.get(ch_key), self.db.get(msg_key)
            channel = self.bot.get_channel(ch_id) if ch_id else None
            if not channel or (old_id and channel.last_message_id == old_id):
                return
            try:
                new = await channel.send(**content())
            except discord.HTTPException:
                return
            self.db[msg_key] = new.id
            save_data(self.db)
            if old_id:
                try:
                    await channel.get_partial_message(old_id).delete()
                except discord.HTTPException:
                    pass

    async def _sticky_later(self, which):
        await asyncio.sleep(STICKY_DELAY)
        await self._repost(which)

    @commands.Cog.listener()
    async def on_message(self, message):
        if not STICKY:
            return
        for which in ("dashboard", "panel"):
            ch_key, msg_key, _ = self._sticky_cfg(which)
            if message.channel.id != self.db.get(ch_key) or message.id == self.db.get(msg_key):
                continue
            if message.author.id == self.bot.user.id and message.embeds \
                    and message.embeds[0].title in (DASH_TITLE, PANEL_TITLE):
                continue
            task = self._sticky_tasks.get(which)
            if task and not task.done():
                task.cancel()
            self._sticky_tasks[which] = asyncio.create_task(self._sticky_later(which))

    # ── setup commands ──
    async def _replace(self, ctx, ch_key, msg_key, **send_kwargs):
        old_ch, old_msg = self.db.get(ch_key), self.db.get(msg_key)
        try:
            await ctx.message.delete()
        except discord.HTTPException:
            pass
        msg = await ctx.send(**send_kwargs)
        self.db[ch_key], self.db[msg_key] = msg.channel.id, msg.id
        save_data(self.db)
        if old_ch and old_msg and old_msg != msg.id:
            try:
                await self.bot.get_partial_messageable(old_ch).get_partial_message(old_msg).delete()
            except discord.HTTPException:
                pass

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setup_dashboard(self, ctx):
        """Run this in #alliance-dashboard to post the overview."""
        await self._replace(ctx, "dashboard_channel_id", "dashboard_message_id",
                            embed=generate_dashboard_embed(self.db))

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setup_panel(self, ctx):
        """Run this in #officer-control-panel to post the officer buttons."""
        await self._replace(ctx, "panel_channel_id", "panel_message_id",
                            embed=panel_embed(), view=OfficerPanel(self))


async def setup(bot):
    await bot.add_cog(AllianceDashboard(bot))
