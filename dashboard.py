"""
NVR Alliance Dashboard
======================
Two messages:
  • Dashboard (!setup_dashboard) – read-only overview for everyone, grouped by ability:
    which shells have Bastion / Build Buff / Storm / Fog ready right now.
  • Officer panel (!setup_panel) – one button per shell. Tap the shell → tap what was used.

What happens automatically:
  • Cooldowns turn 🟢 on the dashboard by themselves when they run out.
  • The bot posts a 🟢 "ready again" alert (optionally pinging a role).
  • The officer panel stays at the bottom of its channel.
  • Everything is saved on the /data volume, so deploys don't reset timers.
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

# key → (emoji, name, cooldown in seconds, button text)
ABILITIES = {
    "bastion":    ("🏰", "Bastion",        12 * 3600, "Bastion removed"),
    "build_buff": ("🔨", "Build Buff",     72 * 3600, "Build Buff used"),
    "storm":      ("⚡", "Storm Vanguard", 36 * 3600, "Storm used"),
    "fog":        ("🌫️", "Fog of War",     48 * 3600, "Fog used"),
}
BUILD_BUFF_ACTIVE = 1 * 3600         # build buff is active for the first hour of its 72h cooldown

LOG_CHANNEL_ID = 1527938722987900978  # who pressed what (None = off)
ALERT_CHANNEL_ID = None              # where "🟢 ready again" alerts go (None = the officer panel channel)
ALERT_ROLE_ID = None                 # role to ping with alerts, e.g. your officer role (None = no ping)
ALERT_DELETE_AFTER = 6 * 3600        # alerts delete themselves after 6h (None = keep)
ALERT_BUILD_BUFF_END = True          # also announce when a build buff's active hour is over
OFFICER_ROLE_IDS = []                # only these roles may use the panel ([] = anyone who can see it)
STICKY_PANEL = True                  # keep the officer panel as the last message in its channel
STICKY_DELAY = 5                     # seconds of quiet before the panel moves down

_DATA_DIR = os.getenv("DATA_DIR") or ("/data" if os.path.isdir("/data") else ".")
DATA_FILE = os.path.join(_DATA_DIR, "dashboard_data.json")
_OLD_DATA_FILE = "dashboard_data.json"   # where older versions saved it (lost on every deploy)


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
    data.setdefault("dashboard_channel_id", None)
    data.setdefault("dashboard_message_id", None)
    data.setdefault("panel_channel_id", None)
    data.setdefault("panel_message_id", None)
    data.setdefault("cooldowns", {})
    data.setdefault("alerted", {})
    for a in ALLIANCES:
        data["cooldowns"].setdefault(a, {})
        data["alerted"].setdefault(a, {})
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


def state(db, alliance, key, now=None):
    """('ready'|'active'|'cooldown', end_timestamp)"""
    now = now or int(time.time())
    end = int(db["cooldowns"].get(alliance, {}).get(key, 0) or 0)
    if end <= now:
        return "ready", 0
    if key == "build_buff":
        active_end = end - ABILITIES["build_buff"][2] + BUILD_BUFF_ACTIVE
        if now < active_end:
            return "active", active_end
    return "cooldown", end


def status_text(db, alliance, key):
    s, t = state(db, alliance, key)
    if s == "ready":
        return "🟢 **Ready**"
    if s == "active":
        return f"🟠 **Active** · ends <t:{t}:R>"
    return f"🔴 Ready <t:{t}:R>"


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
    return f"{h}h {m:02d}m" if h else f"{m}m"


# ── EMBEDS ───────────────────────────────────────────────────
def generate_dashboard_embed(db):
    now = int(time.time())
    embed = discord.Embed(title="🛡️ NVR ALLIANCE DASHBOARD", color=discord.Color.gold())

    active = [(tag(a), state(db, a, "build_buff", now)[1]) for a in ALLIANCES
              if state(db, a, "build_buff", now)[0] == "active"]
    top = []
    if active:
        top.append("🔨 **BUILD BUFF ACTIVE:** " + " · ".join(f"`{t}` ends <t:{e}:R>" for t, e in active))
    else:
        top.append("*No build buff active right now.*")
    embed.description = "\n".join(top) + "\n​"

    for key, (emoji, name, cd, _) in ABILITIES.items():
        ready, busy = [], []
        for a in ALLIANCES:
            s, t = state(db, a, key, now)
            if s == "ready":
                ready.append(f"`{tag(a)}`")
            elif s == "active":
                busy.append((t, f"🟠 `{tag(a)}` active, ends <t:{t}:R>"))
            else:
                busy.append((t, f"🔴 `{tag(a)}` <t:{t}:R>"))
        lines = []
        if ready:
            lines.append("🟢 " + " ".join(ready))
        for _, line in sorted(busy):
            lines.append(line)
        embed.add_field(name=f"{emoji} {name} · {cd // 3600}h", value="\n".join(lines)[:1024] or "–",
                        inline=False)

    embed.set_footer(text="🟢 ready · 🔴 on cooldown · 🟠 active · turns green by itself")
    embed.timestamp = discord.utils.utcnow()
    return embed


def panel_embed():
    e = discord.Embed(
        title="🛠️ OFFICER PANEL",
        description=("**Tap the shell → tap what was used.** That's it.\n"
                     "The timer starts right away and the dashboard updates for everyone.\n\n"
                     "Forgot to press it in time? Open the shell and use **✏️ Fix timers** "
                     "to enter how much time is left."),
        color=discord.Color.gold(),
    )
    e.set_footer(text="You'll get a 🟢 alert when something is ready again.")
    return e


def shell_embed(db, alliance, header=None):
    e = discord.Embed(title=f"🛡️ {alliance}", color=discord.Color.gold())
    lines = [f"{emoji} **{name}** — {status_text(db, alliance, key)}"
             for key, (emoji, name, _, _) in ABILITIES.items()]
    e.description = ((header + "\n\n") if header else "") + "\n".join(lines)
    e.set_footer(text="Tap what was used · greyed out = still on cooldown")
    return e


# ── VIEWS ────────────────────────────────────────────────────
class OfficerPanel(View):
    """Persistent panel: one button per shell + an overview button."""

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
        self.add_item(ov)

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


class ShellCard(View):
    """Private card for one shell: the 4 ability buttons + undo / fix / reset."""

    def __init__(self, cog, alliance, user_id, undo_ready=False):
        super().__init__(timeout=900)
        self.cog, self.alliance, self.user_id = cog, alliance, user_id
        db = cog.db
        for key, (emoji, name, cd, label) in ABILITIES.items():
            s, _ = state(db, alliance, key)
            b = Button(label=label, emoji=emoji, row=0,
                       style=discord.ButtonStyle.success if s == "ready" else discord.ButtonStyle.secondary,
                       disabled=(s != "ready"))
            b.callback = self._use(key)
            self.add_item(b)

        last = cog.last_action.get(user_id)
        undo = Button(label="Undo", emoji="↩️", style=discord.ButtonStyle.secondary, row=1,
                      disabled=not (last and last[0] == alliance))
        undo.callback = self._undo
        fix = Button(label="Fix timers", emoji="✏️", style=discord.ButtonStyle.secondary, row=1)
        fix.callback = self._fix
        reset = Button(label="Reset shell", emoji="🔄", style=discord.ButtonStyle.danger, row=1)
        reset.callback = self._reset
        for b in (undo, fix, reset):
            self.add_item(b)

    def _use(self, key):
        async def cb(interaction: discord.Interaction):
            emoji, name, cd, _ = ABILITIES[key]
            if state(self.cog.db, self.alliance, key)[0] != "ready":
                return await interaction.response.edit_message(
                    embed=shell_embed(self.cog.db, self.alliance,
                                      f"⚠️ **{name}** is already running – someone else just pressed it."),
                    view=ShellCard(self.cog, self.alliance, self.user_id))
            ready_at = int(time.time()) + cd
            await self.cog.set_timer(interaction.user, self.alliance, key, ready_at, f"{emoji} **{name}**")
            extra = (f"Active for 1h, then on cooldown until <t:{ready_at}:f>."
                     if key == "build_buff" else f"Ready again <t:{ready_at}:R>.")
            await interaction.response.edit_message(
                embed=shell_embed(self.cog.db, self.alliance, f"✅ {emoji} **{name}** started. {extra}"),
                view=ShellCard(self.cog, self.alliance, self.user_id))
        return cb

    async def _undo(self, interaction: discord.Interaction):
        msg = await self.cog.undo(interaction.user)
        await interaction.response.edit_message(
            embed=shell_embed(self.cog.db, self.alliance, msg),
            view=ShellCard(self.cog, self.alliance, self.user_id))

    async def _fix(self, interaction: discord.Interaction):
        await interaction.response.send_modal(FixTimersModal(self.cog, self.alliance))

    async def _reset(self, interaction: discord.Interaction):
        confirm = View(timeout=60)
        yes = Button(label=f"Yes, reset all timers for {tag(self.alliance)}", emoji="🔄",
                     style=discord.ButtonStyle.danger)
        no = Button(label="Cancel", style=discord.ButtonStyle.secondary)

        async def do_reset(i: discord.Interaction):
            await self.cog.reset_shell(i.user, self.alliance)
            await i.response.edit_message(
                embed=shell_embed(self.cog.db, self.alliance, "🔄 All timers reset – everything is ready."),
                view=ShellCard(self.cog, self.alliance, self.user_id))

        async def cancel(i: discord.Interaction):
            await i.response.edit_message(embed=shell_embed(self.cog.db, self.alliance),
                                          view=ShellCard(self.cog, self.alliance, self.user_id))
        yes.callback, no.callback = do_reset, cancel
        confirm.add_item(yes)
        confirm.add_item(no)
        await interaction.response.edit_message(
            embed=shell_embed(self.cog.db, self.alliance,
                              f"⚠️ Reset **all** timers for **{tag(self.alliance)}**?"), view=confirm)


class FixTimersModal(Modal):
    """For when someone forgot to press: enter the time LEFT for each ability."""

    def __init__(self, cog, alliance):
        super().__init__(title=f"Fix timers · {tag(alliance)}"[:45])
        self.cog, self.alliance = cog, alliance
        self.fields, self.defaults = {}, {}
        now = int(time.time())
        for key, (emoji, name, cd, _) in ABILITIES.items():
            end = int(cog.db["cooldowns"].get(alliance, {}).get(key, 0) or 0)
            left = max(end - now, 0)
            label = f"{name} · time left" + (" (cooldown, max 72h)" if key == "build_buff" else "")
            shown = fmt_duration(left).replace(" ", "") if left else ""
            field = TextInput(label=label[:45], required=False, max_length=12,
                              placeholder="e.g. 5h30m · 0 = ready · empty = ready",
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
            if not raw:
                raw = "0"                     # cleared a pre-filled value → ready
            secs = parse_duration(raw)
            if secs is None:
                errors.append(f"❌ {name}: couldn't read `{raw}` (try `5h30m`)")
                continue
            if secs > cd:
                errors.append(f"❌ {name}: `{raw}` is longer than its {cd // 3600}h cooldown")
                continue
            new_end = now + secs if secs else 0
            await self.cog.set_timer(interaction.user, self.alliance, key, new_end,
                                     f"{emoji} **{name}**", fixed=True, save=False)
            changes.append(f"{emoji} {name} → " + (f"ready <t:{new_end}:R>" if new_end else "🟢 ready"))
        if changes:
            save_data(self.cog.db)
            await self.cog.refresh_dashboard()
        header = "\n".join((["✏️ **Timers updated:**"] + changes if changes else ["ℹ️ Nothing changed."]) + errors)
        await interaction.response.send_message(
            embed=shell_embed(self.cog.db, self.alliance, header),
            view=ShellCard(self.cog, self.alliance, interaction.user.id), ephemeral=True)


class LegacyPanel(View):
    """Catches clicks on the OLD panel (dropdown + buttons) and turns that message into the new panel."""

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
        self.last_action = {}          # user_id → (alliance, key, previous_end, label)
        self._sticky_task = None
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
            for key in ABILITIES:
                end = int(self.db["cooldowns"][a].get(key, 0) or 0)
                if 0 < end <= now:
                    self.db["alerted"][a][key] = end
            s, _ = state(self.db, a, "build_buff", now)
            end = int(self.db["cooldowns"][a].get("build_buff", 0) or 0)
            if end and s != "active":
                self.db["alerted"][a]["build_buff_active"] = end

    async def log(self, text):
        if LOG_CHANNEL_ID:
            ch = self.bot.get_channel(LOG_CHANNEL_ID)
            if ch:
                try:
                    await ch.send(text, allowed_mentions=discord.AllowedMentions.none())
                except discord.HTTPException:
                    pass

    async def set_timer(self, user, alliance, key, new_end, label, fixed=False, save=True):
        timers = self.db["cooldowns"].setdefault(alliance, {})
        self.last_action[user.id] = (alliance, key, int(timers.get(key, 0) or 0), label)
        timers[key] = int(new_end)
        if save:
            save_data(self.db)
            await self.refresh_dashboard()
        verb = "fixed the timer of" if fixed else "used"
        await self.log(f"✅ **{user.display_name}** {verb} {label} for **{alliance}**.")

    async def undo(self, user):
        last = self.last_action.pop(user.id, None)
        if not last:
            return "❌ Nothing to undo."
        alliance, key, prev, label = last
        self.db["cooldowns"][alliance][key] = prev
        save_data(self.db)
        await self.refresh_dashboard()
        await self.log(f"↩️ **{user.display_name}** undid {label} for **{alliance}**.")
        return f"↩️ Undid {label}."

    async def reset_shell(self, user, alliance):
        self.db["cooldowns"][alliance] = {}
        self.last_action.pop(user.id, None)
        save_data(self.db)
        await self.refresh_dashboard()
        await self.log(f"🔄 **{user.display_name}** reset all timers for **{alliance}**.")

    async def refresh_dashboard(self):
        ch_id, msg_id = self.db.get("dashboard_channel_id"), self.db.get("dashboard_message_id")
        if not (ch_id and msg_id):
            return
        try:
            await self.bot.get_partial_messageable(ch_id).get_partial_message(msg_id).edit(
                embed=generate_dashboard_embed(self.db))
        except discord.HTTPException:
            pass

    # ── background: turn things green + alert ──
    @tasks.loop(seconds=30)
    async def watch(self):
        now = int(time.time())
        alerts = []
        for a in ALLIANCES:
            timers, alerted = self.db["cooldowns"].get(a, {}), self.db["alerted"].setdefault(a, {})
            for key, (emoji, name, _, _) in ABILITIES.items():
                end = int(timers.get(key, 0) or 0)
                if 0 < end <= now and alerted.get(key) != end:
                    alerted[key] = end
                    alerts.append(f"🟢 {emoji} **{name}** is ready again · `{tag(a)}`")
            end = int(timers.get("build_buff", 0) or 0)
            if end > now and state(self.db, a, "build_buff", now)[0] == "cooldown" \
                    and alerted.get("build_buff_active") != end:
                alerted["build_buff_active"] = end
                if ALERT_BUILD_BUFF_END:
                    alerts.append(f"🔨 **Build Buff** is over · `{tag(a)}` (next one <t:{end}:R>)")
        if not alerts:
            return
        save_data(self.db)
        await self.refresh_dashboard()
        ch_id = ALERT_CHANNEL_ID or self.db.get("panel_channel_id")
        if ch_id:
            ping = f"<@&{ALERT_ROLE_ID}> " if ALERT_ROLE_ID else ""
            try:
                await self.bot.get_partial_messageable(ch_id).send(
                    ping + "\n".join(alerts)[:1900], delete_after=ALERT_DELETE_AFTER,
                    allowed_mentions=discord.AllowedMentions(
                        roles=[discord.Object(ALERT_ROLE_ID)] if ALERT_ROLE_ID else False))
            except discord.HTTPException:
                pass

    @watch.before_loop
    async def _before_watch(self):
        await self.bot.wait_until_ready()

    # ── sticky panel ──
    async def _repost_panel(self):
        async with self._sticky_lock:
            ch_id, old_id = self.db.get("panel_channel_id"), self.db.get("panel_message_id")
            channel = self.bot.get_channel(ch_id) if ch_id else None
            if not channel or (old_id and channel.last_message_id == old_id):
                return
            try:
                new = await channel.send(embed=panel_embed(), view=OfficerPanel(self))
            except discord.HTTPException:
                return
            self.db["panel_message_id"] = new.id
            save_data(self.db)
            if old_id:
                try:
                    await channel.get_partial_message(old_id).delete()
                except discord.HTTPException:
                    pass

    async def _sticky_later(self):
        await asyncio.sleep(STICKY_DELAY)
        await self._repost_panel()

    @commands.Cog.listener()
    async def on_message(self, message):
        if not STICKY_PANEL or message.channel.id != self.db.get("panel_channel_id"):
            return
        if message.id == self.db.get("panel_message_id"):
            return
        if message.author.id == self.bot.user.id and message.embeds \
                and message.embeds[0].title == "🛠️ OFFICER PANEL":
            return
        if self._sticky_task and not self._sticky_task.done():
            self._sticky_task.cancel()
        self._sticky_task = asyncio.create_task(self._sticky_later())

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
        """Run this in #alliance-dashboard to post the read-only overview."""
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
