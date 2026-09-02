"""
activity_checks.py — DM-based activity checks (RSVP) with buttons + live dashboard.

Drop this file next to your spy detector cog and load it like any other cog:
    await bot.load_extension("activity_checks")

Requires: discord.py 2.x, members intent + message content intent (you already have both).
"""

import asyncio
import csv
import io
import sqlite3
from datetime import datetime, timezone

import discord
from discord.ext import commands

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

import os
DB_PATH = os.getenv("ACTIVITY_DB_PATH", "/data/activity_checks.db")

# Where the live dashboard message gets posted (same log channel you already use)
DASHBOARD_CHANNEL_ID = 1544402126539591753

# Roles you are allowed to send activity checks to (same OPSEC guard as your broadcasts)
ALLOWED_ROLES = ["Test", "NVR Member"]

# Add the per-user OPSEC fingerprint to activity check DMs too?
# Set to False if you don't care about leak-tracing these messages.
USE_OPSEC_WATERMARK = True

# Change "spydetector" to whatever your existing file/module is actually called.
try:
    from spydetector import generate_signoff, encode_watermark  # noqa
    OPSEC_AVAILABLE = True
except Exception:
    OPSEC_AVAILABLE = False

STATUS_META = {
    "yes":       ("✅", "Attending"),
    "no":        ("❌", "Not attending"),
    "maybe":     ("❔", "Maybe"),
    "pending":   ("⏳", "No response yet"),
    "dm_failed": ("🚫", "DMs closed"),
}


# ---------------------------------------------------------------------------
# DATABASE
# ---------------------------------------------------------------------------

class ActivityDB:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._setup()

    def _setup(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS events (
                event_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id   INTEGER NOT NULL,
                role_id    INTEGER,
                role_name  TEXT,
                title      TEXT NOT NULL,
                description TEXT,
                created_by INTEGER,
                created_at TEXT,
                dash_channel_id INTEGER,
                dash_message_id INTEGER,
                closed     INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS targets (
                event_id      INTEGER NOT NULL,
                user_id       INTEGER NOT NULL,
                user_name     TEXT,
                dm_message_id INTEGER,
                status        TEXT DEFAULT 'pending',
                responded_at  TEXT,
                PRIMARY KEY (event_id, user_id)
            );

            CREATE INDEX IF NOT EXISTS idx_targets_dm
                ON targets (dm_message_id);
        """)
        self.conn.commit()

    # --- events -----------------------------------------------------------
    def create_event(self, guild_id, role_id, role_name, title, description, author_id):
        cur = self.conn.execute(
            "INSERT INTO events (guild_id, role_id, role_name, title, description, created_by, created_at)"
            " VALUES (?,?,?,?,?,?,?)",
            (guild_id, role_id, role_name, title, description, author_id,
             datetime.now(timezone.utc).isoformat(timespec="seconds")),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_event(self, event_id):
        return self.conn.execute(
            "SELECT * FROM events WHERE event_id=?", (event_id,)
        ).fetchone()

    def set_dashboard(self, event_id, channel_id, message_id):
        self.conn.execute(
            "UPDATE events SET dash_channel_id=?, dash_message_id=? WHERE event_id=?",
            (channel_id, message_id, event_id),
        )
        self.conn.commit()

    def close_event(self, event_id):
        self.conn.execute("UPDATE events SET closed=1 WHERE event_id=?", (event_id,))
        self.conn.commit()

    def open_events(self, guild_id):
        return self.conn.execute(
            "SELECT * FROM events WHERE guild_id=? AND closed=0 ORDER BY event_id DESC",
            (guild_id,),
        ).fetchall()

    # --- targets ----------------------------------------------------------
    def add_target(self, event_id, user_id, user_name, dm_message_id, status="pending"):
        self.conn.execute(
            "INSERT OR REPLACE INTO targets (event_id, user_id, user_name, dm_message_id, status)"
            " VALUES (?,?,?,?,?)",
            (event_id, user_id, user_name, dm_message_id, status),
        )
        self.conn.commit()

    def update_dm_message(self, event_id, user_id, dm_message_id):
        self.conn.execute(
            "UPDATE targets SET dm_message_id=? WHERE event_id=? AND user_id=?",
            (dm_message_id, event_id, user_id),
        )
        self.conn.commit()

    def target_by_dm(self, dm_message_id):
        return self.conn.execute(
            "SELECT * FROM targets WHERE dm_message_id=?", (dm_message_id,)
        ).fetchone()

    def set_status(self, event_id, user_id, status):
        self.conn.execute(
            "UPDATE targets SET status=?, responded_at=? WHERE event_id=? AND user_id=?",
            (status, datetime.now(timezone.utc).isoformat(timespec="seconds"),
             event_id, user_id),
        )
        self.conn.commit()

    def targets(self, event_id, status=None):
        if status:
            return self.conn.execute(
                "SELECT * FROM targets WHERE event_id=? AND status=? ORDER BY user_name",
                (event_id, status),
            ).fetchall()
        return self.conn.execute(
            "SELECT * FROM targets WHERE event_id=? ORDER BY user_name", (event_id,)
        ).fetchall()

    def counts(self, event_id):
        rows = self.conn.execute(
            "SELECT status, COUNT(*) AS n FROM targets WHERE event_id=? GROUP BY status",
            (event_id,),
        ).fetchall()
        out = {k: 0 for k in STATUS_META}
        for r in rows:
            out[r["status"]] = r["n"]
        return out


# ---------------------------------------------------------------------------
# THE BUTTONS (persistent view — survives bot restarts)
# ---------------------------------------------------------------------------

class ActivityView(discord.ui.View):
    def __init__(self, cog: "ActivityChecks"):
        super().__init__(timeout=None)   # timeout=None + fixed custom_id = persistent
        self.cog = cog

    @discord.ui.button(label="I'm in", emoji="✅",
                       style=discord.ButtonStyle.success,
                       custom_id="activitycheck:yes")
    async def yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_response(interaction, "yes")

    @discord.ui.button(label="Can't make it", emoji="❌",
                       style=discord.ButtonStyle.danger,
                       custom_id="activitycheck:no")
    async def no(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_response(interaction, "no")

    @discord.ui.button(label="Not sure", emoji="❔",
                       style=discord.ButtonStyle.secondary,
                       custom_id="activitycheck:maybe")
    async def maybe(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_response(interaction, "maybe")


# ---------------------------------------------------------------------------
# THE COG
# ---------------------------------------------------------------------------

class ActivityChecks(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = ActivityDB(DB_PATH)
        self._locks = {}   # per-event lock so dashboard edits don't race

    async def cog_load(self):
        # Re-register the button view so clicks still work after a restart
        self.bot.add_view(ActivityView(self))

    def _lock(self, event_id):
        return self._locks.setdefault(event_id, asyncio.Lock())

    # -- message building --------------------------------------------------

    def build_dm_embed(self, event, user_id, status="pending"):
        emoji, label = STATUS_META.get(status, STATUS_META["pending"])
        e = discord.Embed(
            title=f"📋 Activity Check — {event['title']}",
            description=event["description"] or "Are you able to join?",
            colour=discord.Colour.blurple() if status == "pending" else discord.Colour.green(),
        )
        e.add_field(name="Your answer", value=f"{emoji} **{label}**", inline=False)
        e.set_footer(text=f"Check #{event['event_id']} • You can change your answer any time")

        if USE_OPSEC_WATERMARK and OPSEC_AVAILABLE:
            signoff = generate_signoff(user_id, mode="tactical")
            e.description = f"{e.description}\n\n{encode_watermark(signoff, user_id)}"
        return e

    def build_dashboard_embed(self, event, guild):
        c = self.db.counts(event["event_id"])
        total = sum(c.values())
        answered = c["yes"] + c["no"] + c["maybe"]

        e = discord.Embed(
            title=f"📊 Activity Check #{event['event_id']} — {event['title']}",
            description=event["description"] or "",
            colour=discord.Colour.dark_grey() if event["closed"] else discord.Colour.blurple(),
        )

        bar_len = 20
        filled = int(bar_len * answered / total) if total else 0
        e.add_field(
            name="Response rate",
            value=f"`{'█' * filled}{'░' * (bar_len - filled)}` **{answered}/{total}**"
                  f" ({answered / total * 100:.0f}%)" if total else "No targets",
            inline=False,
        )

        e.add_field(name="✅ Attending", value=str(c["yes"]), inline=True)
        e.add_field(name="❌ Not attending", value=str(c["no"]), inline=True)
        e.add_field(name="❔ Maybe", value=str(c["maybe"]), inline=True)

        for status in ("yes", "no", "maybe", "pending", "dm_failed"):
            rows = self.db.targets(event["event_id"], status)
            if not rows:
                continue
            emoji, label = STATUS_META[status]
            e.add_field(
                name=f"{emoji} {label} ({len(rows)})",
                value=self._fmt_names(rows, guild),
                inline=False,
            )

        state = "CLOSED" if event["closed"] else "OPEN"
        e.set_footer(text=f"{state} • {event['role_name'] or 'custom list'} • updated")
        e.timestamp = datetime.now(timezone.utc)
        return e

    @staticmethod
    def _fmt_names(rows, guild):
        names = []
        for r in rows:
            member = guild.get_member(r["user_id"]) if guild else None
            names.append(member.display_name if member else (r["user_name"] or str(r["user_id"])))
        text = ", ".join(names)
        if len(text) > 1000:
            text = text[:990].rsplit(",", 1)[0] + f", … (+{len(names)} total)"
        return text or "—"

    # -- response handling -------------------------------------------------

    async def handle_response(self, interaction: discord.Interaction, answer: str):
        row = self.db.target_by_dm(interaction.message.id)
        if not row:
            await interaction.response.send_message(
                "This activity check is no longer tracked. Please ping an officer.",
            )
            return

        event = self.db.get_event(row["event_id"])
        if event["closed"]:
            await interaction.response.send_message(
                "⛔ This activity check has already been closed.",
            )
            return

        self.db.set_status(event["event_id"], row["user_id"], answer)

        await interaction.response.edit_message(
            embed=self.build_dm_embed(event, row["user_id"], answer),
            view=ActivityView(self),
        )
        await self.refresh_dashboard(event["event_id"])

    async def refresh_dashboard(self, event_id):
        async with self._lock(event_id):
            event = self.db.get_event(event_id)
            if not event or not event["dash_message_id"]:
                return
            channel = self.bot.get_channel(event["dash_channel_id"])
            if channel is None:
                return
            guild = self.bot.get_guild(event["guild_id"])
            try:
                msg = channel.get_partial_message(event["dash_message_id"])
                await msg.edit(embed=self.build_dashboard_embed(event, guild))
            except discord.HTTPException:
                pass

    # -- commands ----------------------------------------------------------

    @commands.command(name="activitycheck", aliases=["ac"])
    @commands.has_permissions(administrator=True)
    async def activitycheck(self, ctx, role: discord.Role, *, text: str):
        """!activitycheck @Role Title | optional details"""
        if role.is_default() or role.name not in ALLOWED_ROLES:
            await ctx.send(
                f"🚨 **OPSEC ALERT:** Activity checks are restricted to: `{', '.join(ALLOWED_ROLES)}`"
            )
            return

        title, _, description = text.partition("|")
        title, description = title.strip(), description.strip()

        if not ctx.guild.chunked:
            await ctx.guild.chunk()

        members = [m for m in role.members if not m.bot]
        if not members:
            await ctx.send("That role has no non-bot members.")
            return

        event_id = self.db.create_event(
            ctx.guild.id, role.id, role.name, title, description, ctx.author.id
        )
        event = self.db.get_event(event_id)

        status_msg = await ctx.send(f"Sending activity check **#{event_id}** to {len(members)} members…")

        sent = failed = 0
        for member in members:
            try:
                dm = await member.send(
                    embed=self.build_dm_embed(event, member.id),
                    view=ActivityView(self),
                )
                self.db.add_target(event_id, member.id, member.name, dm.id, "pending")
                sent += 1
            except discord.Forbidden:
                self.db.add_target(event_id, member.id, member.name, None, "dm_failed")
                failed += 1
            await asyncio.sleep(0.6)   # gentle on the DM rate limit

        dash_channel = self.bot.get_channel(DASHBOARD_CHANNEL_ID) or ctx.channel
        dash = await dash_channel.send(embed=self.build_dashboard_embed(event, ctx.guild))
        self.db.set_dashboard(event_id, dash.channel.id, dash.id)

        await status_msg.edit(
            content=f"✅ Activity check **#{event_id}** sent to **{sent}** members "
                    f"({failed} had DMs closed).\nLive dashboard: {dash.jump_url}"
        )

    @commands.command(name="activitystatus", aliases=["acstatus"])
    @commands.has_permissions(administrator=True)
    async def activitystatus(self, ctx, event_id: int):
        """Post a fresh snapshot of a check."""
        event = self.db.get_event(event_id)
        if not event:
            await ctx.send("No activity check with that ID.")
            return
        await ctx.send(embed=self.build_dashboard_embed(event, ctx.guild))

    @commands.command(name="activitylist", aliases=["aclist"])
    @commands.has_permissions(administrator=True)
    async def activitylist(self, ctx):
        """List all open checks."""
        events = self.db.open_events(ctx.guild.id)
        if not events:
            await ctx.send("No open activity checks.")
            return
        lines = []
        for ev in events:
            c = self.db.counts(ev["event_id"])
            lines.append(
                f"**#{ev['event_id']}** — {ev['title']} "
                f"(✅{c['yes']} ❌{c['no']} ❔{c['maybe']} ⏳{c['pending']})"
            )
        await ctx.send("\n".join(lines))

    @commands.command(name="activitypending", aliases=["acpending"])
    @commands.has_permissions(administrator=True)
    async def activitypending(self, ctx, event_id: int):
        """Print pingable mentions for everyone who hasn't answered."""
        rows = self.db.targets(event_id, "pending")
        failed = self.db.targets(event_id, "dm_failed")
        if not rows and not failed:
            await ctx.send("Everyone has responded. 🎉")
            return
        out = ""
        if rows:
            out += f"**⏳ No response ({len(rows)}):**\n" + " ".join(f"<@{r['user_id']}>" for r in rows)
        if failed:
            out += f"\n\n**🚫 DMs closed ({len(failed)}):**\n" + " ".join(f"<@{r['user_id']}>" for r in failed)
        for chunk in [out[i:i + 1900] for i in range(0, len(out), 1900)]:
            await ctx.send(chunk)

    @commands.command(name="activityremind", aliases=["acremind"])
    @commands.has_permissions(administrator=True)
    async def activityremind(self, ctx, event_id: int):
        """Re-DM everyone who hasn't answered yet."""
        event = self.db.get_event(event_id)
        if not event or event["closed"]:
            await ctx.send("That check doesn't exist or is closed.")
            return

        pending = self.db.targets(event_id, "pending")
        if not pending:
            await ctx.send("Nobody left to remind.")
            return

        sent = 0
        for row in pending:
            member = ctx.guild.get_member(row["user_id"])
            if member is None:
                continue
            try:
                dm = await member.send(
                    content="⏰ **Reminder** — we still need your answer:",
                    embed=self.build_dm_embed(event, member.id),
                    view=ActivityView(self),
                )
                self.db.update_dm_message(event_id, member.id, dm.id)
                sent += 1
            except discord.Forbidden:
                self.db.set_status(event_id, member.id, "dm_failed")
            await asyncio.sleep(0.6)

        await self.refresh_dashboard(event_id)
        await ctx.send(f"Reminder sent to **{sent}** members.")

    @commands.command(name="activityclose", aliases=["acclose"])
    @commands.has_permissions(administrator=True)
    async def activityclose(self, ctx, event_id: int):
        """Close a check — buttons stop accepting answers."""
        event = self.db.get_event(event_id)
        if not event:
            await ctx.send("No activity check with that ID.")
            return
        self.db.close_event(event_id)
        await self.refresh_dashboard(event_id)
        c = self.db.counts(event_id)
        await ctx.send(
            f"🔒 Check **#{event_id}** closed. Final: ✅{c['yes']} ❌{c['no']} "
            f"❔{c['maybe']} ⏳{c['pending']} 🚫{c['dm_failed']}"
        )

    @commands.command(name="activityexport", aliases=["acexport"])
    @commands.has_permissions(administrator=True)
    async def activityexport(self, ctx, event_id: int):
        """Export the responses as a CSV."""
        rows = self.db.targets(event_id)
        if not rows:
            await ctx.send("Nothing to export.")
            return
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["user_id", "username", "display_name", "status", "responded_at"])
        for r in rows:
            m = ctx.guild.get_member(r["user_id"])
            w.writerow([r["user_id"], r["user_name"],
                        m.display_name if m else "", r["status"], r["responded_at"] or ""])
        buf.seek(0)
        await ctx.send(file=discord.File(io.BytesIO(buf.getvalue().encode()),
                                         filename=f"activity_check_{event_id}.csv"))

    # -- error handling ----------------------------------------------------

    @activitycheck.error
    @activitystatus.error
    @activityremind.error
    @activityclose.error
    async def _err(self, ctx, error):
        if isinstance(error, commands.MissingPermissions):
            await ctx.send("You need administrator permissions for that.")
        elif isinstance(error, (commands.BadArgument, commands.MissingRequiredArgument)):
            await ctx.send(f"Bad usage: `{error}`")
        else:
            raise error


async def setup(bot):
    await bot.add_cog(ActivityChecks(bot))
