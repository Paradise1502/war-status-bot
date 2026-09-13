"""
OPSEC broadcast + leak attribution cog (discord.py 2.x).

Each recipient of a broadcast gets:
  1. A deterministic "sign-off" phrase, unique within the batch, keyed by a
     server-side secret. This is what survives a screenshot.
  2. A zero-width watermark carrying their user ID plus a checksum, spread
     across word boundaries. This survives copy-paste, not screenshots.

Required environment variables:
  OPSEC_FINGERPRINT_SECRET   long random string; the whole system's security
  OPSEC_LOG_CHANNEL_ID       channel ID for audit logs
  OPSEC_ROLES_TACTICAL       comma-separated role IDs allowed for !warbroadcast
  OPSEC_ROLES_ROW            comma-separated role IDs allowed for !rowbroadcast
  OPSEC_ROLES_CASUAL         comma-separated role IDs allowed for !socialbroadcast

Anyone who can read the log channel, or who holds the secret, can forge or
decode every fingerprint. Restrict both accordingly.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import io
import os
import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Iterable, Sequence, Union

import discord
from discord.ext import commands

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

FINGERPRINT_SECRET = os.environ.get("OPSEC_FINGERPRINT_SECRET", "").encode()


def _env_ids(name: str) -> frozenset[int]:
    raw = os.environ.get(name, "")
    return frozenset(int(part) for part in re.findall(r"\d+", raw))


LOG_CHANNEL_ID = int(os.environ.get("OPSEC_LOG_CHANNEL_ID", "0") or 0)

ALLOWED_ROLE_IDS: dict[str, frozenset[int]] = {
    "tactical": _env_ids("OPSEC_ROLES_TACTICAL"),
    "row": _env_ids("OPSEC_ROLES_ROW"),
    "casual": _env_ids("OPSEC_ROLES_CASUAL"),
}

DM_DELAY_SECONDS = 1.0          # pacing between DMs; do not set to 0
MAX_MESSAGE_LENGTH = 2000       # Discord hard limit, watermark included
MAX_VARIANTS = 4                # re-rolls available to break a phrase collision
PROGRESS_EVERY = 10             # edit the status message every N sends
MATCH_THRESHOLD = 0.75          # fraction of phrases needed to report a match
SHORTLIST_THRESHOLD = 0.25      # min exact score to qualify for the fuzzy pass
FUZZY_TOKEN_FLOOR = 0.6         # below this, two words are treated as unrelated

ZW_ZERO = "\u200b"
ZW_ONE = "\u200c"
ID_BITS = 64
TAG_BITS = 16
PAYLOAD_BITS = ID_BITS + TAG_BITS

PLACEHOLDER = re.compile(r"\[opsec\]", re.IGNORECASE)
SKIP_TOKEN = re.compile(r"https?://|`|<[@#a-z]?[!&:]?\d")  # links, code, mentions

# --------------------------------------------------------------------------
# Phrase pools
#
# Four independent groups per mode. Every entry within a group must be unique:
# duplicates collapse two fingerprints into one and make attribution ambiguous.
# --------------------------------------------------------------------------

TACTICAL_GROUPS = (
    (
        "Stay alert.", "Be prepared.", "Stand by.", "Hold the line.",
        "Stay focused.", "Keep watch.", "Eyes open.", "Stay sharp.",
        "Be ready.", "Hold positions.", "Watch the map.", "Stay vigilant.",
        "Maintain discipline.", "Stay online.", "Lock in.",
        "Keep your eyes peeled.", "Hold steady.", "Prepare for action.",
        "Stay on standby.", "Remain alert.",
    ),
    (
        "Watch the markers.", "Follow pings.", "Check alliance chat.",
        "Wait for orders.", "Listen to R4s.", "Track the target.",
        "Follow commands.", "Check your mail.", "Read the pins.",
        "Wait for the call.", "Listen to leadership.", "Follow the rally.",
        "Stay grouped.", "Watch for updates.", "Wait for pings.",
        "Observe the field.", "Follow the leads.", "Check Discord.",
        "Stay coordinated.", "Monitor the feed.",
    ),
    (
        "Move out.", "Advance.", "Deploy.", "Engage.", "Push forward.",
        "Execute.", "March out.", "Send troops.", "Move in.", "Attack.",
        "Push now.", "Go go go.", "Strike.", "Push the line.",
        "Advance troops.", "Move to target.", "Engage the enemy.",
        "Deploy forces.", "Push the objective.", "Execute orders.",
    ),
    (
        "No delays.", "Timing matters.", "Stay quiet until then.",
        "Full effort only.", "Nothing less.", "Same as last time.",
        "Keep formation.", "Report after.", "Confirm when done.",
        "We finish this.", "No hesitation.", "Hold nothing back.",
        "Precision over speed.", "Trust the plan.", "Speed matters here.",
        "Stay in sync.", "Cover each other.", "Regroup after.",
        "Finish clean.", "That is the call.",
    ),
)

CASUAL_GROUPS = (
    (
        "Great work everyone", "Appreciate the effort", "Thanks for staying active",
        "Awesome job today", "Solid push everyone", "Thanks for the dedication",
        "Great activity lately", "Love the energy here", "Excellent coordination",
        "Proud of this team", "Good stuff everyone", "Thanks for stepping up",
        "Appreciate the teamwork", "Amazing turnout", "Great job as always",
        "Thanks for being ready", "Well played everyone", "Appreciate all of you",
        "Fantastic work team", "Thanks for your time",
    ),
    (
        "Let's keep this momentum going", "Stay sharp for the next one",
        "Keep pushing those limits", "Let's maintain this pace",
        "Keep up the great work", "Stay focused on our goals",
        "Let's keep growing stronger", "Keep grinding those stats",
        "Let's stay ahead of the pack", "Keep this activity up",
        "Let's keep dominating", "Stay ready for more",
        "Keep improving every day", "Let's hold the line",
        "Keep your eyes on the prize", "Let's stay united",
        "Keep the communication up", "Let's prepare for what's next",
        "Keep showing up like this", "Let's keep winning together",
    ),
    (
        "Numbers are looking good", "The effort really shows",
        "Nice to see everyone around", "Activity has been solid",
        "Participation keeps climbing", "The teamwork is paying off",
        "Good energy all week", "Everyone pulled their weight",
        "That was a clean run", "Plenty to build on",
        "Momentum is on our side", "Big improvement overall",
        "The consistency is real", "Really strong showing",
        "Steady progress everywhere", "Communication has been great",
        "Coordination keeps improving", "That is the standard now",
        "The hard work is obvious", "Results speak for themselves",
    ),
    (
        "See you on the battlefield", "More updates to follow",
        "Catch you all later", "Have a great day",
        "Enjoy the rest of your week", "Talk to you all soon",
        "See you in Discord", "Stay safe out there", "Take it easy",
        "See you at reset", "Rest up for now",
        "Catch you at the next event", "Enjoy your downtime",
        "See you in the voice channel", "Have a good one",
        "Until next time", "Keep an eye on the pins",
        "See you all tomorrow", "Stay awesome", "Take care everyone",
    ),
)

ROW_GROUPS = (
    (
        "ready", "prepared", "focused", "active", "online", "early",
        "available", "organized", "alert", "steady", "committed", "locked",
        "waiting", "standing", "grouped", "connected", "updated", "aware",
        "calm", "confident", "motivated", "coordinated", "disciplined",
        "engaged", "present", "positioned", "settled", "watching", "moving",
        "rested",
    ),
    (
        "join", "check", "follow", "watch", "keep", "stay", "bring",
        "prepare", "support", "coordinate", "listen", "track", "maintain",
        "hold", "push", "move", "assist", "protect", "secure", "control",
        "gather", "build", "cover", "respond", "react", "review", "confirm",
        "organize", "continue", "remain",
    ),
    (
        "battle", "match", "event", "push", "call", "team", "group", "plan",
        "operation", "strategy", "war", "fight", "phase", "round",
        "objective", "attack", "defense", "movement", "rotation",
        "formation", "schedule", "mission", "effort", "attempt",
        "challenge", "campaign", "engagement", "session", "activity",
        "deployment",
    ),
    (
        "Full squad needed.", "Do not be late.", "Bring your best.",
        "Check in first.", "Rally goes on time.", "No solo moves.",
        "Watch the timer.", "Save your stamina.", "Gear up early.",
        "Sync with the group.", "Leave nothing behind.",
        "Stay until the end.", "Follow the caller.", "Keep chat clear.",
        "Double check your setup.", "Arrive a few minutes early.",
        "Bring full troops.", "No last minute drops.",
        "Confirm your slot.", "Hold until called.",
    ),
)


@dataclass(frozen=True)
class Mode:
    key: str
    label: str
    groups: tuple[tuple[str, ...], ...]
    template: str
    delete_after: int  # seconds

    @property
    def combinations(self) -> int:
        total = 1
        for group in self.groups:
            total *= len(group)
        return total


MODES: dict[str, Mode] = {
    "tactical": Mode(
        key="tactical",
        label="War",
        groups=TACTICAL_GROUPS,
        template="{0} {1} {2} {3}",
        delete_after=86400,
    ),
    "casual": Mode(
        key="casual",
        label="Social",
        groups=CASUAL_GROUPS,
        template="{0}. {1}. {2}. {3}.",
        delete_after=86400,
    ),
    "row": Mode(
        key="row",
        label="RoW",
        groups=ROW_GROUPS,
        template="Be {0}, {1} the {2}. {3}",
        delete_after=86400,
    ),
}

TEST_DELETE_AFTER = 30
TEST_CONFIRM_OVER = 10          # ask before a "test" that DMs more than this many
CONFIRM_TIMEOUT = 30.0

TestTarget = commands.Greedy[Union[discord.Member, discord.Role]]


# --------------------------------------------------------------------------
# Fingerprinting
# --------------------------------------------------------------------------

def _digest(*parts: object) -> bytes:
    if not FINGERPRINT_SECRET:
        raise RuntimeError("OPSEC_FINGERPRINT_SECRET is not set")
    message = "|".join(str(part) for part in parts).encode()
    return hmac.new(FINGERPRINT_SECRET, message, hashlib.sha256).digest()


def signoff_words(user_id: int, mode: Mode, variant: int = 0) -> list[str]:
    """Deterministic, secret-keyed word choice. Same inputs, same output."""
    number = int.from_bytes(_digest("signoff", mode.key, user_id, variant), "big")
    words = []
    for i, group in enumerate(mode.groups):
        words.append(group[(number >> (i * 32)) % len(group)])
    return words


def generate_signoff(user_id: int, mode: Mode, variant: int = 0) -> str:
    return mode.template.format(*signoff_words(user_id, mode, variant))


# --------------------------------------------------------------------------
# Zero-width watermark
# --------------------------------------------------------------------------

def _payload_bits(user_id: int) -> str:
    tag = int.from_bytes(_digest("watermark", user_id)[:2], "big")
    return f"{user_id:0{ID_BITS}b}{tag:0{TAG_BITS}b}"


def encode_watermark(text: str, user_id: int) -> str:
    """Spread the payload across word boundaries instead of dumping it at the end.

    Tokens that look like links, mentions or code are skipped, since an
    invisible character inside those breaks rendering.
    """
    chars = [ZW_ONE if bit == "1" else ZW_ZERO for bit in _payload_bits(user_id)]
    pieces: list[str] = []
    index = 0
    for token in re.split(r"(\s+)", text):
        pieces.append(token)
        if index < len(chars) and token.strip() and not SKIP_TOKEN.search(token):
            pieces.append(chars[index])
            index += 1
    pieces.append("".join(chars[index:]))  # anything that did not fit
    return "".join(pieces)


def decode_watermark(text: str) -> int | None:
    """Recover a user ID from pasted text. Returns None if nothing verifies."""
    bits = "".join("1" if c == ZW_ONE else "0" for c in text if c in (ZW_ZERO, ZW_ONE))
    for start in range(len(bits) - PAYLOAD_BITS, -1, -1):
        window = bits[start:start + PAYLOAD_BITS]
        user_id = int(window[:ID_BITS], 2)
        if user_id < (1 << 40):  # not a plausible snowflake
            continue
        if window[ID_BITS:] == _payload_bits(user_id)[ID_BITS:]:
            return user_id
    return None


# --------------------------------------------------------------------------
# Text handling
# --------------------------------------------------------------------------

def inject_signoff(announcement: str, signoff: str) -> str:
    """Place the sign-off at [opsec], or mid-way through the message."""
    if PLACEHOLDER.search(announcement):
        return PLACEHOLDER.sub(lambda _: signoff, announcement)

    paragraphs = [p for p in announcement.split("\n\n") if p.strip()]
    if len(paragraphs) >= 2:
        mid = len(paragraphs) // 2
        paragraphs[mid] = f"{paragraphs[mid]}\n{signoff}"
        return "\n\n".join(paragraphs)
    return f"{announcement.strip()}\n\n{signoff}"


def normalise(raw: str) -> str:
    """Fold OCR output and our own phrases into the same comparable shape."""
    text = unicodedata.normalize("NFKC", raw)
    text = re.sub(r"[\u200b-\u200f\u202a-\u202e\u2060\ufeff]", "", text)
    text = text.lower().replace("\u2019", "'").replace("\u2018", "'")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return " ".join(text.split())


def _token_similarity(a: str, b: str) -> float:
    """Character-level agreement, so 'excnute' still resembles 'execute'."""
    if a == b:
        return 1.0
    matcher = SequenceMatcher(None, a, b)
    if matcher.quick_ratio() < FUZZY_TOKEN_FLOOR:
        return 0.0
    ratio = matcher.ratio()
    return ratio if ratio >= FUZZY_TOKEN_FLOOR else 0.0


def _window_score(phrase: Sequence[str], target: Sequence[str]) -> float:
    """Best agreement for this phrase anywhere in the target, OCR damage allowed."""
    if not phrase or len(target) < len(phrase):
        return 0.0
    best = 0.0
    for start in range(len(target) - len(phrase) + 1):
        window = target[start:start + len(phrase)]
        total = sum(_token_similarity(a, b) for a, b in zip(phrase, window))
        best = max(best, total / len(phrase))
        if best == 1.0:
            break
    return best


@dataclass
class Match:
    user_id: int
    mode: Mode
    variant: int
    score: float
    exact: bool


def identify(user_ids: Iterable[int], leaked: str) -> list[Match]:
    """Rank candidate authors of a leaked message. Exact pass first, fuzzy second."""
    target = normalise(leaked)
    target_tokens = target.split()
    candidates = list(user_ids)

    def best_for(user_id: int, fuzzy: bool) -> Match | None:
        best: Match | None = None
        for mode in MODES.values():
            for variant in range(MAX_VARIANTS):
                words = signoff_words(user_id, mode, variant)
                full = normalise(mode.template.format(*words))
                if full and full in target:
                    return Match(user_id, mode, variant, 1.0, True)
                phrases = [normalise(w) for w in words]
                if fuzzy:
                    scores = [_window_score(p.split(), target_tokens) for p in phrases]
                else:
                    scores = [1.0 if p and p in target else 0.0 for p in phrases]
                score = sum(scores) / len(scores)
                if best is None or score > best.score:
                    best = Match(user_id, mode, variant, score, False)
        return best

    def strongest(results: list[Match]) -> list[Match]:
        exact = [m for m in results if m.exact]
        if exact:
            return exact
        hits = [m for m in results if m.score >= MATCH_THRESHOLD]
        if not hits:
            return []
        # Only the strongest tier. Weaker candidates are coincidence: two
        # members sharing 2 of 4 phrases is common, sharing 3 is not.
        top = max(m.score for m in hits)
        return [m for m in hits if m.score >= top - 1e-9]

    # Pass 1: cheap exact substring matching over everyone.
    first = [m for m in (best_for(uid, False) for uid in candidates) if m]
    found = strongest(first)
    if found:
        return found

    # Pass 2: character-level matching, but only for members who already
    # matched at least one phrase. Running it on the whole guild is both slow
    # and a false-positive machine.
    shortlist = [m.user_id for m in first if m.score >= SHORTLIST_THRESHOLD]
    if not shortlist:
        return []
    return strongest([m for m in (best_for(uid, True) for uid in shortlist) if m])


# --------------------------------------------------------------------------
# Cog
# --------------------------------------------------------------------------

@dataclass
class Delivery:
    member: discord.Member
    variant: int
    signoff: str
    payload: str
    visible: str


class SpyDetector(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._pending: set[asyncio.Task] = set()

    async def cog_unload(self) -> None:
        for task in list(self._pending):
            task.cancel()

    # -- helpers ----------------------------------------------------------

    def _schedule_delete(self, message: discord.Message, delay: int) -> None:
        """Best-effort cleanup. Note: this does not survive a bot restart."""
        async def run() -> None:
            try:
                await asyncio.sleep(delay)
                await message.delete()
            except (discord.HTTPException, asyncio.CancelledError):
                pass

        task = asyncio.create_task(run())
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    @staticmethod
    def _prepare(members: Sequence[discord.Member], mode: Mode,
                 announcement: str) -> tuple[list[Delivery], list[str]]:
        """Build every message up front. Nothing is sent unless all of this passes."""
        problems: list[str] = []
        deliveries: list[Delivery] = []
        taken: dict[str, int] = {}

        for member in sorted(members, key=lambda m: m.id):
            for variant in range(MAX_VARIANTS):
                signoff = generate_signoff(member.id, mode, variant)
                if signoff not in taken:
                    break
            else:
                problems.append(
                    f"{member} could not be given a unique phrase after "
                    f"{MAX_VARIANTS} attempts"
                )
                continue
            taken[signoff] = member.id

            visible = inject_signoff(announcement, signoff)
            payload = encode_watermark(visible, member.id)
            if len(payload) > MAX_MESSAGE_LENGTH:
                problems.append(
                    f"message for {member} is {len(payload)}/{MAX_MESSAGE_LENGTH} "
                    f"characters"
                )
                continue
            deliveries.append(Delivery(member, variant, signoff, payload, visible))

        return deliveries, problems

    async def _upload_log(self, ctx: commands.Context, title: str, body: str,
                          filename: str) -> None:
        channel = self.bot.get_channel(LOG_CHANNEL_ID)
        if channel is None:
            await ctx.send("Warning: log channel is unavailable, no audit log written.")
            return
        buffer = io.BytesIO(body.encode("utf-8"))
        try:
            await channel.send(
                content=f"**{title}**\nInitiated by: {ctx.author.mention}",
                file=discord.File(fp=buffer, filename=filename),
            )
        except discord.HTTPException as exc:
            await ctx.send(f"Warning: could not write the audit log ({exc}).")

    async def _broadcast(self, ctx: commands.Context, mode: Mode,
                         members: Sequence[discord.Member], announcement: str,
                         *, delete_after: int, scope: str) -> None:
        recipients = {m.id: m for m in members if not m.bot}
        if not recipients:
            await ctx.send("No eligible (non-bot) recipients.")
            return

        deliveries, problems = self._prepare(list(recipients.values()), mode, announcement)
        if problems:
            detail = "\n".join(f"- {p}" for p in problems[:10])
            more = f"\n...and {len(problems) - 10} more" if len(problems) > 10 else ""
            await ctx.send(
                f"**Aborted, nothing was sent.** Preflight found "
                f"{len(problems)} problem(s):\n{detail}{more}"
            )
            return

        hours = delete_after / 3600
        window = f"{delete_after}s" if delete_after < 3600 else f"{hours:.0f}h"
        status = await ctx.send(
            f"Sending {mode.label} broadcast to **{len(deliveries)}** member(s) "
            f"in {scope}. Auto-delete: {window}. This takes about "
            f"{len(deliveries) * DM_DELAY_SECONDS:.0f}s."
        )

        log = io.StringIO()
        log.write(
            f"--- {mode.label.upper()} BROADCAST ---\n"
            f"Scope: {scope}\nInitiated by: {ctx.author} ({ctx.author.id})\n"
            f"Mode: {mode.key} ({mode.combinations:,} combinations)\n"
            f"Base message:\n{announcement}\n"
            f"{'-' * 40}\n"
            f"Phrases are regenerable from member ID + mode + variant.\n\n"
        )

        sent = failed = 0
        for position, delivery in enumerate(deliveries, start=1):
            member = delivery.member
            try:
                message = await member.send(delivery.payload)
            except discord.Forbidden:
                failed += 1
                log.write(f"FAILED  {member} ({member.id}) - DMs closed\n")
            except discord.HTTPException as exc:
                failed += 1
                log.write(f"FAILED  {member} ({member.id}) - {exc}\n")
            else:
                sent += 1
                log.write(f"SENT    {member} ({member.id}) variant={delivery.variant}\n")
                self._schedule_delete(message, delete_after)

            if position % PROGRESS_EVERY == 0:
                try:
                    await status.edit(
                        content=f"Sending {mode.label} broadcast... "
                                f"{position}/{len(deliveries)}"
                    )
                except discord.HTTPException:
                    pass
            if position < len(deliveries):
                await asyncio.sleep(DM_DELAY_SECONDS)

        await self._upload_log(
            ctx,
            f"{mode.label} Broadcast Report - {sent} sent, {failed} failed",
            log.getvalue(),
            f"{mode.key}_log.txt",
        )
        log.close()
        await ctx.send(
            f"{mode.label} broadcast complete. Sent **{sent}**, failed **{failed}**. "
            f"DMs self-delete in {window}."
        )

    async def _role_broadcast(self, ctx: commands.Context, mode: Mode,
                              roles: Sequence[discord.Role],
                              announcement: str) -> None:
        """Accepts any number of roles. Every one must be on the allowlist."""
        if not roles:
            await ctx.send("Name at least one role.")
            return

        allowed = ALLOWED_ROLE_IDS[mode.key]
        if not allowed:
            await ctx.send(
                f"No roles are configured for `{mode.key}` broadcasts. "
                f"Set `OPSEC_ROLES_{mode.key.upper()}` before using this."
            )
            return

        rejected = [r for r in roles if r.is_default() or r.id not in allowed]
        if rejected:
            names = ", ".join(f"<@&{rid}>" for rid in allowed)
            bad = ", ".join(f"`{r.name}`" for r in rejected)
            await ctx.send(
                f"**Blocked, nothing was sent.** Not approved for {mode.label} "
                f"broadcasts: {bad}\nAllowed: {names}",
                allowed_mentions=discord.AllowedMentions.none(),
            )
            return

        if not ctx.guild.chunked:
            await ctx.guild.chunk()

        members: dict[int, discord.Member] = {}
        for role in roles:
            members.update({m.id: m for m in role.members})

        scope = "role " + ", ".join(r.name for r in roles)
        await self._broadcast(
            ctx, mode, list(members.values()), announcement,
            delete_after=mode.delete_after, scope=scope,
        )

    # -- war --------------------------------------------------------------

    @commands.command(name="warbroadcast")
    @commands.guild_only()
    @commands.has_guild_permissions(administrator=True)
    async def warbroadcast(self, ctx, roles: commands.Greedy[discord.Role], *,
                            announcement: str):
        await self._role_broadcast(ctx, MODES["tactical"], roles, announcement)

    @commands.command(name="testwarbroadcast", aliases=["testwar"])
    @commands.guild_only()
    @commands.has_guild_permissions(administrator=True)
    async def testwarbroadcast(self, ctx, targets: TestTarget,
                               *, announcement: str):
        await self._test(ctx, MODES["tactical"], targets, announcement)

    # -- row --------------------------------------------------------------

    @commands.command(name="rowbroadcast")
    @commands.guild_only()
    @commands.has_guild_permissions(administrator=True)
    async def rowbroadcast(self, ctx, roles: commands.Greedy[discord.Role], *,
                            announcement: str):
        await self._role_broadcast(ctx, MODES["row"], roles, announcement)

    @commands.command(name="testrowbroadcast", aliases=["testrow"])
    @commands.guild_only()
    @commands.has_guild_permissions(administrator=True)
    async def testrowbroadcast(self, ctx, targets: TestTarget,
                               *, announcement: str):
        await self._test(ctx, MODES["row"], targets, announcement)

    # -- social -----------------------------------------------------------

    @commands.command(name="socialbroadcast")
    @commands.guild_only()
    @commands.has_guild_permissions(administrator=True)
    async def socialbroadcast(self, ctx, roles: commands.Greedy[discord.Role], *,
                            announcement: str):
        await self._role_broadcast(ctx, MODES["casual"], roles, announcement)

    @commands.command(name="testsocialbroadcast", aliases=["testsocial"])
    @commands.guild_only()
    @commands.has_guild_permissions(administrator=True)
    async def testsocialbroadcast(self, ctx, targets: TestTarget,
                                  *, announcement: str):
        await self._test(ctx, MODES["casual"], targets, announcement)

    async def _test(self, ctx, mode: Mode, targets, announcement: str) -> None:
        """Test sends accept members, roles, or a mix. No allowlist, 30s cleanup."""
        if not targets:
            await ctx.send("Mention at least one member or role.")
            return

        if not ctx.guild.chunked:
            await ctx.guild.chunk()

        members: dict[int, discord.Member] = {}
        labels: list[str] = []
        for target in targets:
            if isinstance(target, discord.Role):
                if target.is_default():
                    await ctx.send("Blocked: @everyone is never a valid target.")
                    return
                members.update({m.id: m for m in target.members if not m.bot})
                labels.append(f"role {target.name}")
            elif not target.bot:
                members[target.id] = target
                labels.append(str(target))

        if not members:
            await ctx.send("No eligible (non-bot) recipients in that selection.")
            return

        if len(members) > TEST_CONFIRM_OVER:
            seconds = len(members) * DM_DELAY_SECONDS
            await ctx.send(
                f"This will DM **{len(members)}** real people and take about "
                f"{seconds / 60:.0f} minute(s). That is a live broadcast, not a "
                f"quiet test \u2014 the only difference is the 30s auto-delete.\n"
                f"Type `confirm` within {CONFIRM_TIMEOUT:.0f}s to proceed, or test "
                f"on two or three members first."
            )

            def check(message: discord.Message) -> bool:
                return (
                    message.author == ctx.author
                    and message.channel == ctx.channel
                    and message.content.strip().lower() == "confirm"
                )

            try:
                await self.bot.wait_for("message", check=check, timeout=CONFIRM_TIMEOUT)
            except asyncio.TimeoutError:
                await ctx.send("Cancelled. Nothing was sent.")
                return

        scope = ", ".join(labels[:3]) + (f" +{len(labels) - 3} more" if len(labels) > 3 else "")
        await self._broadcast(
            ctx, mode, list(members.values()), announcement,
            delete_after=TEST_DELETE_AFTER, scope=f"test: {scope}",
        )

    # -- attribution ------------------------------------------------------

    @commands.command(name="catchscreenshot", aliases=["catch"])
    @commands.guild_only()
    @commands.has_guild_permissions(administrator=True)
    async def catchscreenshot(self, ctx, roles: commands.Greedy[discord.Role], *,
                              leaked: str):
        watermarked = decode_watermark(leaked)
        if watermarked is not None:
            member = ctx.guild.get_member(watermarked)
            who = member.mention if member else f"unknown user `{watermarked}`"
            await ctx.send(
                f"**Watermark decoded.** This text was sent to {who} "
                f"(ID `{watermarked}`). Checksum verified.",
                allowed_mentions=discord.AllowedMentions.none(),
            )
            return

        async with ctx.typing():
            if not ctx.guild.chunked:
                await ctx.guild.chunk()
            if roles:
                pool = {m.id for r in roles for m in r.members if not m.bot}
                scope = "role " + ", ".join(r.name for r in roles)
            else:
                pool = {m.id for m in ctx.guild.members if not m.bot}
                scope = "the whole server"
            matches = await asyncio.to_thread(identify, sorted(pool), leaked)

        if not matches:
            await ctx.send(
                f"No match among {len(pool)} member(s) in {scope}. The phrase may "
                f"have been edited, or the leaker was not in that broadcast."
            )
            return

        top = matches[0].score
        tied = [m for m in matches if m.score >= top - 0.01]
        lines = []
        for match in matches[:5]:
            member = ctx.guild.get_member(match.user_id)
            name = f"`{member}`" if member else f"`{match.user_id}`"
            kind = "exact phrase" if match.exact else f"{match.score:.0%} of phrases"
            lines.append(f"- {name} (ID `{match.user_id}`) - {match.mode.label}, {kind}")

        header = (
            "**Match found.**" if len(tied) == 1
            else f"**{len(tied)} candidates tied.** Treat this as inconclusive."
        )
        await ctx.send(
            f"{header}\n" + "\n".join(lines) +
            "\n\nPartial matches can come from OCR errors. Confirm before acting."
        )

    @commands.command(name="opsecpreview", aliases=["preview"])
    @commands.guild_only()
    @commands.has_guild_permissions(administrator=True)
    async def opsecpreview(self, ctx, target: Union[discord.Member, discord.Role],
                           mode: str = "tactical"):
        selected = MODES.get(mode.lower())
        if selected is None:
            await ctx.send(f"Unknown mode. Options: {', '.join(MODES)}")
            return

        if isinstance(target, discord.Member):
            lines = [
                f"variant {v}: {generate_signoff(target.id, selected, v)}"
                for v in range(MAX_VARIANTS)
            ]
            await ctx.send(
                f"Phrases for `{target}` in **{selected.label}** mode "
                f"({selected.combinations:,} combinations):\n```\n" +
                "\n".join(lines) + "\n```"
            )
            return

        if target.is_default():
            await ctx.send("Blocked: @everyone is never a valid target.")
            return
        if not ctx.guild.chunked:
            await ctx.guild.chunk()

        members = sorted((m for m in target.members if not m.bot), key=lambda m: m.id)
        if not members:
            await ctx.send(f"`{target.name}` has no non-bot members.")
            return

        # Variant assignment depends on who else is in the batch, so run the
        # same preflight the broadcast would to show the phrases people
        # would actually receive.
        deliveries, problems = self._prepare(members, selected, "[opsec]")
        body = io.StringIO()
        body.write(
            f"--- {selected.label.upper()} PHRASE PREVIEW ---\n"
            f"Role: {target.name} ({len(members)} members)\n"
            f"Requested by: {ctx.author} ({ctx.author.id})\n"
            f"{'-' * 40}\n"
            f"LIVE FINGERPRINTS. Anyone holding this can forge a leak.\n\n"
        )
        for delivery in deliveries:
            body.write(
                f"{delivery.member} ({delivery.member.id}) "
                f"variant={delivery.variant}\n  {delivery.signoff}\n"
            )
        for problem in problems:
            body.write(f"PROBLEM: {problem}\n")

        buffer = io.BytesIO(body.getvalue().encode("utf-8"))
        body.close()
        await ctx.send(
            f"Phrases for **{len(deliveries)}** member(s) of `{target.name}` in "
            f"**{selected.label}** mode. Treat this file as sensitive.",
            file=discord.File(fp=buffer, filename=f"preview_{selected.key}.txt"),
        )

    # -- errors -----------------------------------------------------------

    async def cog_command_error(self, ctx, error) -> None:
        if isinstance(error, commands.MissingPermissions):
            await ctx.send("You need the Administrator permission for this.")
        elif isinstance(error, (commands.BadArgument, commands.MissingRequiredArgument)):
            await ctx.send(f"Bad input: {error}")
        elif isinstance(error, commands.NoPrivateMessage):
            await ctx.send("Run this in the server, not in DMs.")
        else:
            raise error


async def setup(bot: commands.Bot) -> None:
    if not FINGERPRINT_SECRET:
        raise RuntimeError(
            "OPSEC_FINGERPRINT_SECRET is not set. Without it the fingerprints "
            "are guessable by anyone holding this file."
        )
    if len(FINGERPRINT_SECRET) < 16:
        raise RuntimeError("OPSEC_FINGERPRINT_SECRET should be at least 16 characters.")
    if not LOG_CHANNEL_ID:
        raise RuntimeError("OPSEC_LOG_CHANNEL_ID is not set.")
    for mode_key, group_set in ((m.key, m.groups) for m in MODES.values()):
        for index, group in enumerate(group_set):
            if len(set(group)) != len(group):
                raise RuntimeError(
                    f"Duplicate phrase in {mode_key} group {index}; "
                    f"duplicates make attribution ambiguous."
                )
    await bot.add_cog(SpyDetector(bot))
