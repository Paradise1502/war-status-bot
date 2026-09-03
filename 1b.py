"""
lb.py — shared leaderboard engine.

Imports nothing from main.py, so there are no circular-import problems.

The idea: almost every leaderboard you have is the same operation — take a
table, rank rows by a column, filter by server and power, show the top or
bottom N. The only real differences are which column and which dataset.

So we first turn both datasets into the same shape (a "gains table"), then one
ranking function and one embed builder serve all of them.
"""

import re
from datetime import date, timedelta

import discord


# ---------------------------------------------------------------------------
# Servers
# ---------------------------------------------------------------------------

SERVER_NAMES = {
    "375": "NVR",
    "357": "YSS",
    "756": "SAB",
    "341": "NW:E",
    "320": "EvG",
    "5":   "OMG",
}

SERVER_COLORS = {
    "375": 0xE74C3C,   # red — home
    "357": 0x3498DB,   # blue — main rival
    "756": 0xE67E22,
    "341": 0x9B59B6,
    "320": 0x2ECC71,
    "5":   0xF1C40F,
}

DEFAULT_SERVER = "375"


def server_label(server):
    if server is None:
        return "All servers"
    tag = SERVER_NAMES.get(str(server))
    return f"{tag} ({server})" if tag else f"Server {server}"


def server_color(server):
    return SERVER_COLORS.get(str(server), 0x2F3136)


# ---------------------------------------------------------------------------
# Number parsing / formatting
# ---------------------------------------------------------------------------

def to_int(val):
    if val is None:
        return 0
    s = str(val).replace("\u00A0", "").replace(",", "").replace(" ", "").strip()
    if s in ("", "-"):
        return 0
    try:
        return int(s)
    except ValueError:
        digits = "".join(ch for ch in s if ch.isdigit() or ch == "-")
        try:
            return int(digits) if digits not in ("", "-") else 0
        except ValueError:
            return 0


def fmt(num):
    """Compact: 12.3M / 45.6K / 789"""
    n = abs(num)
    if n >= 1_000_000_000:
        return f"{num/1_000_000_000:.2f}B"
    if n >= 1_000_000:
        return f"{num/1_000_000:.2f}M"
    if n >= 1_000:
        return f"{num/1_000:.1f}K"
    return str(num)


def fmt_full(num):
    return f"{num:,}"


RANK_ICONS = {1: "🥇", 2: "🥈", 3: "🥉"}


def rank_icon(i):
    return RANK_ICONS.get(i, f"`{i:>2}.`")


# ---------------------------------------------------------------------------
# Turning a table into rows of dicts
# ---------------------------------------------------------------------------

def as_dicts(table):
    """[headers, row, ...] -> (headers, [ {header: value}, ... ])"""
    if not table:
        return [], []
    headers = [str(h).strip() for h in table[0]]
    out = []
    for row in table[1:]:
        d = {h: (str(row[i]).strip() if i < len(row) else "")
             for i, h in enumerate(headers)}
        out.append(d)
    return headers, out


def find_col(headers, *candidates):
    """Locate a header by exact name, then substring. Case-insensitive."""
    lowered = [h.strip().lower() for h in headers]
    for c in candidates:
        cl = c.strip().lower()
        if cl in lowered:
            return headers[lowered.index(cl)]
    for c in candidates:
        cl = c.strip().lower()
        for i, h in enumerate(lowered):
            if cl in h:
                return headers[i]
    return None


# ---------------------------------------------------------------------------
# Gains
# ---------------------------------------------------------------------------
# Columns that are absolute values, not accumulating counters. These are taken
# from the latest row rather than subtracted.
# ---------------------------------------------------------------------------

ABSOLUTE_SCAN_COLUMNS = {
    "lord_id", "name", "alliance_id", "alliance_tag", "town_center",
    "home_server", "in_power_rankings", "power", "map_id", "faction",
    "highest_power", "legion_power", "tech_power", "building_power",
    "hero_power",
}


def materialize_gains(latest, prev, id_col="lord_id",
                      absolute_columns=ABSOLUTE_SCAN_COLUMNS):
    """
    Subtract prev from latest, giving a table of gains in the same
    [headers, row, ...] shape. Absolute columns keep their latest value.

    Players missing from prev are kept as-is (they appeared mid-window).
    Negative gains are clamped to 0, guarding against sheet corrections.
    """
    if not latest:
        return None
    if not prev:
        return latest

    headers = [str(h).strip() for h in latest[0]]
    lower = [h.lower() for h in headers]

    try:
        idx = lower.index(id_col.strip().lower())
    except ValueError:
        return latest

    prev_map = {str(r[idx]).strip(): r for r in prev[1:]
                if idx < len(r) and str(r[idx]).strip()}

    out = [headers]
    for row in latest[1:]:
        if idx >= len(row):
            continue
        rid = str(row[idx]).strip()
        base = prev_map.get(rid)
        if base is None:
            out.append(list(row))
            continue

        new_row = []
        for i, val in enumerate(row):
            if lower[i] in absolute_columns:
                new_row.append(val)
            else:
                gain = to_int(val) - to_int(base[i] if i < len(base) else 0)
                new_row.append(str(max(0, gain)))
        out.append(new_row)
    return out


# ---------------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------------

def rank_table(table, value, *,
               id_col="lord_id",
               name_col=None,
               alliance_col=None,
               power_col=None,
               server_col=None,
               server=None,
               min_power=0,
               top=True,
               limit=10,
               drop_zero=False,
               extra_cols=None):
    """
    Rank rows of a table.

    value: a column name, a list of column names (summed), or a callable
           taking a getter function get(col) -> int.

    Returns (entries, total_eligible). Each entry is a dict with
    name / alliance / lord_id / power / value / extras.
    """
    headers, rows = as_dicts(table)
    if not rows:
        return [], 0

    id_col       = find_col(headers, id_col) or id_col
    name_col     = find_col(headers, name_col or "name", "character name") or ""
    alliance_col = find_col(headers, alliance_col or "alliance_tag", "alliance") or ""
    power_col    = find_col(headers, power_col or "highest_power",
                            "historical highest power", "power") or ""
    server_col   = find_col(headers, server_col or "home_server", "server") or ""

    if callable(value):
        value_fn = value
    else:
        cols = [value] if isinstance(value, str) else list(value)
        resolved = [find_col(headers, c) for c in cols]
        missing = [c for c, r in zip(cols, resolved) if r is None]
        if missing:
            raise ValueError(f"Column(s) not found: {', '.join(missing)}")
        value_fn = lambda get: sum(get(c) for c in resolved)

    entries = []
    for r in rows:
        get = lambda c: to_int(r.get(c, 0))

        if server and server_col:
            row_server = "".join(ch for ch in str(r.get(server_col, "")) if ch.isdigit())
            if row_server != str(server):
                continue

        power = get(power_col) if power_col else 0
        if min_power and power < min_power:
            continue

        try:
            val = value_fn(get)
        except Exception:
            continue

        if drop_zero and val == 0:
            continue

        entry = {
            "lord_id":  str(r.get(id_col, "")).strip(),
            "name":     str(r.get(name_col, "?")).strip() or "?",
            "alliance": str(r.get(alliance_col, "")).strip(),
            "power":    power,
            "value":    val,
        }
        if extra_cols:
            for label, col in extra_cols.items():
                resolved_col = find_col(headers, col)
                entry[label] = to_int(r.get(resolved_col, 0)) if resolved_col else 0
        entries.append(entry)

    total = len(entries)
    entries.sort(key=lambda e: (e["value"], e["name"]), reverse=top)
    return entries[:limit], total


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
# Accepts, in any order:
#   a server   -> "357", "s357", "nvr", "yss", "all"
#   a count    -> "50"
#   a window   -> "7d", "2w", "season", "2026-08-15"
#   a season   -> "sos2", "sos4", ...
#
# A bare number is a COUNT unless it's a known server id above 100.
# For single-digit servers (OMG = 5) use the s-prefix: "s5".
# ---------------------------------------------------------------------------

TAG_TO_SERVER = {v.lower(): k for k, v in SERVER_NAMES.items()}


def parse_window(token):
    if not token:
        return None
    t = str(token).strip().lower()
    if t in ("season", "all-season", "full", "total"):
        return ("season", None)
    m = re.fullmatch(r"(\d+)\s*d(?:ays?)?", t)
    if m:
        return ("days", int(m.group(1)))
    m = re.fullmatch(r"(\d+)\s*w(?:eeks?)?", t)
    if m:
        return ("days", int(m.group(1)) * 7)
    m = re.fullmatch(r"(20\d{2})-(\d{2})-(\d{2})", t)
    if m:
        try:
            return ("date", date(int(m.group(1)), int(m.group(2)), int(m.group(3))))
        except ValueError:
            return None
    return None


def parse_server(token):
    """Return a server id string, 'all', or None if this isn't a server token."""
    t = str(token).strip().lower()
    if t in ("all", "*", "everyone"):
        return "all"
    if t in TAG_TO_SERVER:
        return TAG_TO_SERVER[t]
    m = re.fullmatch(r"s(\d+)", t)
    if m:
        return m.group(1)
    if t.isdigit() and t in SERVER_NAMES and int(t) > 100:
        return t
    return None


def parse_args(args, valid_seasons, default_season,
               default_server=DEFAULT_SERVER, default_limit=10, max_limit=100):
    """
    Returns (opts, unknown_tokens) where opts has:
        server (str or None for all), limit, window, season
    """
    opts = {
        "server": default_server,
        "limit": default_limit,
        "window": None,
        "season": default_season,
    }
    unknown = []

    for a in args:
        t = str(a).strip().lower()
        if not t:
            continue

        if t in valid_seasons:
            opts["season"] = t
            continue

        srv = parse_server(t)
        if srv is not None:
            opts["server"] = None if srv == "all" else srv
            continue

        if parse_window(t) is not None:
            opts["window"] = t
            continue

        if t.isdigit():
            opts["limit"] = max(1, min(max_limit, int(t)))
            continue

        unknown.append(a)

    return opts, unknown


# ---------------------------------------------------------------------------
# Embeds + pagination
# ---------------------------------------------------------------------------

def build_pages(entries, per_page=10, start_rank=1,
                line_fn=None, unit="", show_detail=None):
    """Split ranked entries into pages of pre-rendered text."""
    if line_fn is None:
        def line_fn(i, e):
            tag = f"`[{e['alliance']}]` " if e["alliance"] else ""
            head = f"{rank_icon(i)} {tag}**{e['name']}** — `{fmt(e['value'])}{unit}`"
            if show_detail:
                head += "\n" + show_detail(e)
            return head

    pages = []
    for start in range(0, len(entries), per_page):
        chunk = entries[start:start + per_page]
        lines = [line_fn(start_rank + start + i, e) for i, e in enumerate(chunk)]
        pages.append("\n".join(lines))
    return pages or ["*No players matched these filters.*"]


class LeaderboardView(discord.ui.View):
    """Prev/Next buttons. Only the person who ran the command can page."""

    def __init__(self, pages, embed_factory, author_id, timeout=300):
        super().__init__(timeout=timeout)
        self.pages = pages
        self.embed_factory = embed_factory
        self.author_id = author_id
        self.index = 0
        self.message = None
        if len(pages) <= 1:
            self.clear_items()
        self._sync()

    def _sync(self):
        if not self.children:
            return
        self.prev_button.disabled = self.index == 0
        self.next_button.disabled = self.index >= len(self.pages) - 1

    def current_embed(self):
        return self.embed_factory(self.pages[self.index], self.index, len(self.pages))

    async def interaction_check(self, interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "Only the person who ran this command can page through it.",
                ephemeral=True,
            )
            return False
        return True

    async def on_timeout(self):
        self.clear_items()
        if self.message:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass

    @discord.ui.button(label="◀", style=discord.ButtonStyle.secondary)
    async def prev_button(self, interaction, button):
        self.index = max(0, self.index - 1)
        self._sync()
        await interaction.response.edit_message(embed=self.current_embed(), view=self)

    @discord.ui.button(label="▶", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction, button):
        self.index = min(len(self.pages) - 1, self.index + 1)
        self._sync()
        await interaction.response.edit_message(embed=self.current_embed(), view=self)


def make_embed_factory(*, title, subtitle, footer, color):
    def factory(page_text, index, total):
        embed = discord.Embed(
            title=title,
            description=f"{subtitle}\n\n{page_text}",
            color=color,
        )
        suffix = f" · Page {index + 1}/{total}" if total > 1 else ""
        embed.set_footer(text=footer + suffix)
        return embed
    return factory


async def send_leaderboard(ctx, *, title, subtitle, footer, color,
                           entries, per_page=10, unit="", show_detail=None):
    """Build pages, send the first, attach paging buttons if needed."""
    pages = build_pages(entries, per_page=per_page, unit=unit,
                        show_detail=show_detail)
    factory = make_embed_factory(title=title, subtitle=subtitle,
                                 footer=footer, color=color)
    view = LeaderboardView(pages, factory, ctx.author.id)
    message = await ctx.send(embed=view.current_embed(),
                             view=view if len(pages) > 1 else None)
    view.message = message
    return message
