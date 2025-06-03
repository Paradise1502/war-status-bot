import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import discord
from discord.ext import commands

# Google Sheets Auth
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_json = os.getenv("CREDENTIALS_JSON")
creds_dict = json.loads(creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# Now your bot setup
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

@bot.command()
async def rssheal(ctx, lord_id: str):
    try:
        # Get the two latest sheet tabs
        tabs = client.open("Copy SoS5").worksheets()  # Replace with your sheet name if needed
        if len(tabs) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = tabs[-1]
        previous = tabs[-2]

        latest_data = latest.get_all_values()
        previous_data = previous.get_all_values()

        headers = latest_data[0]
        name_index = 1  # Column B
        id_index = headers.index("lord_id")

        # Column indices: AF=31, AG=32, AH=33, AI=34 (zero-indexed)
        gold_idx, wood_idx, ore_idx, mana_idx = 31, 32, 33, 34

        def find_row(data):
            for row in data[1:]:
                if row[id_index] == lord_id:
                    return row
            return None

        row_latest = find_row(latest_data)
        username = row_latest[name_index]
        row_prev = find_row(previous_data)

        if not row_latest or not row_prev:
            await ctx.send("❌ Lord ID not found in both sheets.")
            return

        def to_int(val):
            try: return int(val)
            except: return 0

        gold = to_int(row_latest[gold_idx]) - to_int(row_prev[gold_idx])
        wood = to_int(row_latest[wood_idx]) - to_int(row_prev[wood_idx])
        ore  = to_int(row_latest[ore_idx])  - to_int(row_prev[ore_idx])
        mana = to_int(row_latest[mana_idx]) - to_int(row_prev[mana_idx])

        await ctx.send(
            f"📊 RSS Spent by `{username}` (`{lord_id}`) between `{previous.title}` → `{latest.title}`:\n"
            f"🪙 Gold: {gold:,}\n"
            f"🪵 Wood: {wood:,}\n"
            f"⛏️ Ore: {ore:,}\n"
            f"💧 Mana: {mana:,}"
        )

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def stats(ctx, lord_id: str):
    try:
        sheets = client.open("Copy SoS5").worksheets()
        if len(sheets) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest, previous = sheets[-1], sheets[-2]
        data_latest = latest.get_all_values()
        data_prev = previous.get_all_values()
        headers = data_latest[0]

        id_index = headers.index("lord_id")
        name_index = 1
        alliance_index = 3
        power_idx = 12
        kills_idx = 9
        healed_idx = 18
        dead_idx = 17

        def to_int(val):
            try: return int(val.replace(',', '').replace('-', '').strip())
            except: return 0

        # Build prev map
        prev_map = {row[id_index]: row for row in data_prev[1:] if len(row) > dead_idx}

        # Find target rows
        row_latest = next((row for row in data_latest[1:] if row[id_index] == lord_id), None)
        row_prev = prev_map.get(lord_id)

        if not row_latest or not row_prev:
            await ctx.send("❌ Lord ID not found in both sheets.")
            return

        name = row_latest[name_index].strip()
        alliance = row_latest[alliance_index].strip()

        power_latest = to_int(row_latest[power_idx])
        power_gain = power_latest - to_int(row_prev[power_idx])

        kills_latest = to_int(row_latest[kills_idx])
        kills_gain = kills_latest - to_int(row_prev[kills_idx])

        healed_latest = to_int(row_latest[healed_idx])
        healed_gain = healed_latest - to_int(row_prev[healed_idx])

        dead_latest = to_int(row_latest[dead_idx])
        dead_gain = dead_latest - to_int(row_prev[dead_idx])

        # Rankings by gain within MFD only
        def build_rank_map(index):
            return sorted([
                (row[alliance_index], row[name_index], to_int(row[index]) - to_int(prev_map[row[id_index]][index]))
                for row in data_latest[1:]
                if row[id_index] in prev_map and row[alliance_index].strip() == "MFD"
            ], key=lambda x: x[2], reverse=True)

        power_rank = [x[1] for x in build_rank_map(power_idx)].index(name) + 1 if alliance == "MFD" else None
        kills_rank = [x[1] for x in build_rank_map(kills_idx)].index(name) + 1 if alliance == "MFD" else None
        dead_rank = [x[1] for x in build_rank_map(dead_idx)].index(name) + 1 if alliance == "MFD" else None
        healed_rank = [x[1] for x in build_rank_map(healed_idx)].index(name) + 1 if alliance == "MFD" else None

        msg = f"📊 Stats for `{lord_id}` ({name})\n"
        msg += f"🔹 Alliance: [{alliance}]\n\n"
        msg += f"🏆 Power:  {power_latest:,} (+{power_gain:,})"
        msg += f" — Rank #{power_rank} in MFD\n" if power_rank else "\n"

        msg += f"⚔️ Kills:  {kills_latest:,} (+{kills_gain:,})"
        msg += f" — Rank #{kills_rank} in MFD\n" if kills_rank else "\n"

        msg += f"💀 Dead:   {dead_latest:,} (+{dead_gain:,})"
        msg += f" — Rank #{dead_rank} in MFD\n" if dead_rank else "\n"

        msg += f"💉 Healed: {healed_latest:,} (+{healed_gain:,})"
        msg += f" — Rank #{healed_rank} in MFD" if healed_rank else ""

        if alliance != "MFD":
            msg += "\n\n❌ Not in MFD — Ranks not available."

        await ctx.send(msg)

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def mana(ctx, lord_id: str):
    try:
        sheets = client.open("Copy SoS5").worksheets()
        if len(sheets) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest, previous = sheets[-1], sheets[-2]
        data_latest, data_prev = latest.get_all_values(), previous.get_all_values()
        headers = data_latest[0]

        id_index = headers.index("lord_id")
        name_index = 1
        alliance_index = 3
        mana_index = 26  # Column AA

        def to_int(val):
            try: return int(val.replace(',', '').replace('-', '').strip())
            except: return 0

        def find_row(data):
            for row in data[1:]:
                if len(row) > mana_index and row[id_index].strip() == lord_id:
                    return row
            return None

        row_latest = find_row(data_latest)
        row_prev = find_row(data_prev)

        if not row_latest or not row_prev:
            await ctx.send("❌ Lord ID not found in both sheets.")
            return

        alliance = row_latest[alliance_index].strip() if len(row_latest) > alliance_index else ""
        name = row_latest[name_index].strip()
        mana_gain = to_int(row_latest[mana_index]) - to_int(row_prev[mana_index])

        # Filter MFD players only for ranking
        mfd_gains = []
        for row in data_latest[1:]:
            if len(row) > mana_index and row[alliance_index].strip() == "MFD":
                id_val = row[id_index].strip()
                row_old = next((r for r in data_prev[1:] if len(r) > mana_index and r[id_index].strip() == id_val), None)
                if row_old:
                    gain = to_int(row[mana_index]) - to_int(row_old[mana_index])
                    mfd_gains.append((id_val, gain))

        mfd_gains.sort(key=lambda x: x[1], reverse=True)
        rank = next((i+1 for i, (lid, _) in enumerate(mfd_gains) if lid == lord_id), None)

        message = f"🌿 Mana gathered by `[{alliance}] {name}`:\n💧 Mana: {mana_gain:,}"
        if alliance == "MFD" and rank:
            message += f"\n🏅 MFD Rank: #{rank}"
        else:
            message += "\n❌ Not in MFD"

        await ctx.send(message)

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def topmana(ctx, top_n: int = 10):
    try:
        sheets = client.open("Copy SoS5").worksheets()
        if len(sheets) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = sheets[-1]
        previous = sheets[-2]

        data_latest = latest.get_all_values()
        data_prev = previous.get_all_values()
        headers = data_latest[0]

        id_index = headers.index("lord_id")
        name_index = 1
        alliance_index = 3
        mana_idx = 26  # Column AA
        power_idx = 12  # Column M

        def to_int(val):
            try: return int(val.replace(',', '').replace('-', '').strip())
            except: return 0

        prev_map = {
            row[id_index]: {
                "mana": to_int(row[mana_idx])
            }
            for row in data_prev[1:]
            if len(row) > mana_idx and row[id_index]
        }

        gains = []

        for row in data_latest[1:]:
            if len(row) > max(mana_idx, power_idx):
                lord_id = row[id_index]
                alliance = row[alliance_index].strip() if len(row) > alliance_index else ""
                name = f"[{alliance}] {row[name_index].strip()}"
                if lord_id not in prev_map:
                    continue  # skip if not in previous sheet

                mana_now = to_int(row[mana_idx])
                mana_prev = prev_map[lord_id]["mana"]
                gain = mana_now - mana_prev
                power = to_int(row[power_idx])

                if power >= 25_000_000:
                    gains.append((name, gain))

        gains.sort(key=lambda x: x[1], reverse=True)
        result = "\n".join([f"{i+1}. `{name}` — 💧 +{mana:,}" for i, (name, mana) in enumerate(gains[:top_n])])

        await ctx.send(f"📊 **Top {top_n} Mana Gains** (≥25M Power)\n`{previous.title}` → `{latest.title}`:\n{result}")

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def topheal(ctx, top_n: int = 10):
    try:
        sheets = client.open("Copy SoS5").worksheets()
        if len(sheets) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = sheets[-1]
        previous = sheets[-2]

        data_latest = latest.get_all_values()
        data_prev = previous.get_all_values()
        headers = data_latest[0]

        id_index = headers.index("lord_id")
        name_index = 1
        alliance_index = 3
        heal_idx = 18   # Column S
        power_idx = 12  # Column M

        def to_int(val):
            try: return int(val.replace(',', '').replace('-', '').strip())
            except: return 0

        # Clean and map previous sheet IDs
        prev_map = {}
        for row in data_prev[1:]:
            if len(row) > heal_idx:
                raw_id = row[id_index].strip() if row[id_index] else ""
                if raw_id:
                    prev_map[raw_id] = to_int(row[heal_idx])

        gains = []
        for row in data_latest[1:]:
            if len(row) > max(heal_idx, power_idx):
                raw_id = row[id_index].strip() if row[id_index] else ""
                if raw_id not in prev_map:
                    continue  # skip if not in both

                alliance = row[alliance_index].strip() if len(row) > alliance_index else ""
                name = f"[{alliance}] {row[name_index].strip()}"
                healed_now = to_int(row[heal_idx])
                healed_prev = prev_map[raw_id]
                gain = healed_now - healed_prev
                power = to_int(row[power_idx])

                if power >= 25_000_000:
                    gains.append((name, gain))

        gains.sort(key=lambda x: x[1], reverse=True)
        result = "\n".join([f"{i+1}. `{name}` — ❤️‍🩹 +{heal:,}" for i, (name, heal) in enumerate(gains[:top_n])])

        await ctx.send(f"📊 **Top {top_n} Healers (Gain)** (≥25M Power)\n`{previous.title}` → `{latest.title}`:\n{result}")

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def toprssheal(ctx, top_n: int = 10):
    try:
        sheets = client.open("Copy SoS5").worksheets()
        if len(sheets) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = sheets[-1]
        previous = sheets[-2]

        data_latest = latest.get_all_values()
        data_prev = previous.get_all_values()
        headers = data_latest[0]

        id_index = headers.index("lord_id")
        name_index = 1
        alliance_index = 3
        power_idx = 12  # Column M

        gold_idx = 31  # AF
        wood_idx = 32  # AG
        ore_idx  = 33  # AH
        mana_idx = 34  # AI

        def to_int(val):
            try: return int(val.replace(',', '').replace('-', '').strip())
            except: return 0

        # Build previous sheet map
        prev_map = {}
        for row in data_prev[1:]:
            if len(row) > mana_idx:
                raw_id = row[id_index].strip() if row[id_index] else ""
                if raw_id:
                    prev_map[raw_id] = {
                        "gold": to_int(row[gold_idx]),
                        "wood": to_int(row[wood_idx]),
                        "ore": to_int(row[ore_idx]),
                        "mana": to_int(row[mana_idx])
                    }

        gains = []
        for row in data_latest[1:]:
            if len(row) > mana_idx:
                raw_id = row[id_index].strip() if row[id_index] else ""
                if raw_id not in prev_map:
                    continue  # Skip users not in both sheets

                power = to_int(row[power_idx])
                if power < 25_000_000:
                    continue

                name = row[name_index].strip() if len(row) > name_index else "?"
                alliance = row[alliance_index].strip() if len(row) > alliance_index else ""
                full_name = f"[{alliance}] {name}"

                gold = to_int(row[gold_idx]) - prev_map[raw_id]["gold"]
                wood = to_int(row[wood_idx]) - prev_map[raw_id]["wood"]
                ore  = to_int(row[ore_idx])  - prev_map[raw_id]["ore"]
                mana = to_int(row[mana_idx]) - prev_map[raw_id]["mana"]
                total = gold + wood + ore + mana

                gains.append((full_name, total, gold, wood, ore, mana))

        gains.sort(key=lambda x: x[1], reverse=True)
        result = "\n".join([
            f"{i+1}. `{name}` — 💸 +{total:,} (🪙{gold:,} 🪵{wood:,} ⛏️{ore:,} 💧{mana:,})"
            for i, (name, total, gold, wood, ore, mana) in enumerate(gains[:top_n])
        ])

        await ctx.send(f"📊 **Top {top_n} RSS Heal Gains** (≥25M Power)\n`{previous.title}` → `{latest.title}`:\n{result}")

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def kills(ctx, lord_id: str):
    try:
        sheets = client.open("Copy SoS5").worksheets()
        if len(sheets) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = sheets[-1]
        previous = sheets[-2]

        data_latest = latest.get_all_values()
        data_prev = previous.get_all_values()
        headers = data_latest[0]

        id_index = headers.index("lord_id")
        name_index = 1
        alliance_index = 3
        power_index = 12

        total_idx = 9    # Column J
        t5_idx = 36      # Column AK
        t4_idx = 37      # Column AL
        t3_idx = 38      # Column AM
        t2_idx = 39      # Column AN
        t1_idx = 40      # Column AO

        def to_int(val):
            try:
                return int(val.replace(',', '').replace('-', '').strip())
            except:
                return 0

        def find_row(data):
            for row in data[1:]:
                if row[id_index] == lord_id:
                    return row
            return None

        row_latest = find_row(data_latest)
        row_prev = find_row(data_prev)

        if not row_latest or not row_prev:
            await ctx.send("❌ Lord ID not found in both sheets.")
            return

        power = to_int(row_latest[power_index])
        if power < 25_000_000:
            await ctx.send("❌ Player is below 25M power.")
            return

        name = row_latest[name_index].strip()
        alliance = row_latest[alliance_index].strip()
        tag = f"[{alliance}] {name}"

        def get_diff(idx):
            return to_int(row_latest[idx]) - to_int(row_prev[idx])

        def get_now(idx):
            return to_int(row_latest[idx])

        total = get_now(total_idx)
        total_diff = get_diff(total_idx)
        t5 = get_now(t5_idx)
        t5_diff = get_diff(t5_idx)
        t4 = get_now(t4_idx)
        t4_diff = get_diff(t4_idx)
        t3 = get_now(t3_idx)
        t3_diff = get_diff(t3_idx)
        t2 = get_now(t2_idx)
        t2_diff = get_diff(t2_idx)
        t1 = get_now(t1_idx)
        t1_diff = get_diff(t1_idx)

        await ctx.send(
            f"📊 **Kill Stats for `{tag}`**\n"
            f"`{previous.title}` → `{latest.title}`\n\n"
            f"⚔️ **Total:** {total:,} (+{total_diff:,})\n"
            f"T5: {t5:,} (+{t5_diff:,})\n"
            f"T4: {t4:,} (+{t4_diff:,})\n"
            f"T3: {t3:,} (+{t3_diff:,})\n"
            f"T2: {t2:,} (+{t2_diff:,})\n"
            f"T1: {t1:,} (+{t1_diff:,})"
        )

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def topkills(ctx, top_n: int = 10):
    try:
        sheets = client.open("Copy SoS5").worksheets()
        if len(sheets) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = sheets[-1]
        previous = sheets[-2]
        data_latest = latest.get_all_values()
        data_prev = previous.get_all_values()
        headers = data_latest[0]

        id_index = headers.index("lord_id")
        name_index = 1  # Column B
        alliance_index = 3  # Column D
        power_index = 12  # Column M
        kills_index = 9   # Column J

        def to_int(val):
            try: return int(val.replace(",", "").replace("-", "").strip())
            except: return 0

        # Build map from previous sheet
        prev_map = {
            row[id_index].strip(): to_int(row[kills_index])
            for row in data_prev[1:]
            if len(row) > kills_index and row[id_index].strip()
        }

        gains = []
        for row in data_latest[1:]:
            if len(row) <= kills_index:
                continue

            raw_id = row[id_index].strip()
            if not raw_id or raw_id not in prev_map:
                continue

            power = to_int(row[power_index])
            if power < 25_000_000:
                continue

            name = row[name_index].strip()
            alliance = row[alliance_index].strip()
            kills_now = to_int(row[kills_index])
            kills_then = prev_map[raw_id]
            gain = kills_now - kills_then

            full_name = f"[{alliance}] {name}"
            gains.append((full_name, gain))

        gains.sort(key=lambda x: x[1], reverse=True)

        lines = [
            f"{i+1}. `{name}` — ⚔️ +{gain:,}"
            for i, (name, gain) in enumerate(gains[:top_n])
        ]

        await ctx.send("**🏆 Top Kill Gains:**\n" + "\n".join(lines))

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def topdeads(ctx, top_n: int = 10):
    try:
        sheets = client.open("Copy SoS5").worksheets()
        if len(sheets) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest = sheets[-1]
        previous = sheets[-2]

        data_latest = latest.get_all_values()
        data_prev = previous.get_all_values()
        headers = data_latest[0]

        id_index = headers.index("lord_id")
        name_index = 1
        alliance_index = 3
        power_index = 12  # Column M
        dead_index = 17   # Column R

        def to_int(val):
            try:
                return int(val.replace(',', '').replace('-', '').strip())
            except:
                return 0

        prev_map = {}
        for row in data_prev[1:]:
            if len(row) > dead_index:
                raw_id = row[id_index].strip()
                if raw_id:
                    prev_map[raw_id] = to_int(row[dead_index])

        results = []
        for row in data_latest[1:]:
            if len(row) > dead_index:
                raw_id = row[id_index].strip()
                if raw_id not in prev_map:
                    continue

                power = to_int(row[power_index])
                if power < 25_000_000:
                    continue

                dead_now = to_int(row[dead_index])
                dead_then = prev_map[raw_id]
                gain = dead_now - dead_then

                name = row[name_index].strip()
                alliance = row[alliance_index].strip()
                full_name = f"[{alliance}] {name}"
                results.append((full_name, gain))

        results.sort(key=lambda x: x[1], reverse=True)
        output = "\n".join([f"{i+1}. `{name}` — 💀 +{gain:,}" for i, (name, gain) in enumerate(results[:top_n])])

        if not output:
            await ctx.send("No valid data found.")
        else:
            await ctx.send(f"**🏆 Top {top_n} Dead Units Gained:**\n{output}")

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
async def progress(ctx, lord_id: str):
    try:
        sheets = client.open("Copy SoS5").worksheets()
        if len(sheets) < 2:
            await ctx.send("❌ Not enough sheets to compare.")
            return

        latest, previous = sheets[-1], sheets[-2]
        latest_data, previous_data = latest.get_all_values(), previous.get_all_values()
        headers = latest_data[0]

        def get_index(col):
            if len(col) == 1:
                return ord(col) - ord("A")
            elif len(col) == 2:
                return (ord(col[0]) - ord("A") + 1) * 26 + ord(col[1]) - ord("A")
            return -1

        id_idx = headers.index("lord_id")
        name_idx = 1
        alliance_idx = 3
        power_idx = get_index("M")
        kills_idx = get_index("J")
        dead_idx = get_index("R")
        healed_idx = get_index("S")
        gold_idx = get_index("AF")
        wood_idx = get_index("AG")
        ore_idx = get_index("AH")
        mana_idx = get_index("AI")

        def find_row(data):
            for row in data[1:]:
                if row[id_idx] == lord_id:
                    return row
            return None

        def to_int(val):
            try:
                return int(val.replace(",", "").strip())
            except:
                return 0

        row_latest = find_row(latest_data)
        row_prev = find_row(previous_data)

        if not row_latest or not row_prev:
            await ctx.send("❌ Lord ID not found in both sheets.")
            return

        name = row_latest[name_idx]
        alliance = row_latest[alliance_idx]
        power_gain = to_int(row_latest[power_idx]) - to_int(row_prev[power_idx])
        kills_gain = to_int(row_latest[kills_idx]) - to_int(row_prev[kills_idx])
        dead_gain = to_int(row_latest[dead_idx]) - to_int(row_prev[dead_idx])
        healed_gain = to_int(row_latest[healed_idx]) - to_int(row_prev[healed_idx])
        gold = to_int(row_latest[gold_idx]) - to_int(row_prev[gold_idx])
        wood = to_int(row_latest[wood_idx]) - to_int(row_prev[wood_idx])
        ore = to_int(row_latest[ore_idx]) - to_int(row_prev[ore_idx])
        mana = to_int(row_latest[mana_idx]) - to_int(row_prev[mana_idx])
        total_rss = gold + wood + ore + mana

        # Rankings
        mfd_gains = {
            "rss": [],
            "power": [],
            "kills": [],
            "dead": [],
            "healed": []
        }

        for row_l, row_p in zip(latest_data[1:], previous_data[1:]):
            if row_l[alliance_idx] != "MFD":
                continue
            lid = row_l[id_idx]
            mfd_gains["rss"].append((lid, (
                to_int(row_l[gold_idx]) - to_int(row_p[gold_idx]) +
                to_int(row_l[wood_idx]) - to_int(row_p[wood_idx]) +
                to_int(row_l[ore_idx]) - to_int(row_p[ore_idx]) +
                to_int(row_l[mana_idx]) - to_int(row_p[mana_idx])
            )))
            mfd_gains["power"].append((lid, to_int(row_l[power_idx]) - to_int(row_p[power_idx])))
            mfd_gains["kills"].append((lid, to_int(row_l[kills_idx]) - to_int(row_p[kills_idx])))
            mfd_gains["dead"].append((lid, to_int(row_l[dead_idx]) - to_int(row_p[dead_idx])))
            mfd_gains["healed"].append((lid, to_int(row_l[healed_idx]) - to_int(row_p[healed_idx])))

        def find_rank(lid, category):
            lst = sorted(mfd_gains[category], key=lambda x: x[1], reverse=True)
            for i, (check_id, _) in enumerate(lst, 1):
                if check_id == lid:
                    return i
            return None

        timeframe = f"{previous.title} → {latest.title}"
        embed = discord.Embed(
            title=f"📈 Progress Report for 〔{alliance}〕 {name}",
            color=discord.Color.green()
        )
        embed.add_field(name="🟩 Power", value=f"+{power_gain:,}" + (f"\n🏅Rank: #{find_rank(lord_id, 'power')}" if alliance == "MFD" else ""), inline=False)
        embed.add_field(name="⚔️ Kills", value=f"+{kills_gain:,}" + (f"\n🏅Rank: #{find_rank(lord_id, 'kills')}" if alliance == "MFD" else ""), inline=False)
        embed.add_field(name="💀 Dead", value=f"+{dead_gain:,}" + (f"\n🏅Rank: #{find_rank(lord_id, 'dead')}" if alliance == "MFD" else ""), inline=False)
        embed.add_field(name="❤️ Healed", value=f"+{healed_gain:,}" + (f"\n🏅Rank: #{find_rank(lord_id, 'healed')}" if alliance == "MFD" else ""), inline=False)

        rss_text = (
            f"🪙 Gold: {gold:,}\n"
            f"🪵 Wood: {wood:,}\n"
            f"⛏️ Ore: {ore:,}\n"
            f"💧 Mana: {mana:,}\n"
            f"📦 Total: {total_rss:,}"
        )
        if alliance == "MFD":
            rank = find_rank(lord_id, "rss")
            if rank:
                rss_text += f"\n🏅Rank: #{rank}"

        embed.add_field(name="📦 RSS Spent", value=rss_text, inline=False)
        embed.set_footer(text=f"Timespan: {timeframe}")
        await ctx.send(embed=embed)

    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

import os
TOKEN = os.getenv("TOKEN")

# Replace with your actual war-status channel ID
CHANNEL_ID = 1369071691111600168

# Allowed role ID
ALLOWED_ROLE_ID = 1258156722871337021

# Permission check decorator
def role_check():
    async def predicate(ctx):
        if any(role.id == ALLOWED_ROLE_ID for role in ctx.author.roles):
            return True
        await ctx.send("❌ You don’t have permission to use this command.")
        return False
    return commands.check(predicate)


@bot.event
async def on_ready():
    print(f'✅ Bot is online as {bot.user}')


@bot.command()
@role_check()
async def warred(ctx):
    await ctx.send("✅ Command received: Setting status to 🔴 FULL WAR...\n⚠️ *Channel rename may be delayed due to Discord rate limits.*")
    channel = bot.get_channel(CHANNEL_ID)
    if channel:
        try:
            await channel.edit(name="〘🔴〙war-status-fullwar")
        except discord.errors.HTTPException as e:
            print(f"Rename failed or delayed: {e}")


@bot.command()
@role_check()
async def waryellow(ctx):
    await ctx.send("✅ Command received: Setting status to 🟡 Skirmishes...\n⚠️ *Channel rename may be delayed due to Discord rate limits.*")
    channel = bot.get_channel(CHANNEL_ID)
    if channel:
        try:
            await channel.edit(name="〘🟡〙war-status-skirmishes")
        except discord.errors.HTTPException as e:
            print(f"Rename failed or delayed: {e}")


@bot.command()
@role_check()
async def wargreen(ctx):
    await ctx.send("✅ Command received: Setting status to 🟢 No Fighting...\n⚠️ *Channel rename may be delayed due to Discord rate limits.*")
    channel = bot.get_channel(CHANNEL_ID)
    if channel:
        try:
            await channel.edit(name="〘🟢〙war-status-no-fighting")
        except discord.errors.HTTPException as e:
            print(f"Rename failed or delayed: {e}")


@bot.command()
@role_check()
async def warfarm(ctx):
    await ctx.send("🌾 Status set to *Go Farm Mana*.\nEnemies are too bad, dodged again — smh.\nGo stack that mana, RSS heal is expensive.")
    channel = bot.get_channel(CHANNEL_ID)
    if channel:
        try:
            await channel.edit(name="〘🌾〙war-status-go-farm")
        except discord.errors.HTTPException as e:
            print(f"Rename failed or delayed: {e}")

@bot.command()
async def commands(ctx):
    channel_id = ctx.channel.id
    allowed_channel_id = 1378735765827358791  # your allowed channel ID

    if channel_id != allowed_channel_id:
        await ctx.send(f"❌ Commands are only allowed in <#{allowed_channel_id}>.")
        return

    help_text = """
📜 **Available Commands:**

**🟣 War Status**
- `!warred` — Set status to 🔴 FULL WAR
- `!waryellow` — Set status to 🟡 Skirmishes
- `!wargreen` — Set status to 🟢 No Fighting
- `!warfarm` — Set status to 🌾 Go Farm Mana

**📊 Player Stats**
- `!rssheal [lord_id]` — Show RSS spent on healing between last two sheets
- `!stats [lord_id]` — Show power, kills, healed, dead stats + gain + MFD rank
- `!kills [lord_id]` — Show kills and troop tier breakdown
- `!mana [lord_id]` — Mana gathered + gain + MFD rank
- `!progress [lord_id]` — Full profile: power, kills, dead, heal, RSS, mana (+gain & rank)

**🏆 Leaderboards**
- `!topmana` — Top mana gatherers
- `!topheal` — Top units healed
- `!toprssheal` — Top RSS heal spenders
- `!topkills` — Top kill gainers
- `!topdeads` — Top dead units
"""
    await ctx.send(help_text)

bot.run(TOKEN)
