# War Status Bot

A Discord bot for running an alliance during a Call of Dragons season: war stats and leaderboards from daily scans and merits exports, head-to-head matchups, a daily report, shell build and ability tracking, an RSS selling queue, translation, activity checks and leak tracing.

## Files

| File | What it does |
| --- | --- |
| `main.py` | The bot: stats and leaderboards, matchups, daily report, war/shell channels, RSS queue, translator, help |
| `db.py` | Postgres storage for scans and merits exports, exclusions, settings and `!link` |
| `lb.py` | Shared leaderboard engine: argument parsing, ranking, paged embeds |
| `dashboard.py` | Alliance ability dashboard and officer panel (cog) |
| `activity_checks.py` | DM checks with buttons and a live dashboard (cog): `!activitycheck` (attending / not / maybe) and `!seasoncheck` (staying / leaving or quitting / don't know yet) |
| `spydetect.py` | Fingerprinted broadcasts and leak attribution (cog) |

## Environment variables

| Name | Required | Purpose |
| --- | --- | --- |
| `TOKEN` | yes | Discord bot token |
| `DATABASE_URL` | yes | Postgres connection string |
| `CREDENTIALS_JSON` | yes | Google service-account JSON, for the older seasons kept in Google Sheets |
| `DATA_DIR` | no | Where the JSON state files go. Defaults to `/data` when it exists, else the working directory |
| `ACTIVITY_DB_PATH` | no | SQLite file for activity checks. Default `/data/activity_checks.db` |
| `DEEPL_API_KEY` | no | Makes translation more reliable (free DeepL key) |
| `MYMEMORY_EMAIL` | no | Raises the MyMemory translation fallback's free limit |
| `OPSEC_FINGERPRINT_SECRET` | no | Secret for broadcast fingerprints. Falls back to the built-in value; changing it changes every phrase, so older broadcasts can then only be traced with the old secret |

The bot needs the **Members** and **Message Content** privileged intents, and the `applications.commands` scope for the right-click **Translate** command.

## Where data lives

- **Postgres:** scans, merits exports, exclusions, `!link`, report settings
- **`DATA_DIR`:** `shell_state.json`, `rss_queue.json`, `dashboard_data.json`
- **`ACTIVITY_DB_PATH`:** activity checks

Use a persistent volume for `/data`, or everything in it resets on each deploy. If a JSON state file is ever unreadable, the bot keeps it as `<name>.corrupt-<time>` and starts fresh, so it can be recovered by hand.

## Daily workflow

- **Merits exports:** the export script posts each server's `.xlsx` (named like `375_2026-10-16_2026-10-20.xlsx`) through a webhook in the ingest channel, and the bot stores it automatically. By hand: attach the files to `!ingestmerits`. Always export with the season start as the start date.
- **Scans:** attach the CSV to `!ingest` (season defaults to the current one).
- **Daily report:** goes to the channel set with `!reportchannel #channel` once each day's export is in (not before 10:00 UTC).
- **Reminders:** from 15:00 UTC, the bot pings `SCAN_REMINDER_USER_ID` once the newest scan is 3 days old (`SCAN_REMINDER_AFTER_DAYS`), and the scan admin role if yesterday's export is missing.

## New season

1. Add the season to `SEASON_SHEETS` (value `None` if it only lives in the database).
2. Add a block to `SEASON_CONFIG` with its label, start and end dates, home server and matchups.
3. Add any new servers to `ALL_SERVERS` and `SERVER_COLORS`.
4. Change `DEFAULT_SEASON`, and update `TEAM_ROSTER` and `DEAD_REQUIREMENTS` if they change.

## Commands

`!help` in a stats channel browses everything, and `!help <command>` explains one.
