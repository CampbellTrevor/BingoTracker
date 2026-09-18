"""Refresh the bundled Wise Old Man gains cache for the default event CSV."""

from __future__ import annotations

import argparse
import ast
import csv
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


APP_DIR = Path(__file__).resolve().parent
DEFAULT_APP_FILE = APP_DIR / "bingostats.py"
DEFAULT_CSV_FILE = APP_DIR / "Summer Bingo 2026 Event Log.csv"
DEFAULT_OUTPUT_FILE = APP_DIR / "wom_group_cache.json"
WOM_API_BASE_URL = "https://api.wiseoldman.net/v2"


def _literal_assignment(source_path: Path, variable_name: str):
    tree = ast.parse(source_path.read_text(encoding="utf-8-sig"))
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if any(
            isinstance(target, ast.Name) and target.id == variable_name
            for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise ValueError(f"Could not find {variable_name} in {source_path.name}")


def _parse_event_datetime(value: str) -> datetime:
    cleaned = str(value or "").strip()
    for date_format in (
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(cleaned, date_format)
        except ValueError:
            continue
    raise ValueError(f"Unsupported event date: {cleaned!r}")


def _event_date_range(csv_path: Path) -> tuple[str, str]:
    dates: list[datetime] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("Date"):
                dates.append(_parse_event_datetime(row["Date"]))
    if not dates:
        raise ValueError(f"No valid Date values found in {csv_path.name}")
    return min(dates).date().isoformat(), max(dates).date().isoformat()


def _normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _event_player_lookups(csv_path: Path, app_file: Path) -> dict[str, str]:
    aliases = _literal_assignment(app_file, "WOM_PLAYER_ALIASES")
    aliases_by_key = {
        _normalize_name(source): str(target).strip()
        for source, target in aliases.items()
    }
    players: dict[str, str] = {}
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            raw_name = str(row.get("Player Name") or "").strip()
            if not raw_name:
                continue
            query_name = aliases_by_key.get(_normalize_name(raw_name), raw_name)
            players[_normalize_name(query_name)] = query_name
    return players


def _requested_metrics(app_file: Path) -> set[str]:
    supported = set(_literal_assignment(app_file, "SUPPORTED_WOM_BOSS_METRICS"))
    category_map = _literal_assignment(app_file, "CATEGORY_TO_WOM_BOSSES")
    return {
        metric
        for category_metrics in category_map.values()
        for metric in category_metrics
        if metric in supported
    }


def _fetch_bulk_gains(group_id: int, start_date: str, end_date: str):
    query = urlencode({"startDate": start_date, "endDate": end_date})
    url = f"{WOM_API_BASE_URL}/groups/{group_id}/bulk-gained?{query}"
    request = Request(url, headers={"User-Agent": "Bapheads-BingoStats/1.0"})
    with urlopen(request, timeout=120) as response:
        return json.load(response)


def _fetch_player_gains(username: str, start_date: str, end_date: str):
    query = urlencode({"startDate": start_date, "endDate": end_date})
    encoded_username = quote(username, safe="")
    url = f"{WOM_API_BASE_URL}/players/{encoded_username}/gained?{query}"
    request = Request(url, headers={"User-Agent": "Bapheads-BingoStats/1.0"})
    with urlopen(request, timeout=60) as response:
        return json.load(response)


def _bulk_player_keys(rows) -> set[str]:
    keys: set[str] = set()
    for row in rows:
        player = row.get("player") if isinstance(row, dict) else None
        if not isinstance(player, dict):
            continue
        player_name = (
            player.get("username")
            or player.get("displayName")
            or player.get("name")
        )
        if player_name:
            keys.add(_normalize_name(player_name))
    return keys


def merge_player_gains(
    cache_payload: dict,
    player_key: str,
    player_gains: dict,
    requested_metrics: set[str],
) -> None:
    data = player_gains.get("data", {}) if isinstance(player_gains, dict) else {}
    bosses = data.get("bosses", {}) if isinstance(data, dict) else {}
    if not isinstance(bosses, dict):
        raise ValueError(f"Unexpected player-gains response for {player_key}")

    for metric_name in requested_metrics:
        metric_row = bosses.get(metric_name, {})
        kills = metric_row.get("kills", {}) if isinstance(metric_row, dict) else {}
        gained = kills.get("gained", 0) if isinstance(kills, dict) else 0
        numeric_gain = float(gained or 0)
        if numeric_gain.is_integer():
            numeric_gain = int(numeric_gain)
        if numeric_gain:
            cache_payload["metrics"][metric_name][player_key] = numeric_gain


def build_cache_payload(
    rows,
    group_id: int,
    start_date: str,
    end_date: str,
    requested_metrics: set[str],
) -> dict:
    metric_maps = {metric: {} for metric in sorted(requested_metrics)}
    metrics_seen: set[str] = set()

    for row in rows:
        if not isinstance(row, dict):
            continue
        player = row.get("player")
        if not isinstance(player, dict):
            continue
        player_name = (
            player.get("username")
            or player.get("displayName")
            or player.get("name")
        )
        player_key = _normalize_name(player_name)
        if not player_key:
            continue

        for metric_row in row.get("data", []):
            if not isinstance(metric_row, dict):
                continue
            metric_name = metric_row.get("metric")
            if metric_name not in metric_maps:
                continue
            gained = metric_row.get("gained", 0) or 0
            numeric_gain = float(gained)
            if numeric_gain.is_integer():
                numeric_gain = int(numeric_gain)
            metrics_seen.add(metric_name)
            # Missing player keys already resolve to zero in the app, so keep
            # only actual gains in the committed cache.
            if numeric_gain:
                metric_maps[metric_name][player_key] = numeric_gain

    missing_metrics = sorted(requested_metrics - metrics_seen)
    if missing_metrics:
        raise ValueError(
            "Wise Old Man response did not include mapped metrics: "
            + ", ".join(missing_metrics)
        )

    return {
        "group_id": group_id,
        "start_date": start_date,
        "end_date": end_date,
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace(
            "+00:00", "Z"
        ),
        "metrics": metric_maps,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV_FILE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_FILE)
    parser.add_argument("--app-file", type=Path, default=DEFAULT_APP_FILE)
    parser.add_argument("--group-id", type=int, default=11794)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    args = parser.parse_args()

    derived_start, derived_end = _event_date_range(args.csv)
    start_date = args.start_date or derived_start
    end_date = args.end_date or derived_end
    if (args.start_date is None) != (args.end_date is None):
        parser.error("--start-date and --end-date must be supplied together")

    requested_metrics = _requested_metrics(args.app_file)
    rows = _fetch_bulk_gains(args.group_id, start_date, end_date)
    if not isinstance(rows, list):
        raise ValueError("Unexpected Wise Old Man bulk-gained response format")

    payload = build_cache_payload(
        rows,
        args.group_id,
        start_date,
        end_date,
        requested_metrics,
    )

    # The group endpoint is authoritative for current members. A participant
    # can leave the group after submitting, so make one player-level request
    # only for each event name absent from the bulk response.
    bulk_player_keys = _bulk_player_keys(rows)
    event_players = _event_player_lookups(args.csv, args.app_file)
    supplemented_players: list[str] = []
    unavailable_players: list[str] = []
    for player_key, query_name in sorted(event_players.items()):
        if player_key in bulk_player_keys:
            continue
        try:
            player_gains = _fetch_player_gains(query_name, start_date, end_date)
        except HTTPError as exc:
            if exc.code == 404:
                unavailable_players.append(query_name)
                continue
            raise
        merge_player_gains(
            payload,
            player_key,
            player_gains,
            requested_metrics,
        )
        supplemented_players.append(query_name)

    args.output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        f"Wrote {len(payload['metrics'])} metrics for {len(rows)} group members "
        f"to {args.output} ({start_date}..{end_date})."
    )
    if supplemented_players:
        print("Supplemented former/non-group players: " + ", ".join(supplemented_players))
    if unavailable_players:
        print(
            "No Wise Old Man profile found: " + ", ".join(unavailable_players),
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
