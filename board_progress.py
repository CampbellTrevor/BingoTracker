from __future__ import annotations

import base64
import html
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class BoardTile:
    """One physical slot on the bingo board image."""

    tile_id: str
    label: str
    canonical_key: str
    stage: str
    left: float
    top: float
    width: float
    height: float
    always_available: bool = False


STAGE_DETAILS = {
    "start": {
        "label": "Opening path",
        "unlock": "Starts at the left side of the board",
    },
    "grid_one": {
        "label": "First grid",
        "unlock": "Complete the full grid to progress (any order)",
    },
    "ordered_path": {
        "label": "Ordered path",
        "unlock": "Voidwaker → PNM/Nightmare → COX (must be in order)",
    },
    "final_grid": {
        "label": "Final grid",
        "unlock": "Reached after the ordered path; grid completion is any order",
    },
    "bonus": {
        "label": "Bonus tile",
        "unlock": "Unlocks after the final grid",
    },
}


# Alias matching is intentionally conservative. Broad legacy categories such as
# "Barrows / Moons" and "Colo / Inferno" are ambiguous on this board, so they
# remain unmatched and are surfaced in the app's diagnostics instead of being
# silently credited to the wrong slot.
CANONICAL_ALIASES = {
    "toa": ("TOA", "Tombs of Amascut"),
    "nex": ("Nex",),
    "hueycoatl": ("Huey", "Hueycoatl", "The Hueycoatl"),
    "gwd": ("GWD", "God Wars", "God Wars Dungeon"),
    "venator_shards": ("Venator Shard", "Venator Shards"),
    "vorkath": ("Vorkath",),
    "dks": ("DKS", "Dagannoth Kings"),
    "revs": ("Revs", "Revenant", "Revenants"),
    "tob": ("TOB", "Theatre of Blood", "Theater of Blood"),
    "colosseum": ("Colo", "Colosseum", "Fortis Colosseum"),
    "zenytes": ("Zenyte", "Zenytes"),
    "royal_titans": ("Royal Titan", "Royal Titans", "The Royal Titans"),
    "barrows": ("Barrows",),
    "cerberus": ("Cerberus",),
    "the_mad_angel": ("Mad Angel", "The Mad Angel"),
    "voidwaker": ("Voidwaker",),
    "pnm_nightmare": (
        "PNM",
        "Nightmare",
        "Phosani's Nightmare",
        "Phosanis Nightmare",
        "PNM/Nightmare",
        "Nightmare / PNM",
    ),
    "cox": ("COX", "Chambers of Xeric"),
    "corrupted_gauntlet": ("CG", "Corrupted Gauntlet", "The Corrupted Gauntlet"),
    "araxxor": ("Araxxor",),
    "dt2": ("DT2", "DT2 Bosses", "Desert Treasure 2", "Desert Treasure 2 Bosses"),
    "doom": ("Doom", "Doom of Mokhaiotl"),
    "inferno": ("Inferno",),
    "yama": ("Yama",),
    "moons_of_peril": ("Moons", "Moons of Peril", "Perilous Moons"),
    "zulrah": ("Zulrah",),
    "maggot_king": ("Maggot King", "The Maggot King"),
    "corp_beast": ("Corp", "Corp Beast", "Corporeal Beast"),
}


def _tile(
    tile_id: str,
    label: str,
    canonical_key: str,
    stage: str,
    left: float,
    top: float,
    width: float = 6.457,
    height: float = 11.877,
    *,
    always_available: bool = False,
) -> BoardTile:
    return BoardTile(
        tile_id=tile_id,
        label=label,
        canonical_key=canonical_key,
        stage=stage,
        left=left,
        top=top,
        width=width,
        height=height,
        always_available=always_available,
    )


BOARD_TILES = (
    _tile("start_toa", "TOA", "toa", "start", 4.387, 46.628),
    _tile("start_nex", "NEX", "nex", "start", 10.762, 46.628),
    _tile("start_hueycoatl", "HUEYCOATL", "hueycoatl", "start", 17.301, 46.628),

    _tile("grid_gwd", "GWD", "gwd", "grid_one", 24.669, 34.164),
    _tile("grid_venator_shards", "VENATOR SHARDS", "venator_shards", "grid_one", 31.457, 34.164),
    _tile("grid_vorkath", "VORKATH", "vorkath", "grid_one", 38.245, 34.164),
    _tile("grid_dks", "DKS", "dks", "grid_one", 45.033, 34.164),
    _tile("grid_revs", "REVS", "revs", "grid_one", 24.669, 46.628),
    _tile("grid_tob", "TOB", "tob", "grid_one", 31.457, 46.628),
    _tile("grid_colosseum", "COLOSSEUM", "colosseum", "grid_one", 38.245, 46.628),
    _tile("grid_zenytes", "ZENYTES", "zenytes", "grid_one", 45.033, 46.628),
    _tile("grid_royal_titans", "ROYAL TITANS", "royal_titans", "grid_one", 24.669, 59.091),
    _tile("grid_barrows", "BARROWS", "barrows", "grid_one", 31.457, 59.091),
    _tile("grid_cerberus", "CERBERUS", "cerberus", "grid_one", 38.245, 59.091),
    _tile("grid_the_mad_angel", "THE MAD ANGEL", "the_mad_angel", "grid_one", 45.033, 59.091),

    _tile("path_voidwaker", "VOIDWAKER", "voidwaker", "ordered_path", 51.904, 46.628, 6.540),
    _tile("path_pnm_nightmare", "PNM/NIGHTMARE", "pnm_nightmare", "ordered_path", 58.444, 46.628),
    _tile("path_cox", "COX", "cox", "ordered_path", 64.901, 46.628, 6.374),

    _tile(
        "final_corrupted_gauntlet",
        "CORRUPTED GAUNTLET",
        "corrupted_gauntlet",
        "final_grid",
        72.103,
        34.311,
        6.540,
        always_available=True,
    ),
    _tile("final_araxxor", "ARAXXOR", "araxxor", "final_grid", 78.974, 34.311, 6.540),
    _tile("final_dt2", "DT2", "dt2", "final_grid", 85.844, 34.311, 6.540),
    _tile("final_tob", "TOB", "tob", "final_grid", 92.632, 34.311, 6.540),
    _tile("final_doom", "DOOM", "doom", "final_grid", 72.103, 46.628, 6.540),
    _tile("final_inferno", "INFERNO", "inferno", "final_grid", 78.974, 46.628, 6.540),
    _tile("final_yama", "YAMA", "yama", "final_grid", 85.844, 46.628, 6.540),
    _tile("final_cox", "COX", "cox", "final_grid", 92.632, 46.628, 6.540),
    _tile("final_moons_of_peril", "MOONS OF PERIL", "moons_of_peril", "final_grid", 72.103, 59.091, 6.540),
    _tile("final_zulrah", "ZULRAH", "zulrah", "final_grid", 78.974, 59.091, 6.540),
    _tile("final_maggot_king", "MAGGOT KING", "maggot_king", "final_grid", 85.844, 59.091, 6.540),
    _tile("final_toa", "TOA", "toa", "final_grid", 92.632, 59.091, 6.540),

    _tile("bonus_corp_beast", "CORP BEAST", "corp_beast", "bonus", 83.361, 78.299, 6.540),
)


def normalize_tile_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _build_alias_lookup() -> dict[str, str]:
    lookup: dict[str, str] = {}
    for canonical_key, aliases in CANONICAL_ALIASES.items():
        for alias in (canonical_key, *aliases):
            normalized = normalize_tile_name(alias)
            previous = lookup.get(normalized)
            if previous is not None and previous != canonical_key:
                raise ValueError(f"Board alias {alias!r} maps to both {previous!r} and {canonical_key!r}")
            lookup[normalized] = canonical_key
    return lookup


ALIAS_LOOKUP = _build_alias_lookup()
DUPLICATE_KEYS = {
    key for key, count in Counter(tile.canonical_key for tile in BOARD_TILES).items() if count > 1
}


def match_tile_key(value: Any) -> str | None:
    return ALIAS_LOOKUP.get(normalize_tile_name(value))


def load_tile_rules(rules_path: Path) -> dict[str, dict[str, Any]]:
    """Load the future completion-rule file without treating missing rules as complete."""

    try:
        payload = json.loads(rules_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}

    raw_rules = payload.get("tiles", {}) if isinstance(payload, dict) else {}
    if not isinstance(raw_rules, dict):
        return {}
    return {
        str(tile_id): rule
        for tile_id, rule in raw_rules.items()
        if isinstance(rule, dict)
    }


def _sorted_team_rows(df: pd.DataFrame, team: str) -> pd.DataFrame:
    team_rows = df[df["Team"].astype(str) == str(team)].copy()
    sort_columns = [column for column in ("Date", "Submission_Order") if column in team_rows.columns]
    if sort_columns:
        team_rows = team_rows.sort_values(sort_columns, kind="stable", na_position="last")
    return team_rows


def index_team_submissions(
    df: pd.DataFrame,
    team: str,
) -> tuple[dict[str, list[dict[str, Any]]], pd.DataFrame]:
    """Group raw submissions by board key; do not infer completion."""

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    unmatched_indices: list[Any] = []
    team_rows = _sorted_team_rows(df, team)

    for row_index, row in team_rows.iterrows():
        canonical_key = match_tile_key(row.get("Category"))
        if canonical_key is None:
            unmatched_indices.append(row_index)
            continue
        grouped[canonical_key].append(row.to_dict())

    unmatched = team_rows.loc[unmatched_indices].copy() if unmatched_indices else team_rows.iloc[0:0].copy()
    return dict(grouped), unmatched


def board_readiness_rows(rules: dict[str, dict[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for tile in BOARD_TILES:
        rule = rules.get(tile.tile_id, {})
        status = str(rule.get("status", "pending")).strip().title() or "Pending"
        aliases = ", ".join(CANONICAL_ALIASES[tile.canonical_key])
        unlock = STAGE_DETAILS[tile.stage]["unlock"]
        if tile.always_available:
            unlock = "Available from event start (one starting CG chest)"
        rows.append(
            {
                "Slot ID": tile.tile_id,
                "Board Tile": tile.label,
                "Section": STAGE_DETAILS[tile.stage]["label"],
                "Unlock": unlock,
                "CSV names recognized": aliases,
                "Completion rule": status,
            }
        )
    return rows


def _format_date(value: Any) -> str:
    if value is None or pd.isna(value):
        return "Date unavailable"
    timestamp = pd.Timestamp(value)
    if timestamp.hour or timestamp.minute or timestamp.second:
        return timestamp.strftime("%d %b %Y, %H:%M")
    return timestamp.strftime("%d %b %Y")


def _escape(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return html.escape(str(value), quote=True)


def _submission_markup(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<div class="bp-empty">No matching submissions yet.</div>'

    items: list[str] = []
    for row in rows:
        player = _escape(row.get("Player")) or "Unknown player"
        item = _escape(row.get("Item")) or "Item not supplied"
        date_text = _escape(_format_date(row.get("Date")))
        items.append(
            '<li><span class="bp-player">'
            + player
            + '</span><span class="bp-item">'
            + item
            + '</span><span class="bp-date">'
            + date_text
            + "</span></li>"
        )

    return '<ol class="bp-submissions">' + "".join(items) + "</ol>"


def _image_data_uri(image_path: Path) -> str:
    encoded = base64.b64encode(image_path.read_bytes()).decode("ascii")
    return f"data:image/png;base64,{encoded}"


def render_board_html(
    image_path: Path,
    grouped_submissions: dict[str, list[dict[str, Any]]],
    rules: dict[str, dict[str, Any]],
    team: str,
) -> str:
    """Render a rules-pending board preview with safe hover/focus details."""

    image_uri = _image_data_uri(image_path)
    hotspots: list[str] = []

    for tile in BOARD_TILES:
        submissions = grouped_submissions.get(tile.canonical_key, [])
        submission_count = len(submissions)
        rule = rules.get(tile.tile_id, {})
        rule_status = str(rule.get("status", "pending")).strip().lower() or "pending"

        if submission_count and tile.canonical_key in DUPLICATE_KEYS:
            state_class = "bp-state-shared"
            state_label = "Shared tile-name submissions; slot assignment and completion rules pending"
            data_status = "shared-submissions-rules-pending"
        elif submission_count:
            state_class = "bp-state-submitted"
            state_label = "Submissions logged; completion rule pending"
            data_status = "submitted-rules-pending"
        elif tile.always_available:
            state_class = "bp-state-opening"
            state_label = "Available from the start; completion rule pending"
            data_status = "opening-exception-rules-pending"
        else:
            state_class = "bp-state-pending"
            state_label = "Completion rule pending"
            data_status = "rules-pending"

        stage = STAGE_DETAILS[tile.stage]
        unlock_note = stage["unlock"]
        if tile.always_available:
            unlock_note = "Available from event start using the team's one starting CG chest."

        duplicate_note = ""
        if tile.canonical_key in DUPLICATE_KEYS:
            duplicate_note = (
                '<div class="bp-note">This label appears in more than one board slot. '
                "Slot assignment will be applied after the completion rules are configured.</div>"
            )

        rule_note = (
            '<div class="bp-rule"><strong>Rule status:</strong> '
            + _escape(rule_status.title())
            + "</div>"
        )
        aria_label = _escape(
            f"{tile.label}. {state_label}. {submission_count} matched submission(s)."
        )
        tooltip_id = f"bp-tooltip-{tile.tile_id}"
        count_badge = f'<span class="bp-count">{submission_count}</span>' if submission_count else ""
        opening_badge = '<span class="bp-opening">OPEN</span>' if tile.always_available else ""

        tip_position = " bp-tip-left" if tile.left < 15 else " bp-tip-right" if tile.left > 78 else ""
        if tile.top < 40:
            tip_position += " bp-tip-below"

        hotspots.append(
            f'<div class="bp-hotspot {state_class}" '
            f'data-tile-id="{tile.tile_id}" data-status="{data_status}" '
            f'data-submission-count="{submission_count}" tabindex="0" role="group" '
            f'aria-label="{aria_label}" aria-describedby="{tooltip_id}" '
            f'style="left:{tile.left}%;top:{tile.top}%;width:{tile.width}%;height:{tile.height}%">'
            + count_badge
            + opening_badge
            + f'<div class="bp-tooltip{tip_position}" id="{tooltip_id}" role="tooltip">'
            + f'<div class="bp-tooltip-title">{_escape(tile.label)}</div>'
            + f'<div class="bp-section">{_escape(stage["label"])}</div>'
            + f'<div class="bp-status">{_escape(state_label)}</div>'
            + f'<div class="bp-unlock"><strong>Board gate:</strong> {_escape(unlock_note)}</div>'
            + rule_note
            + duplicate_note
            + f'<div class="bp-drop-title">Matched submissions ({submission_count})</div>'
            + _submission_markup(submissions)
            + "</div></div>"
        )

    safe_team = _escape(team)
    return f"""
<style>
  .bp-shell {{ margin: 0.25rem 0 0.5rem; }}
  .bp-team-label {{ color: #64748b; font-size: 0.86rem; margin: 0 0 0.4rem; }}
  .bp-board-scroll {{ overflow-x: auto; overflow-y: hidden; padding-bottom: 0.35rem; }}
  .bp-board {{
    position: relative;
    width: 100%;
    max-width: 1208px;
    min-width: 880px;
    aspect-ratio: 1208 / 682;
    margin: 0 auto;
    isolation: isolate;
  }}
  .bp-board-image {{ position: absolute; inset: 0; width: 100%; height: 100%; display: block; }}
  .bp-hotspot {{
    position: absolute;
    z-index: 2;
    box-sizing: border-box;
    border-radius: clamp(7px, 0.9vw, 14px);
    cursor: help;
    outline: none;
    transition: border-color 120ms ease, background 120ms ease, box-shadow 120ms ease;
  }}
  .bp-state-pending {{ border: 2px dashed rgba(226,232,240,.78); background: rgba(15,23,42,.12); }}
  .bp-state-opening {{ border: 3px solid #fbbf24; background: rgba(251,191,36,.16); }}
  .bp-state-submitted {{ border: 3px solid #38bdf8; background: rgba(14,165,233,.17); }}
  .bp-state-shared {{ border: 3px solid #c084fc; background: rgba(168,85,247,.18); }}
  .bp-hotspot:hover, .bp-hotspot:focus, .bp-hotspot:focus-within {{
    z-index: 100;
    box-shadow: 0 0 0 3px rgba(255,255,255,.86), 0 10px 30px rgba(0,0,0,.45);
  }}
  .bp-count, .bp-opening {{
    position: absolute;
    top: -9px;
    min-width: 21px;
    height: 21px;
    padding: 0 5px;
    border-radius: 999px;
    color: #082f49;
    background: #bae6fd;
    border: 2px solid white;
    font: 800 11px/17px system-ui, sans-serif;
    text-align: center;
    box-sizing: border-box;
  }}
  .bp-count {{ right: -9px; }}
  .bp-opening {{ left: -10px; color: #422006; background: #fde68a; font-size: 9px; }}
  .bp-tooltip {{
    position: absolute;
    bottom: calc(100% + 8px);
    left: 50%;
    transform: translateX(-50%) translateY(4px);
    width: 280px;
    max-height: 200px;
    overflow-y: auto;
    padding: 12px 13px;
    border: 1px solid rgba(148,163,184,.45);
    border-radius: 11px;
    color: #e2e8f0;
    background: rgba(15,23,42,.98);
    box-shadow: 0 15px 40px rgba(0,0,0,.45);
    visibility: hidden;
    opacity: 0;
    pointer-events: none;
    transition: opacity 120ms ease, transform 120ms ease;
    font: 13px/1.35 system-ui, sans-serif;
    text-align: left;
    box-sizing: border-box;
  }}
  .bp-tip-left {{ left: 0; transform: translateX(0) translateY(4px); }}
  .bp-tip-right {{ left: auto; right: 0; transform: translateX(0) translateY(4px); }}
  .bp-tip-below {{ top: calc(100% + 8px); bottom: auto; }}
  .bp-hotspot:hover .bp-tooltip,
  .bp-hotspot:focus .bp-tooltip,
  .bp-hotspot:focus-within .bp-tooltip {{
    visibility: visible;
    opacity: 1;
    pointer-events: auto;
    transform: translateX(-50%) translateY(0);
  }}
  .bp-hotspot:hover .bp-tip-left, .bp-hotspot:focus .bp-tip-left,
  .bp-hotspot:focus-within .bp-tip-left, .bp-hotspot:hover .bp-tip-right,
  .bp-hotspot:focus .bp-tip-right, .bp-hotspot:focus-within .bp-tip-right {{ transform: translateX(0) translateY(0); }}
  .bp-tooltip-title {{ font-size: 15px; font-weight: 850; letter-spacing: .02em; color: white; }}
  .bp-section {{ color: #7dd3fc; font-weight: 700; margin: 1px 0 7px; }}
  .bp-status {{ color: #fde68a; margin-bottom: 7px; }}
  .bp-unlock, .bp-rule, .bp-note {{ color: #cbd5e1; margin: 5px 0; }}
  .bp-note {{ padding: 6px 7px; border-radius: 6px; background: rgba(30,41,59,.9); }}
  .bp-drop-title {{ margin-top: 9px; padding-top: 8px; border-top: 1px solid #334155; font-weight: 750; }}
  .bp-submissions {{ margin: 6px 0 0; padding-left: 18px; }}
  .bp-submissions li {{ margin: 0 0 7px; padding-left: 2px; }}
  .bp-player, .bp-item, .bp-date {{ display: block; }}
  .bp-player {{ color: white; font-weight: 750; }}
  .bp-item {{ color: #bae6fd; }}
  .bp-date {{ color: #94a3b8; font-size: 11px; }}
  .bp-more, .bp-empty {{ color: #94a3b8; font-style: italic; }}
  .bp-legend {{ display: flex; flex-wrap: wrap; gap: 8px 16px; margin-top: 7px; color: #64748b; font-size: 12px; }}
  .bp-legend span::before {{ content: ""; display: inline-block; width: 10px; height: 10px; margin-right: 5px; border-radius: 3px; vertical-align: -1px; }}
  .bp-legend-submitted::before {{ background: #38bdf8; }}
  .bp-legend-shared::before {{ background: #c084fc; }}
  .bp-legend-opening::before {{ background: #fbbf24; }}
  .bp-legend-pending::before {{ background: #94a3b8; }}
  @media (max-width: 900px) {{
    .bp-board-scroll {{ margin-right: -1rem; padding-right: 1rem; }}
    .bp-tooltip {{ width: 250px; max-height: 180px; }}
    .bp-team-label::after {{ content: " Scroll sideways to see the full board."; }}
  }}
</style>
<div class="bp-shell">
  <div class="bp-team-label">Board preview for <strong>{safe_team}</strong> — hover, tap, or keyboard-focus a tile.</div>
  <div class="bp-board-scroll">
    <div class="bp-board">
      <img class="bp-board-image" src="{image_uri}" alt="BapHeads Summer Bingo 2026 board">
      {''.join(hotspots)}
    </div>
  </div>
  <div class="bp-legend">
    <span class="bp-legend-submitted">Matched submissions (not completion)</span>
    <span class="bp-legend-shared">Shared TOA/TOB/COX submissions</span>
    <span class="bp-legend-opening">CG opening exception</span>
    <span class="bp-legend-pending">Rule pending</span>
  </div>
</div>
"""


def board_submission_summary(
    grouped_submissions: dict[str, list[dict[str, Any]]],
    unmatched: pd.DataFrame,
) -> dict[str, int]:
    return {
        "matched_submissions": sum(len(rows) for rows in grouped_submissions.values()),
        "matched_tile_types": len(grouped_submissions),
        "unmatched_submissions": len(unmatched),
        "cg_submissions": len(grouped_submissions.get("corrupted_gauntlet", [])),
    }
