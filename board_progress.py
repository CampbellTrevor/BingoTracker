from __future__ import annotations

import base64
import html
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from tile_completion_rules import (
    TILE_RULES,
    apply_submission,
    create_empty_state,
    evaluate_completion,
    format_route_progress,
)


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
    "toa": ("TOA", "Tombs of Amascut", "Tombs of Amascut 2"),
    "nex": ("Nex",),
    "hueycoatl": ("Huey", "Hueycoatl", "The Hueycoatl"),
    "gwd": ("GWD", "God Wars", "God Wars Dungeon"),
    "venator_shards": ("Venator Shard", "Venator Shards", "Muspah"),
    "vorkath": ("Vorkath",),
    "dks": ("DKS", "Dagannoth Kings"),
    "revs": ("Revs", "Revenant", "Revenants"),
    "tob": ("TOB", "Theatre of Blood", "Theater of Blood"),
    "colosseum": ("Colo", "Colosseum", "Fortis Colosseum"),
    "zenytes": ("Zenyte", "Zenytes", "Zenyte Shards"),
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
    "cox": ("COX", "Chambers of Xeric", "Chambers of Xeric 2"),
    "corrupted_gauntlet": ("CG", "Gauntlet", "Corrupted Gauntlet", "The Corrupted Gauntlet"),
    "araxxor": ("Araxxor",),
    "dt2": ("DT2", "DT2 Bosses", "Desert Treasure 2", "Desert Treasure 2 Bosses"),
    "doom": ("Doom", "Doom of Mokhaiotl"),
    "inferno": ("Inferno",),
    "yama": ("Yama",),
    "tormented_demons": (
        "Tormented Demon",
        "Tormented Demons",
        # Retained for compatibility with the earlier draft/example export.
        "Moons",
        "Moons of Peril",
        "Perilous Moons",
    ),
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
    _tile("final_tormented_demons", "TORMENTED DEMONS", "tormented_demons", "final_grid", 72.103, 59.091, 6.540),
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

# The Summer form's long TOA/COX names identify physical board slots. Short
# TOA/COX aliases remain generic so older exports can still fill both matching
# slots sequentially according to the board gates.
SLOT_SPECIFIC_ALIAS_TARGETS = {
    normalize_tile_name("Tombs of Amascut"): "start_toa",
    normalize_tile_name("Tombs of Amascut 2"): "final_toa",
    normalize_tile_name("Chambers of Xeric"): "path_cox",
    normalize_tile_name("Chambers of Xeric 2"): "final_cox",
}


def match_tile_key(value: Any) -> str | None:
    return ALIAS_LOOKUP.get(normalize_tile_name(value))


def load_tile_rules(rules_path: Path) -> dict[str, dict[str, Any]]:
    """Load the per-slot completion-rule metadata used by the rules table."""

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
    # Entry/source order is authoritative for lock transitions. Dates remain
    # display metadata because malformed dates must not move an early locked row
    # to the end and accidentally make it eligible.
    sort_columns = [
        column
        for column in ("Submission_Order", "Source_Row")
        if column in team_rows.columns
    ]
    if not sort_columns and "Date" in team_rows.columns:
        sort_columns = ["Date"]
    if sort_columns:
        team_rows = team_rows.sort_values(sort_columns, kind="stable", na_position="last")
    return team_rows


TILES_BY_ID = {tile.tile_id: tile for tile in BOARD_TILES}
TILE_IDS_BY_KEY: dict[str, tuple[str, ...]] = {
    canonical_key: tuple(
        tile.tile_id for tile in BOARD_TILES if tile.canonical_key == canonical_key
    )
    for canonical_key in CANONICAL_ALIASES
}
START_TILE_IDS = ("start_toa", "start_nex", "start_hueycoatl")
GRID_ONE_TILE_IDS = tuple(tile.tile_id for tile in BOARD_TILES if tile.stage == "grid_one")
ORDERED_PATH_TILE_IDS = ("path_voidwaker", "path_pnm_nightmare", "path_cox")
FINAL_GRID_TILE_IDS = tuple(tile.tile_id for tile in BOARD_TILES if tile.stage == "final_grid")
CG_TILE_ID = "final_corrupted_gauntlet"
BONUS_TILE_ID = "bonus_corp_beast"
RACE_TILE_IDS = tuple(tile.tile_id for tile in BOARD_TILES if tile.tile_id != BONUS_TILE_ID)
COMPLETION_RULE_ID_BY_TILE_ID = {
    "start_toa": "opening_hallway_toa",
    "start_nex": "opening_hallway_nex",
    "start_hueycoatl": "opening_hallway_hueycoatl",
    "path_voidwaker": "middle_hallway_voidwaker",
    "path_pnm_nightmare": "middle_hallway_nightmare",
    "path_cox": "middle_hallway_cox",
    **{
        tile.tile_id: tile.tile_id
        for tile in BOARD_TILES
        if tile.tile_id in TILE_RULES
    },
}
if set(COMPLETION_RULE_ID_BY_TILE_ID) != {tile.tile_id for tile in BOARD_TILES}:
    raise RuntimeError("Every board slot must have exactly one completion rule")


def _all_complete(completed_tile_ids: set[str], required_tile_ids: tuple[str, ...]) -> bool:
    return set(required_tile_ids).issubset(completed_tile_ids)


def _eligible_tile_ids(
    completed_tile_ids: set[str],
    *,
    cg_starting_chest_used: bool = False,
) -> set[str]:
    """Return the slots eligible immediately after the current completions."""

    eligible: set[str] = set()

    # Hallway 1 is strictly TOA -> NEX -> HUEYCOATL.
    if "start_toa" not in completed_tile_ids:
        eligible.add("start_toa")
    elif "start_nex" not in completed_tile_ids:
        eligible.add("start_nex")
    elif "start_hueycoatl" not in completed_tile_ids:
        eligible.add("start_hueycoatl")
    elif not _all_complete(completed_tile_ids, GRID_ONE_TILE_IDS):
        eligible.update(set(GRID_ONE_TILE_IDS) - completed_tile_ids)
    # Hallway 2 is strictly VOIDWAKER -> PNM/NIGHTMARE -> COX.
    elif "path_voidwaker" not in completed_tile_ids:
        eligible.add("path_voidwaker")
    elif "path_pnm_nightmare" not in completed_tile_ids:
        eligible.add("path_pnm_nightmare")
    elif "path_cox" not in completed_tile_ids:
        eligible.add("path_cox")
    elif not _all_complete(completed_tile_ids, FINAL_GRID_TILE_IDS):
        eligible.update(set(FINAL_GRID_TILE_IDS) - completed_tile_ids)
    elif BONUS_TILE_ID not in completed_tile_ids:
        eligible.add(BONUS_TILE_ID)

    # Exactly one prepared CG chest may contribute before the Final grid. If it
    # does not finish the tile, further CG rows wait for the normal grid gate.
    final_grid_unlocked = _all_complete(completed_tile_ids, ORDERED_PATH_TILE_IDS)
    if (
        CG_TILE_ID not in completed_tile_ids
        and not final_grid_unlocked
        and not cg_starting_chest_used
    ):
        eligible.add(CG_TILE_ID)
    return eligible


def _locked_reason(tile_id: str) -> str:
    if tile_id == "start_nex":
        return "NEX was locked because the opening TOA tile was not finished yet."
    if tile_id == "start_hueycoatl":
        return "HUEYCOATL was locked because NEX was not finished yet."
    if tile_id in GRID_ONE_TILE_IDS:
        return "The first grid was locked until TOA, NEX, and HUEYCOATL were finished in order."
    if tile_id == "path_voidwaker":
        return "VOIDWAKER was locked until every tile in the first grid was finished."
    if tile_id == "path_pnm_nightmare":
        return "PNM/NIGHTMARE was locked because VOIDWAKER was not finished yet."
    if tile_id == "path_cox":
        return "COX was locked because PNM/NIGHTMARE was not finished yet."
    if tile_id == CG_TILE_ID:
        return (
            "The one prepared starting CG chest was already used; further CG drops wait "
            "until the Final grid unlocks."
        )
    if tile_id in FINAL_GRID_TILE_IDS:
        return "The final grid was locked until VOIDWAKER, PNM/NIGHTMARE, and COX were finished in order."
    if tile_id == BONUS_TILE_ID:
        return "CORP BEAST was locked until every tile in the final grid was finished."
    return "This tile was locked when the submission was received."


def _gate_text(tile_id: str) -> str:
    if tile_id == "start_toa":
        return "Available at event start."
    if tile_id == "start_nex":
        return "Unlocks after the opening TOA tile is finished."
    if tile_id == "start_hueycoatl":
        return "Unlocks after NEX is finished."
    if tile_id in GRID_ONE_TILE_IDS:
        return "Unlocks after TOA, NEX, and HUEYCOATL are finished in order."
    if tile_id == "path_voidwaker":
        return "Unlocks after all 12 tiles in the first grid are finished."
    if tile_id == "path_pnm_nightmare":
        return "Unlocks after VOIDWAKER is finished."
    if tile_id == "path_cox":
        return "Unlocks after PNM/NIGHTMARE is finished."
    if tile_id == CG_TILE_ID:
        return (
            "One prepared team chest may contribute at event start; if that does not finish "
            "the tile, further CG submissions unlock with the Final grid."
        )
    if tile_id in FINAL_GRID_TILE_IDS:
        return "Unlocks after the second hallway ends with COX."
    if tile_id == BONUS_TILE_ID:
        return "Unlocks after all 12 tiles in the final grid are finished."
    return STAGE_DETAILS[TILES_BY_ID[tile_id].stage]["unlock"]


def _current_section(completed_tile_ids: set[str]) -> tuple[str, int, int]:
    if not _all_complete(completed_tile_ids, START_TILE_IDS):
        return "Opening hallway", len(completed_tile_ids.intersection(START_TILE_IDS)), len(START_TILE_IDS)
    if not _all_complete(completed_tile_ids, GRID_ONE_TILE_IDS):
        return "First grid", len(completed_tile_ids.intersection(GRID_ONE_TILE_IDS)), len(GRID_ONE_TILE_IDS)
    if not _all_complete(completed_tile_ids, ORDERED_PATH_TILE_IDS):
        return "Second hallway", len(completed_tile_ids.intersection(ORDERED_PATH_TILE_IDS)), len(ORDERED_PATH_TILE_IDS)
    if not _all_complete(completed_tile_ids, FINAL_GRID_TILE_IDS):
        return "Final grid", len(completed_tile_ids.intersection(FINAL_GRID_TILE_IDS)), len(FINAL_GRID_TILE_IDS)
    return (
        "Race complete",
        len(completed_tile_ids.intersection(RACE_TILE_IDS)),
        len(RACE_TILE_IDS),
    )


def _next_objective(
    completed_tile_ids: set[str],
    eligible_tile_ids: set[str],
    section: str,
) -> str:
    main_eligible = [
        tile.tile_id
        for tile in BOARD_TILES
        if tile.tile_id in eligible_tile_ids and tile.tile_id != CG_TILE_ID
    ]
    if section == "Race complete":
        objective = (
            "Everything is finished"
            if BONUS_TILE_ID in completed_tile_ids
            else "The 30-tile race is complete; the Corp Beast bonus is available"
        )
    elif section in {"First grid", "Final grid"} and main_eligible:
        objective = f"Any of {len(main_eligible)} unlocked {section.lower()} tiles"
    elif main_eligible:
        objective = " → ".join(TILES_BY_ID[tile_id].label for tile_id in main_eligible)
    else:
        objective = section

    if CG_TILE_ID in eligible_tile_ids and CG_TILE_ID not in completed_tile_ids:
        objective += "; CG is also open from the starting chest"
    return objective


def _blocked_route_ids(
    tile_id: str,
    credited_item_keys: dict[str, list[str]],
) -> set[str]:
    """Apply the planner's cross-stage mega-rare reuse restrictions."""

    if tile_id == "final_tob" and "scythe_of_vitur" in credited_item_keys["grid_tob"]:
        return {"scythe"}
    if tile_id == "final_cox" and "twisted_bow" in credited_item_keys["path_cox"]:
        return {"twisted_bow"}
    if tile_id == "final_toa" and "tumekens_shadow" in credited_item_keys["start_toa"]:
        return {"shadow"}
    return set()


def calculate_team_progress(df: pd.DataFrame, team: str) -> dict[str, Any]:
    """Replay one team's log through the board gates and completion formulas."""

    completed_events: dict[str, dict[str, Any]] = {}
    completion_sequence: dict[str, int] = {}
    rule_states = {
        tile.tile_id: create_empty_state(COMPLETION_RULE_ID_BY_TILE_ID[tile.tile_id])
        for tile in BOARD_TILES
    }
    credited_submissions: dict[str, list[dict[str, Any]]] = defaultdict(list)
    credited_item_keys: dict[str, list[str]] = defaultdict(list)
    nonqualifying_submissions: dict[str, list[dict[str, Any]]] = defaultdict(list)
    extra_submissions: dict[str, list[dict[str, Any]]] = defaultdict(list)
    locked_attempts: dict[str, list[dict[str, Any]]] = defaultdict(list)
    ignored_locked: list[dict[str, Any]] = []
    nonqualifying: list[dict[str, Any]] = []
    unmatched: list[dict[str, Any]] = []
    accepted: list[dict[str, Any]] = []
    cg_starting_chest_used = False

    for sequence, (_, row) in enumerate(_sorted_team_rows(df, team).iterrows(), start=1):
        submission = row.to_dict()
        canonical_key = match_tile_key(row.get("Category"))
        if canonical_key is None:
            diagnostic = {
                **submission,
                "Board Slot": "Unmatched",
                "Disposition": "Not counted",
                "Reason": "CSV tile name does not map unambiguously to a board tile.",
            }
            unmatched.append(diagnostic)
            continue

        specific_tile_id = SLOT_SPECIFIC_ALIAS_TARGETS.get(
            normalize_tile_name(row.get("Category"))
        )
        candidate_tile_ids = (
            (specific_tile_id,)
            if specific_tile_id is not None
            else TILE_IDS_BY_KEY[canonical_key]
        )
        completed_tile_ids = set(completed_events)
        eligible_tile_ids = _eligible_tile_ids(
            completed_tile_ids,
            cg_starting_chest_used=cg_starting_chest_used,
        )
        acceptable_tile_ids = [
            tile_id
            for tile_id in candidate_tile_ids
            if tile_id not in completed_events and tile_id in eligible_tile_ids
        ]

        if len(acceptable_tile_ids) > 1:
            raise RuntimeError(
                f"Board configuration error: multiple eligible slots for {canonical_key}: "
                + ", ".join(acceptable_tile_ids)
            )

        if acceptable_tile_ids:
            tile_id = acceptable_tile_ids[0]
            final_grid_unlocked = _all_complete(completed_tile_ids, ORDERED_PATH_TILE_IDS)
            if tile_id == CG_TILE_ID and not final_grid_unlocked:
                cg_starting_chest_used = True

            blocked_routes = _blocked_route_ids(tile_id, credited_item_keys)
            outcome = apply_submission(
                rule_states[tile_id],
                row.get("Item"),
                row.get("Player"),
                blocked_routes,
            )
            if not outcome.accepted:
                reason_by_code = {
                    "known nonqualifying item": "This item is explicitly excluded by the tile rule.",
                    "unknown item alias": "This item name is not recognized by the tile rule.",
                    "item does not advance this tile": "This item does not advance this physical tile's rule.",
                    "item only advances blocked route": "This item belongs only to a route that was permanently blocked by an earlier tile.",
                    "player name is required": "This completion route requires a named player.",
                    "player already counted": "This player has already received credit for this distinct-player route.",
                }
                diagnostic = {
                    **submission,
                    "Board Slot": tile_id,
                    "Disposition": "No rule progress",
                    "Reason": reason_by_code.get(outcome.reason, outcome.reason),
                }
                nonqualifying_submissions[tile_id].append(diagnostic)
                nonqualifying.append(diagnostic)
                continue

            credited_submissions[tile_id].append(submission)
            if outcome.item_key is not None:
                credited_item_keys[tile_id].append(outcome.item_key)
            evaluation = evaluate_completion(rule_states[tile_id], blocked_routes)
            completed_route = next(
                (route.label for route in evaluation.routes if route.complete),
                None,
            )
            did_complete = evaluation.complete
            diagnostic = {
                **submission,
                "Board Slot": tile_id,
                "Disposition": "Completed tile" if did_complete else "Advanced tile progress",
                "Reason": (
                    f"Completion route met: {completed_route}."
                    if did_complete
                    else "Qualifying submission recorded toward the tile requirements."
                ),
            }
            accepted.append(diagnostic)
            if did_complete:
                completed_events[tile_id] = submission
                completion_sequence[tile_id] = sequence
            continue

        incomplete_tile_ids = [
            tile_id for tile_id in candidate_tile_ids if tile_id not in completed_events
        ]
        if incomplete_tile_ids:
            # Rejected attempts are attached to the next unfinished physical
            # slot and are never banked for a later unlock.
            tile_id = incomplete_tile_ids[0]
            reason = _locked_reason(tile_id)
            diagnostic = {
                **submission,
                "Board Slot": tile_id,
                "Disposition": "Ignored while locked",
                "Reason": reason,
            }
            locked_attempts[tile_id].append(submission)
            ignored_locked.append(diagnostic)
            continue

        # Once every physical slot with this label is complete, later rows are
        # retained as extra hover history but do not change progress.
        tile_id = max(candidate_tile_ids, key=lambda candidate: completion_sequence[candidate])
        extra_submissions[tile_id].append(submission)

    completed_tile_ids = set(completed_events)
    eligible_tile_ids = _eligible_tile_ids(
        completed_tile_ids,
        cg_starting_chest_used=cg_starting_chest_used,
    )
    section, section_completed, section_total = _current_section(completed_tile_ids)
    section_rank = {
        "Opening hallway": 0,
        "First grid": 1,
        "Second hallway": 2,
        "Final grid": 3,
        "Race complete": 4,
    }[section]
    states: dict[str, dict[str, Any]] = {}
    for tile in BOARD_TILES:
        if tile.tile_id in completed_events:
            status = "complete"
        elif tile.tile_id in eligible_tile_ids:
            status = "available"
        else:
            status = "locked"
        blocked_routes = _blocked_route_ids(tile.tile_id, credited_item_keys)
        rule = TILE_RULES[COMPLETION_RULE_ID_BY_TILE_ID[tile.tile_id]]
        rule_summary = (
            rule.routes[0].label
            if len(rule.routes) == 1
            else "Complete any one route: " + " / ".join(route.label for route in rule.routes)
        )
        states[tile.tile_id] = {
            "status": status,
            "completion": completed_events.get(tile.tile_id),
            "submissions": credited_submissions.get(tile.tile_id, []),
            "credited_item_keys": credited_item_keys.get(tile.tile_id, []),
            "nonqualifying": nonqualifying_submissions.get(tile.tile_id, []),
            "extras": extra_submissions.get(tile.tile_id, []),
            "locked_attempts": locked_attempts.get(tile.tile_id, []),
            "rule_summary": rule_summary,
            "rule_progress": format_route_progress(rule_states[tile.tile_id], blocked_routes),
            "rule_notes": rule.notes,
        }

    extra_count = sum(len(rows) for rows in extra_submissions.values())
    race_completed_count = len(completed_tile_ids.intersection(RACE_TILE_IDS))
    return {
        "team": team,
        "states": states,
        "accepted": accepted,
        "ignored_locked": ignored_locked,
        "nonqualifying": nonqualifying,
        "unmatched": unmatched,
        "completed_count": len(completed_events),
        "race_completed_count": race_completed_count,
        "race_total": len(RACE_TILE_IDS),
        "bonus_complete": BONUS_TILE_ID in completed_events,
        "available_count": len(eligible_tile_ids),
        "locked_count": len(BOARD_TILES) - len(completed_events) - len(eligible_tile_ids),
        "ignored_locked_count": len(ignored_locked),
        "nonqualifying_count": len(nonqualifying),
        "unmatched_count": len(unmatched),
        "extra_submission_count": extra_count,
        "current_section": section,
        "section_rank": section_rank,
        "section_completed": section_completed,
        "section_total": section_total,
        "next_objective": _next_objective(completed_tile_ids, eligible_tile_ids, section),
        "cg_complete": CG_TILE_ID in completed_events,
        "cg_starting_chest_used": cg_starting_chest_used,
    }


def board_readiness_rows(rules: dict[str, dict[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for tile in BOARD_TILES:
        rule = rules.get(tile.tile_id, {})
        status = str(rule.get("status", "Pending")).strip() or "Pending"
        aliases = ", ".join(CANONICAL_ALIASES[tile.canonical_key])
        unlock = _gate_text(tile.tile_id)
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
    progress: dict[str, Any],
    team: str,
) -> str:
    """Render rule-based complete/available/locked states and hover details."""

    image_uri = _image_data_uri(image_path)
    hotspots: list[str] = []

    for tile in BOARD_TILES:
        tile_state = progress["states"][tile.tile_id]
        status = tile_state["status"]
        submissions = tile_state["submissions"]
        nonqualifying = tile_state["nonqualifying"]
        extras = tile_state["extras"]
        ignored = tile_state["locked_attempts"]
        submission_count = len(submissions)
        nonqualifying_count = len(nonqualifying)
        extra_count = len(extras)
        ignored_count = len(ignored)
        rule_summary = tile_state["rule_summary"]
        rule_progress = tile_state["rule_progress"]
        rule_notes = tile_state["rule_notes"]

        if status == "complete":
            state_label = "Complete"
            badge_label = "DONE"
        elif status == "available":
            state_label = "In progress" if submission_count else "Available now"
            badge_label = "NOW"
        else:
            state_label = "Locked"
            badge_label = "LOCK"
        state_class = f"bp-state-{status}"
        ignored_class = " bp-has-ignored" if ignored_count else ""

        stage = STAGE_DETAILS[tile.stage]
        aria_label = _escape(
            f"{tile.label}. {state_label}. {submission_count} qualifying submissions; "
            f"{nonqualifying_count} nonqualifying; {extra_count} additional after completion; "
            f"{ignored_count} ignored while locked."
        )
        tooltip_id = f"bp-tooltip-{tile.tile_id}"
        status_badge = (
            f'<span class="bp-state-badge bp-badge-{status}" aria-hidden="true">'
            f"{badge_label}</span>"
        )
        if tile.always_available and status == "available":
            status_badge = (
                '<span class="bp-state-badge bp-badge-available" '
                'aria-hidden="true">OPEN</span>'
            )
        ignored_badge = (
            f'<span class="bp-ignored-badge" aria-hidden="true">!{ignored_count}</span>'
            if ignored_count
            else ""
        )

        tip_position = " bp-tip-left" if tile.left < 15 else " bp-tip-right" if tile.left > 78 else ""
        if tile.top < 40:
            tip_position += " bp-tip-below"

        details: list[str] = []
        details.extend(
            [
                '<div class="bp-rule"><strong>Completion rule:</strong> '
                + _escape(rule_summary)
                + "</div>",
                '<div class="bp-drop-title">Route progress</div>',
                '<ul class="bp-routes">'
                + "".join(f"<li>{_escape(line)}</li>" for line in rule_progress)
                + "</ul>",
            ]
        )
        if rule_notes:
            details.append(
                '<div class="bp-note">'
                + " ".join(_escape(note) for note in rule_notes)
                + "</div>"
            )

        if submissions:
            details.extend(
                [
                    f'<div class="bp-drop-title">Qualifying submissions ({submission_count})</div>',
                    _submission_markup(submissions),
                ]
            )
        elif status == "available":
            details.append('<div class="bp-empty">No qualifying submission yet.</div>')
        else:
            details.append('<div class="bp-empty">This tile cannot accept a submission yet.</div>')

        if nonqualifying:
            details.extend(
                [
                    f'<div class="bp-drop-title bp-nonqualifying-title">Nonqualifying submissions ({nonqualifying_count})</div>',
                    '<div class="bp-note bp-nonqualifying-note">These rows were submitted while the tile was open, but did not advance its completion rule.</div>',
                    _submission_markup(nonqualifying),
                ]
            )

        if extras:
            details.extend(
                [
                    '<div class="bp-drop-title">Additional submissions (no extra progress)</div>',
                    _submission_markup(extras),
                ]
            )
        if ignored:
            details.extend(
                [
                    f'<div class="bp-drop-title bp-ignored-title">Ignored while locked ({ignored_count})</div>',
                    '<div class="bp-note bp-ignored-note">These rows did not count and were not banked. '
                    + _escape(_locked_reason(tile.tile_id))
                    + "</div>",
                    _submission_markup(ignored),
                ]
            )

        hotspots.append(
            f'<div class="bp-hotspot {state_class}{ignored_class}" '
            f'data-tile-id="{tile.tile_id}" data-status="{status}" '
            f'data-submission-count="{submission_count}" '
            f'data-nonqualifying-count="{nonqualifying_count}" '
            f'data-extra-count="{extra_count}" '
            f'data-ignored-count="{ignored_count}" '
            f'tabindex="0" role="group" '
            f'aria-label="{aria_label}" aria-describedby="{tooltip_id}" '
            f'style="left:{tile.left}%;top:{tile.top}%;width:{tile.width}%;height:{tile.height}%">'
            + status_badge
            + ignored_badge
            + f'<div class="bp-tooltip{tip_position}" id="{tooltip_id}" role="region" '
            + f'tabindex="0" aria-label="{_escape(tile.label)} details">'
            + f'<div class="bp-tooltip-title">{_escape(tile.label)}</div>'
            + f'<div class="bp-section">{_escape(stage["label"])}</div>'
            + f'<div class="bp-status">{_escape(state_label)}</div>'
            + f'<div class="bp-unlock"><strong>Board gate:</strong> {_escape(_gate_text(tile.tile_id))}</div>'
            + "".join(details)
            + "</div></div>"
        )

    safe_team = _escape(team)
    return f"""
<style>
  .bp-shell {{ margin: 0.25rem 0 0.5rem; }}
  .bp-team-label {{ color: #94a3b8; font-size: 0.86rem; margin: 0 0 0.4rem; }}
  .bp-board-scroll {{ overflow-x: auto; overflow-y: hidden; padding-bottom: 0.35rem; }}
  .bp-board {{
    position: relative;
    width: 100%;
    max-width: 1208px;
    min-width: 880px;
    aspect-ratio: 16 / 9;
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
  .bp-state-complete {{ border: 3px solid #22c55e; background-color: rgba(34,197,94,.24); }}
  .bp-state-available {{ border: 3px solid #f59e0b; background-color: rgba(245,158,11,.16); }}
  .bp-state-locked {{ border: 2px dashed #94a3b8; background-color: rgba(15,23,42,.52); }}
  .bp-has-ignored {{
    background-image: repeating-linear-gradient(135deg, rgba(251,113,133,.20) 0 5px, transparent 5px 10px);
  }}
  .bp-hotspot:hover, .bp-hotspot:focus, .bp-hotspot:focus-within {{
    z-index: 100;
    box-shadow: 0 0 0 3px rgba(255,255,255,.86), 0 10px 30px rgba(0,0,0,.45);
  }}
  .bp-state-badge, .bp-ignored-badge {{
    position: absolute;
    min-width: 21px;
    height: 21px;
    padding: 0 5px;
    border-radius: 999px;
    border: 2px solid white;
    font: 800 9px/17px system-ui, sans-serif;
    text-align: center;
    box-sizing: border-box;
  }}
  .bp-state-badge {{ top: 3px; right: 3px; }}
  .bp-badge-complete {{ color: #052e16; background: #86efac; }}
  .bp-badge-available {{ color: #451a03; background: #fcd34d; }}
  .bp-badge-locked {{ color: #0f172a; background: #cbd5e1; }}
  .bp-ignored-badge {{ right: 3px; bottom: 3px; color: #4c0519; background: #fda4af; }}
  .bp-tooltip {{
    position: absolute;
    bottom: calc(100% + 8px);
    left: 50%;
    transform: translateX(-50%) translateY(4px);
    width: 280px;
    max-height: 280px;
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
  .bp-routes {{ margin: 5px 0 0; padding-left: 18px; color: #bae6fd; }}
  .bp-routes li {{ margin-bottom: 4px; }}
  .bp-nonqualifying-title {{ color: #fbbf24; }}
  .bp-nonqualifying-note {{ border-left: 3px solid #f59e0b; }}
  .bp-ignored-title {{ color: #fda4af; }}
  .bp-ignored-note {{ border-left: 3px solid #fb7185; }}
  .bp-drop-title {{ margin-top: 9px; padding-top: 8px; border-top: 1px solid #334155; font-weight: 750; }}
  .bp-submissions {{ margin: 6px 0 0; padding-left: 18px; }}
  .bp-submissions li {{ margin: 0 0 7px; padding-left: 2px; }}
  .bp-player, .bp-item, .bp-date {{ display: block; }}
  .bp-player {{ color: white; font-weight: 750; }}
  .bp-item {{ color: #bae6fd; }}
  .bp-date {{ color: #94a3b8; font-size: 11px; }}
  .bp-more, .bp-empty {{ color: #94a3b8; font-style: italic; }}
  .bp-legend {{ display: flex; flex-wrap: wrap; gap: 8px 16px; margin-top: 7px; color: #94a3b8; font-size: 12px; }}
  .bp-legend span::before {{ content: ""; display: inline-block; width: 10px; height: 10px; margin-right: 5px; border-radius: 3px; vertical-align: -1px; }}
  .bp-legend-complete::before {{ background: #22c55e; }}
  .bp-legend-available::before {{ background: #f59e0b; }}
  .bp-legend-locked::before {{ background: #94a3b8; }}
  .bp-legend-ignored::before {{ background: #fb7185; }}
  @media (max-width: 900px) {{
    .bp-board-scroll {{ margin-right: -1rem; padding-right: 1rem; }}
    .bp-tooltip {{ width: 250px; max-height: 240px; }}
    .bp-team-label::after {{ content: " Scroll sideways to see the full board."; }}
  }}
</style>
<div class="bp-shell">
  <div class="bp-team-label">Board progress for <strong>{safe_team}</strong> — hover, tap, or keyboard-focus a tile.</div>
  <div class="bp-board-scroll">
    <div class="bp-board">
      <img class="bp-board-image" src="{image_uri}" alt="BapHeads Summer Bingo 2026 board">
      {''.join(hotspots)}
    </div>
  </div>
  <div class="bp-legend">
    <span class="bp-legend-complete">Complete</span>
    <span class="bp-legend-available">Available now</span>
    <span class="bp-legend-locked">Locked</span>
    <span class="bp-legend-ignored">Ignored early submission</span>
  </div>
</div>
"""
