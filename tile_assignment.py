from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable


@dataclass(frozen=True)
class RosterMember:
    draft_name: str
    wom_name: str


@dataclass(frozen=True)
class ComponentSpec:
    key: str
    label: str
    kind: str
    weight: float
    metrics: tuple[str, ...] = ()
    multipliers: tuple[float, ...] = ()
    benchmark: float = 0.0
    targets: tuple[tuple[str, float], ...] = ()
    gate_target: float = 0.0


@dataclass(frozen=True)
class TileSpec:
    tile_id: str
    section: str
    label: str
    family: str
    components: tuple[ComponentSpec, ...]
    burden: float = 1.0
    confidence_cap: str | None = None
    note: str = ""
    raid: bool = False


@dataclass(frozen=True)
class PlayerStats:
    draft_name: str
    wom_name: str
    matched_name: str | None
    player_id: int | None
    updated_at: str | None
    available: bool
    ehb: float | None
    combat_level: int
    skills: dict[str, float]
    bosses: dict[str, float]
    activities: dict[str, float]


@dataclass(frozen=True)
class ReadinessRecord:
    tile_id: str
    section: str
    tile: str
    family: str
    player: str
    score: float | None
    band: str
    confidence: str
    evidence: str
    why: str
    component_details: str
    burden: float
    raid: bool


FIDDLSTCKS_ROSTER = (
    RosterMember("FIDDLSTCKS", "Fiddlestcks"),
    RosterMember("50 TRAITS", "50 Traits"),
    RosterMember("ERTHWERM GIM", "Erthwerm GIM"),
    RosterMember("PHZYQO", "phzyqo"),
    RosterMember("MELDYNOIR", "Meldynoir"),
    RosterMember("LORNA SNOIRE", "Lorna Snore"),
    RosterMember("UPDONG", "Updong"),
    RosterMember("NOTARNR", "NotArnr"),
    RosterMember("GIM WEWI", "GIM WEWI"),
    RosterMember("GIMYNEWTRON", "GIMyNewtron"),
    RosterMember("ZEXY DAN", "Zexy Dan"),
    RosterMember("GIM RETRO J", "gim retro j"),
    RosterMember("GIM RETRO S", "gim retro s"),
    RosterMember("BUSSY ROOTER", "Bussy Rooter"),
    RosterMember("NOOPERSWOZ", "NoopersWoz"),
    RosterMember("SMOT SMOT", "smot smot"),
    RosterMember("TAREM", "Tarem"),
    RosterMember("AKSELERATORX", "AkseleratorX"),
    RosterMember("DONG TWIRL", "dong twirl"),
    RosterMember("RYUTHEGRUMP", "RyuTheGrump"),
)


MID_HYBRID = (
    ("attack", 85),
    ("strength", 85),
    ("defence", 85),
    ("hitpoints", 90),
    ("ranged", 85),
    ("magic", 85),
    ("prayer", 70),
)
ENDGAME_HYBRID = (
    ("attack", 95),
    ("strength", 95),
    ("defence", 90),
    ("hitpoints", 95),
    ("ranged", 95),
    ("magic", 95),
    ("prayer", 77),
)
RANGED_MAGIC = (
    ("ranged", 95),
    ("magic", 95),
    ("hitpoints", 95),
    ("prayer", 77),
)
MELEE_SUPPORT = (
    ("attack", 90),
    ("strength", 90),
    ("defence", 85),
    ("hitpoints", 90),
    ("prayer", 70),
)


def _boss(
    key: str,
    label: str,
    weight: float,
    metrics: Iterable[str],
    benchmark: float,
    multipliers: Iterable[float] = (),
) -> ComponentSpec:
    return ComponentSpec(
        key=key,
        label=label,
        kind="boss",
        weight=weight,
        metrics=tuple(metrics),
        multipliers=tuple(multipliers),
        benchmark=benchmark,
    )


def _achievement(key: str, label: str, weight: float, metric: str) -> ComponentSpec:
    return ComponentSpec(
        key=key,
        label=label,
        kind="achievement",
        weight=weight,
        metrics=(metric,),
    )


def _activity(
    key: str,
    label: str,
    weight: float,
    metric: str,
    benchmark: float,
) -> ComponentSpec:
    return ComponentSpec(
        key=key,
        label=label,
        kind="activity",
        weight=weight,
        metrics=(metric,),
        benchmark=benchmark,
    )


def _skills(key: str, label: str, weight: float, targets: Iterable[tuple[str, float]]) -> ComponentSpec:
    return ComponentSpec(
        key=key,
        label=label,
        kind="skills",
        weight=weight,
        targets=tuple(targets),
    )


def _gate(key: str, label: str, weight: float, skill: str, target: float) -> ComponentSpec:
    return ComponentSpec(
        key=key,
        label=label,
        kind="gate",
        weight=weight,
        metrics=(skill,),
        gate_target=target,
    )


def _ehb(weight: float) -> ComponentSpec:
    return ComponentSpec(
        key="ehb",
        label="EHB",
        kind="ehb",
        weight=weight,
        metrics=("ehb",),
        benchmark=250,
    )


GRID_TILE_SPECS = (
    TileSpec(
        "grid_gwd",
        "First grid",
        "GWD",
        "gwd",
        (
            _boss(
                "direct",
                "GWD KC",
                0.70,
                ("general_graardor", "commander_zilyana", "kreearra", "kril_tsutsaroth"),
                300,
            ),
            _skills("skills", "Combat skills", 0.20, MID_HYBRID),
            _ehb(0.10),
        ),
    ),
    TileSpec(
        "grid_venator_shards",
        "First grid",
        "VENATOR SHARDS",
        "venator_shards",
        (
            _boss("direct", "Phantom Muspah KC", 0.75, ("phantom_muspah",), 150),
            _skills("skills", "Ranged/magic skills", 0.20, RANGED_MAGIC),
            _ehb(0.05),
        ),
    ),
    TileSpec(
        "grid_vorkath",
        "First grid",
        "VORKATH",
        "vorkath",
        (
            _boss("direct", "Vorkath KC", 0.80, ("vorkath",), 200),
            _skills("skills", "Combat skills", 0.15, MID_HYBRID),
            _ehb(0.05),
        ),
    ),
    TileSpec(
        "grid_dks",
        "First grid",
        "DKS",
        "dks",
        (
            _boss(
                "direct",
                "Dagannoth Kings KC",
                0.75,
                ("dagannoth_prime", "dagannoth_rex", "dagannoth_supreme"),
                300,
            ),
            _skills("skills", "Hybrid combat skills", 0.20, MID_HYBRID),
            _ehb(0.05),
        ),
    ),
    TileSpec(
        "grid_revs",
        "First grid",
        "REVS",
        "revs",
        (
            _boss(
                "adjacent",
                "Wilderness boss KC proxy",
                0.45,
                (
                    "callisto",
                    "artio",
                    "vetion",
                    "calvarion",
                    "venenatis",
                    "spindel",
                    "chaos_elemental",
                    "chaos_fanatic",
                ),
                200,
            ),
            _skills("skills", "Wilderness combat skills", 0.40, RANGED_MAGIC),
            _ehb(0.15),
        ),
        burden=0.75,
        confidence_cap="Low",
        note="WoM does not track Revenant KC; this is a Wilderness-PvM proxy.",
    ),
    TileSpec(
        "grid_tob",
        "First grid",
        "TOB",
        "tob",
        (
            _boss(
                "direct",
                "TOB effective KC",
                0.75,
                ("theatre_of_blood", "theatre_of_blood_hard_mode"),
                75,
                (1, 2),
            ),
            _skills("skills", "Endgame combat skills", 0.15, ENDGAME_HYBRID),
            _ehb(0.10),
        ),
        burden=1.5,
        raid=True,
    ),
    TileSpec(
        "grid_colosseum",
        "First grid",
        "COLOSSEUM",
        "colosseum",
        (
            _achievement("direct", "Sol Heredit completions", 0.60, "sol_heredit"),
            _activity("adjacent", "Colosseum glory", 0.20, "colosseum_glory", 30000),
            _skills("skills", "Endgame combat skills", 0.15, ENDGAME_HYBRID),
            _ehb(0.05),
        ),
        burden=1.5,
    ),
    TileSpec(
        "grid_zenytes",
        "First grid",
        "ZENYTES",
        "zenytes",
        (
            _skills(
                "skills",
                "Demonics combat proxy",
                0.80,
                ENDGAME_HYBRID,
            ),
            _ehb(0.20),
        ),
        confidence_cap="Low",
        note="WoM does not track Demonic Gorilla KC or Monkey Madness II completion.",
    ),
    TileSpec(
        "grid_royal_titans",
        "First grid",
        "ROYAL TITANS",
        "royal_titans",
        (
            _boss("direct", "Royal Titans KC", 0.75, ("the_royal_titans",), 100),
            _skills("skills", "Combat skills", 0.20, MID_HYBRID),
            _ehb(0.05),
        ),
        burden=0.75,
    ),
    TileSpec(
        "grid_barrows",
        "First grid",
        "BARROWS",
        "barrows",
        (
            _boss("direct", "Barrows chests", 0.85, ("barrows_chests",), 250),
            _skills("skills", "Combat skills", 0.10, MID_HYBRID),
            _ehb(0.05),
        ),
        burden=0.75,
    ),
    TileSpec(
        "grid_cerberus",
        "First grid",
        "CERBERUS",
        "cerberus",
        (
            _boss("direct", "Cerberus KC", 0.70, ("cerberus",), 200),
            _gate("gate", "Slayer access", 0.20, "slayer", 91),
            _skills("skills", "Combat skills", 0.05, MELEE_SUPPORT),
            _ehb(0.05),
        ),
    ),
    TileSpec(
        "grid_the_mad_angel",
        "First grid",
        "THE MAD ANGEL",
        "the_mad_angel",
        (
            _boss("direct", "Mad Angel KC", 0.75, ("mad_angel",), 50),
            _skills("skills", "Combat skills", 0.20, ENDGAME_HYBRID),
            _ehb(0.05),
        ),
    ),
    TileSpec(
        "final_corrupted_gauntlet",
        "Final grid",
        "CORRUPTED GAUNTLET",
        "corrupted_gauntlet",
        (
            _boss("direct", "Corrupted Gauntlet KC", 0.75, ("the_corrupted_gauntlet",), 100),
            _boss("adjacent", "Regular Gauntlet KC", 0.10, ("the_gauntlet",), 100),
            _skills("skills", "Endgame combat skills", 0.10, ENDGAME_HYBRID),
            _ehb(0.05),
        ),
        burden=1.5,
        note="OPEN FROM START: the team begins with one CG chest.",
    ),
    TileSpec(
        "final_araxxor",
        "Final grid",
        "ARAXXOR",
        "araxxor",
        (
            _boss("direct", "Araxxor KC", 0.70, ("araxxor",), 200),
            _gate("gate", "Slayer access", 0.20, "slayer", 92),
            _skills("skills", "Combat skills", 0.05, MELEE_SUPPORT),
            _ehb(0.05),
        ),
    ),
    TileSpec(
        "final_dt2",
        "Final grid",
        "DT2",
        "dt2",
        (
            _boss(
                "direct",
                "DT2 boss KC",
                0.75,
                ("vardorvis", "duke_sucellus", "the_leviathan", "the_whisperer"),
                300,
            ),
            _skills("skills", "Endgame combat skills", 0.15, ENDGAME_HYBRID),
            _ehb(0.10),
        ),
        burden=1.5,
    ),
    TileSpec(
        "final_tob",
        "Final grid",
        "TOB",
        "tob",
        (
            _boss(
                "direct",
                "TOB effective KC",
                0.75,
                ("theatre_of_blood", "theatre_of_blood_hard_mode"),
                75,
                (1, 2),
            ),
            _skills("skills", "Endgame combat skills", 0.15, ENDGAME_HYBRID),
            _ehb(0.10),
        ),
        burden=1.5,
        raid=True,
    ),
    TileSpec(
        "final_doom",
        "Final grid",
        "DOOM",
        "doom",
        (
            _boss("direct", "Doom KC", 0.75, ("doom_of_mokhaiotl",), 50),
            _skills("skills", "Endgame combat skills", 0.15, ENDGAME_HYBRID),
            _ehb(0.10),
        ),
        burden=1.5,
    ),
    TileSpec(
        "final_inferno",
        "Final grid",
        "INFERNO",
        "inferno",
        (
            _achievement("direct", "Inferno completions", 0.70, "tzkal_zuk"),
            _boss("adjacent", "Fight Caves KC", 0.10, ("tztok_jad",), 25),
            _skills("skills", "Endgame combat skills", 0.15, ENDGAME_HYBRID),
            _ehb(0.05),
        ),
        burden=1.5,
    ),
    TileSpec(
        "final_yama",
        "Final grid",
        "YAMA",
        "yama",
        (
            _boss("direct", "Yama KC", 0.75, ("yama",), 50),
            _skills("skills", "Endgame combat skills", 0.15, ENDGAME_HYBRID),
            _ehb(0.10),
        ),
        burden=1.5,
    ),
    TileSpec(
        "final_cox",
        "Final grid",
        "COX",
        "cox",
        (
            _boss(
                "direct",
                "COX effective KC",
                0.75,
                ("chambers_of_xeric", "chambers_of_xeric_challenge_mode"),
                150,
                (1, 2),
            ),
            _skills("skills", "Endgame combat skills", 0.15, ENDGAME_HYBRID),
            _ehb(0.10),
        ),
        burden=1.5,
        raid=True,
    ),
    TileSpec(
        "final_moons_of_peril",
        "Final grid",
        "MOONS OF PERIL",
        "moons_of_peril",
        (
            _boss("direct", "Lunar chests", 0.85, ("lunar_chests",), 150),
            _skills("skills", "Melee/defence skills", 0.10, MELEE_SUPPORT),
            _ehb(0.05),
        ),
        burden=0.75,
    ),
    TileSpec(
        "final_zulrah",
        "Final grid",
        "ZULRAH",
        "zulrah",
        (
            _boss("direct", "Zulrah KC", 0.80, ("zulrah",), 300),
            _skills("skills", "Ranged/magic skills", 0.15, RANGED_MAGIC),
            _ehb(0.05),
        ),
    ),
    TileSpec(
        "final_maggot_king",
        "Final grid",
        "MAGGOT KING",
        "maggot_king",
        (
            _boss("direct", "Maggot King KC", 0.70, ("maggot_king",), 50),
            _skills("skills", "Endgame combat skills", 0.20, ENDGAME_HYBRID),
            _ehb(0.10),
        ),
        burden=1.5,
    ),
    TileSpec(
        "final_toa",
        "Final grid",
        "TOA",
        "toa",
        (
            _boss(
                "direct",
                "TOA effective KC",
                0.75,
                ("tombs_of_amascut", "tombs_of_amascut_expert"),
                100,
                (1, 2),
            ),
            _skills("skills", "Endgame combat skills", 0.15, ENDGAME_HYBRID),
            _ehb(0.10),
        ),
        burden=1.5,
        raid=True,
    ),
)


TILES_BY_ID = {tile.tile_id: tile for tile in GRID_TILE_SPECS}
CONFIDENCE_ORDER = ("Unavailable", "Low", "Medium", "High")


def normalize_player_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _safe_number(value: Any) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(numeric) or numeric < 0:
        return 0.0
    return numeric


def _extract_metric_map(snapshot_data: dict[str, Any], section: str, value_key: str) -> dict[str, float]:
    raw_section = snapshot_data.get(section, {})
    if not isinstance(raw_section, dict):
        return {}
    values: dict[str, float] = {}
    for key, payload in raw_section.items():
        if isinstance(payload, dict) and value_key in payload:
            values[str(key)] = _safe_number(payload.get(value_key))
    return values


def calculate_combat_level(skills: dict[str, float]) -> int:
    required = ("attack", "strength", "defence", "hitpoints", "ranged", "magic", "prayer")
    if any(skill not in skills for skill in required):
        return 0
    attack = skills["attack"]
    strength = skills["strength"]
    defence = skills["defence"]
    hitpoints = skills["hitpoints"]
    ranged = skills["ranged"]
    magic = skills["magic"]
    prayer = skills["prayer"]
    base = 0.25 * (defence + hitpoints + math.floor(prayer / 2))
    melee = 0.325 * (attack + strength)
    ranged_style = 0.325 * math.floor(1.5 * ranged)
    magic_style = 0.325 * math.floor(1.5 * magic)
    return int(math.floor(base + max(melee, ranged_style, magic_style)))


def parse_bulk_hiscores(
    payload: Any,
    roster: tuple[RosterMember, ...] = FIDDLSTCKS_ROSTER,
) -> list[PlayerStats]:
    rows = payload if isinstance(payload, list) else []
    rows_by_name: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        player = row.get("player")
        if not isinstance(player, dict):
            continue
        for candidate in (player.get("username"), player.get("displayName")):
            key = normalize_player_name(candidate)
            if key:
                rows_by_name[key] = row

    parsed: list[PlayerStats] = []
    for member in roster:
        row = rows_by_name.get(normalize_player_name(member.wom_name))
        if row is None:
            parsed.append(
                PlayerStats(
                    draft_name=member.draft_name,
                    wom_name=member.wom_name,
                    matched_name=None,
                    player_id=None,
                    updated_at=None,
                    available=False,
                    ehb=None,
                    combat_level=0,
                    skills={},
                    bosses={},
                    activities={},
                )
            )
            continue

        player = row.get("player", {})
        snapshot = row.get("data", {})
        snapshot_data = snapshot.get("data", {}) if isinstance(snapshot, dict) else {}
        if not isinstance(snapshot_data, dict):
            snapshot_data = {}
        skills = _extract_metric_map(snapshot_data, "skills", "level")
        bosses = _extract_metric_map(snapshot_data, "bosses", "kills")
        activities = _extract_metric_map(snapshot_data, "activities", "score")
        available = bool(skills)
        raw_ehb = player.get("ehb")
        parsed.append(
            PlayerStats(
                draft_name=member.draft_name,
                wom_name=member.wom_name,
                matched_name=str(player.get("displayName") or player.get("username") or member.wom_name),
                player_id=int(player["id"]) if str(player.get("id", "")).isdigit() else None,
                updated_at=str(player.get("updatedAt")) if player.get("updatedAt") else None,
                available=available,
                ehb=_safe_number(raw_ehb) if raw_ehb is not None else None,
                combat_level=calculate_combat_level(skills),
                skills=skills,
                bosses=bosses,
                activities=activities,
            )
        )
    return parsed


def _percentile(value: float, population: Iterable[float], *, positive_only: bool = False) -> float:
    values = [float(item) for item in population if math.isfinite(float(item))]
    if positive_only:
        values = [item for item in values if item > 0]
        if value <= 0:
            return 0.0
    if not values:
        return 0.0
    return sum(item <= value for item in values) / len(values)


def _experience_score(value: float, benchmark: float, population: Iterable[float]) -> float:
    if value <= 0 or benchmark <= 0:
        return 0.0
    absolute = min(math.log1p(value) / math.log1p(benchmark), 1.0)
    relative = _percentile(math.log1p(value), (math.log1p(v) for v in population if v > 0), positive_only=True)
    return 0.65 * absolute + 0.35 * relative


def _achievement_score(value: float) -> float:
    if value <= 0:
        return 0.0
    if value < 2:
        return 0.80
    if value < 5:
        return 0.90
    return 1.0


def _format_number(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return f"{int(round(value)):,}"
    return f"{value:,.1f}"


METRIC_DISPLAY_NAMES = {
    "general_graardor": "Graardor",
    "commander_zilyana": "Zilyana",
    "kreearra": "Kree'arra",
    "kril_tsutsaroth": "K'ril",
    "dagannoth_prime": "Prime",
    "dagannoth_rex": "Rex",
    "dagannoth_supreme": "Supreme",
    "theatre_of_blood": "Normal TOB",
    "theatre_of_blood_hard_mode": "Hard-mode TOB",
    "vardorvis": "Vardorvis",
    "duke_sucellus": "Duke",
    "the_leviathan": "Leviathan",
    "the_whisperer": "Whisperer",
    "chambers_of_xeric": "Normal COX",
    "chambers_of_xeric_challenge_mode": "Challenge-mode COX",
    "tombs_of_amascut": "Normal TOA",
    "tombs_of_amascut_expert": "Expert TOA",
}


def _display_metric_name(metric: str) -> str:
    return METRIC_DISPLAY_NAMES.get(metric, metric.replace("_", " ").title())


def readiness_band(score: float | None) -> str:
    if score is None:
        return "Unscored"
    if score >= 80:
        return "Excellent"
    if score >= 65:
        return "Strong"
    if score >= 50:
        return "Possible"
    if score >= 35:
        return "Stretch"
    return "Weak"


def _component_raw(player: PlayerStats, component: ComponentSpec) -> tuple[float | None, list[str]]:
    missing: list[str] = []
    if component.kind in {"boss", "achievement"}:
        weights = component.multipliers or tuple(1.0 for _ in component.metrics)
        raw = 0.0
        for metric, multiplier in zip(component.metrics, weights):
            if metric not in player.bosses:
                missing.append(metric)
                continue
            raw += player.bosses[metric] * multiplier
        return (raw if len(missing) < len(component.metrics) else None), missing
    if component.kind == "activity":
        metric = component.metrics[0]
        if metric not in player.activities:
            return None, [metric]
        return player.activities[metric], []
    if component.kind == "ehb":
        if player.ehb is None:
            return None, ["ehb"]
        return player.ehb, []
    if component.kind == "gate":
        metric = component.metrics[0]
        if metric not in player.skills:
            return None, [metric]
        return player.skills[metric], []
    if component.kind == "skills":
        levels = []
        for metric, _target in component.targets:
            if metric not in player.skills:
                missing.append(metric)
                levels.append(0.0)
            else:
                levels.append(player.skills[metric])
        return (sum(levels) / len(levels) if levels else None), missing
    return None, [component.key]


def _component_score(
    player: PlayerStats,
    component: ComponentSpec,
    players: list[PlayerStats],
    raw_by_player: dict[str, float | None],
    direct_positive: bool,
) -> float:
    raw = raw_by_player.get(player.draft_name)
    if raw is None:
        return 0.0
    population = [value for value in raw_by_player.values() if value is not None]
    if component.kind in {"boss", "activity"}:
        return _experience_score(raw, component.benchmark, population)
    if component.kind == "achievement":
        return _achievement_score(raw)
    if component.kind == "ehb":
        if raw <= 0:
            return 0.0
        relative = _percentile(math.log1p(raw), (math.log1p(v) for v in population if v > 0), positive_only=True)
        absolute = min(math.log1p(raw) / math.log1p(component.benchmark), 1.0)
        return 0.60 * relative + 0.40 * absolute
    if component.kind == "gate":
        if direct_positive:
            return 1.0
        if raw >= component.gate_target:
            return 1.0
        if raw >= component.gate_target - 3:
            return 0.60
        return 0.0
    if component.kind == "skills":
        absolute_parts = []
        relative_parts = []
        for skill, target in component.targets:
            level = player.skills.get(skill)
            if level is None:
                absolute_parts.append(0.0)
                relative_parts.append(0.0)
                continue
            if target <= 60:
                absolute_parts.append(1.0 if level >= target else 0.0)
            else:
                absolute_parts.append(max(0.0, min((level - 60) / (target - 60), 1.0)))
            skill_population = [candidate.skills[skill] for candidate in players if skill in candidate.skills]
            relative_parts.append(_percentile(level, skill_population))
        absolute = sum(absolute_parts) / len(absolute_parts) if absolute_parts else 0.0
        relative = sum(relative_parts) / len(relative_parts) if relative_parts else 0.0
        return 0.80 * absolute + 0.20 * relative
    return 0.0


def _snapshot_age_days(updated_at: str | None, now: datetime) -> float | None:
    if not updated_at:
        return None
    try:
        parsed = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max((now - parsed.astimezone(timezone.utc)).total_seconds() / 86400, 0.0)
    except (TypeError, ValueError):
        return None


def _confidence(
    tile: TileSpec,
    player: PlayerStats,
    direct_positive: bool,
    adjacent_positive: bool,
    missing_weight: float,
    now: datetime,
) -> str:
    if not player.available:
        return "Unavailable"
    if tile.confidence_cap == "Low":
        return "Low"
    if missing_weight > 0.25:
        return "Low"
    level = "High" if direct_positive else "Medium"
    age_days = _snapshot_age_days(player.updated_at, now)
    if age_days is None or age_days > 90:
        level = "Low"
    elif age_days > 30 and level == "High":
        level = "Medium"
    return level


def _evidence_for(
    tile: TileSpec,
    player: PlayerStats,
    raws: dict[str, float | None],
) -> str:
    parts = []
    for component in tile.components:
        raw = raws.get(component.key)
        if raw is None:
            parts.append(f"{component.label}: missing")
        elif component.kind in {"boss", "achievement"}:
            multipliers = component.multipliers or tuple(1.0 for _ in component.metrics)
            breakdown = []
            for metric, multiplier in zip(component.metrics, multipliers):
                value = player.bosses.get(metric)
                if value is None:
                    breakdown.append(f"{_display_metric_name(metric)} missing")
                    continue
                multiplier_note = f" x{_format_number(multiplier)}" if multiplier != 1 else ""
                breakdown.append(
                    f"{_display_metric_name(metric)} {_format_number(value)}{multiplier_note}"
                )
            if len(component.metrics) > 1:
                parts.append(
                    f"{component.label} {_format_number(raw)} ({', '.join(breakdown)})"
                )
            else:
                parts.append(f"{component.label} {_format_number(raw)}")
        elif component.kind == "skills":
            skill_values = [
                (
                    f"{_display_metric_name(skill)} {_format_number(player.skills[skill])}"
                    if skill in player.skills
                    else f"{_display_metric_name(skill)} missing"
                )
                for skill, _target in component.targets
            ]
            parts.append(f"{component.label} ({', '.join(skill_values)})")
        elif component.kind == "gate":
            parts.append(f"{component.label} {_format_number(raw)}")
        elif component.kind == "ehb":
            parts.append(f"EHB {_format_number(raw)}")
        else:
            parts.append(f"{component.label} {_format_number(raw)}")
    if player.combat_level:
        parts.append(f"combat {player.combat_level}")
    return " | ".join(parts)


def _why_for(tile: TileSpec, raws: dict[str, float | None], score: float) -> str:
    primary = next((component for component in tile.components if component.key == "direct"), None)
    adjacent = next((component for component in tile.components if component.key == "adjacent"), None)
    if primary is not None and (raws.get(primary.key) or 0) > 0:
        lead = f"{primary.label}: {_format_number(float(raws[primary.key]))}"
    elif adjacent is not None and (raws.get(adjacent.key) or 0) > 0:
        lead = f"Proxy evidence: {adjacent.label} {_format_number(float(raws[adjacent.key]))}"
    else:
        lead = "No target KC recorded; score comes from skills and general PvM experience"
    if tile.note:
        return f"{lead}. {tile.note}"
    if score >= 65:
        return f"{lead}; the combined readiness index is strong."
    return f"{lead}; treat as a planning lead, not a completion guarantee."


def score_grid_tiles(
    players: list[PlayerStats],
    tiles: tuple[TileSpec, ...] = GRID_TILE_SPECS,
    *,
    now: datetime | None = None,
) -> list[ReadinessRecord]:
    now = now or datetime.now(timezone.utc)
    available_players = [player for player in players if player.available]
    results: list[ReadinessRecord] = []

    for tile in tiles:
        raw_by_component: dict[str, dict[str, float | None]] = {}
        missing_weight_by_player = {player.draft_name: 0.0 for player in available_players}
        for component in tile.components:
            raw_by_component[component.key] = {}
            for player in available_players:
                raw, missing = _component_raw(player, component)
                raw_by_component[component.key][player.draft_name] = raw
                input_count = len(component.metrics) + len(component.targets)
                if input_count:
                    missing_weight_by_player[player.draft_name] += (
                        component.weight * len(missing) / input_count
                    )

        for player in players:
            if not player.available:
                results.append(
                    ReadinessRecord(
                        tile_id=tile.tile_id,
                        section=tile.section,
                        tile=tile.label,
                        family=tile.family,
                        player=player.draft_name,
                        score=None,
                        band="Unscored",
                        confidence="Unavailable",
                        evidence="No matched WoM profile/snapshot",
                        why="Excluded from automatic assignment until a WoM profile is matched.",
                        component_details="Unavailable",
                        burden=tile.burden,
                        raid=tile.raid,
                    )
                )
                continue

            player_raws = {
                component.key: raw_by_component[component.key].get(player.draft_name)
                for component in tile.components
            }
            direct_positive = (player_raws.get("direct") or 0) > 0
            adjacent_positive = (player_raws.get("adjacent") or 0) > 0
            total_score = 0.0
            details = []
            for component in tile.components:
                component_score = _component_score(
                    player,
                    component,
                    available_players,
                    raw_by_component[component.key],
                    direct_positive,
                )
                contribution = component_score * component.weight * 100
                total_score += contribution
                details.append(
                    f"{component.label}: {component_score * 100:.1f}/100 x {component.weight:.0%} = {contribution:.1f}"
                )
            total_score = round(max(0.0, min(total_score, 100.0)), 1)
            confidence = _confidence(
                tile,
                player,
                direct_positive,
                adjacent_positive,
                missing_weight_by_player.get(player.draft_name, 0.0),
                now,
            )
            results.append(
                ReadinessRecord(
                    tile_id=tile.tile_id,
                    section=tile.section,
                    tile=tile.label,
                    family=tile.family,
                    player=player.draft_name,
                    score=total_score,
                    band=readiness_band(total_score),
                    confidence=confidence,
                    evidence=_evidence_for(tile, player, player_raws),
                    why=_why_for(tile, player_raws, total_score),
                    component_details="; ".join(details),
                    burden=tile.burden,
                    raid=tile.raid,
                )
            )

    return results


def rankings_by_tile(records: list[ReadinessRecord]) -> dict[str, list[ReadinessRecord]]:
    grouped: dict[str, list[ReadinessRecord]] = {}
    for record in records:
        grouped.setdefault(record.tile_id, []).append(record)
    for tile_id, rows in grouped.items():
        grouped[tile_id] = sorted(
            rows,
            key=lambda row: (
                row.score is None,
                -(row.score or 0),
                row.player,
            ),
        )
    return grouped


def _confidence_penalty(confidence: str) -> float:
    return {"High": 0.0, "Medium": 3.0, "Low": 8.0}.get(confidence, 100.0)


def build_balanced_assignments(
    records: list[ReadinessRecord],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    grouped = rankings_by_tile(records)
    scored_players = sorted(
        {
            record.player
            for record in records
            if record.score is not None and record.confidence != "Unavailable"
        }
    )
    if not scored_players:
        return [], []

    tile_count = len(grouped)
    soft_target = tile_count / len(scored_players)
    soft_max = math.ceil(soft_target)
    hard_max = soft_max + 1
    total_burden = sum(TILES_BY_ID[tile_id].burden for tile_id in grouped)
    mean_burden = total_burden / len(scored_players)
    counts = {player: 0 for player in scored_players}
    burdens = {player: 0.0 for player in scored_players}
    families = {player: set() for player in scored_players}

    ordered_tiles = sorted(
        grouped,
        key=lambda tile_id: (
            sum(1 for row in grouped[tile_id] if row.score is not None and row.score >= 50),
            -TILES_BY_ID[tile_id].burden,
            list(TILES_BY_ID).index(tile_id),
        ),
    )

    primary_by_tile: dict[str, ReadinessRecord] = {}
    for tile_id in ordered_tiles:
        candidates = [row for row in grouped[tile_id] if row.score is not None]
        under_hard_cap = [row for row in candidates if counts[row.player] < hard_max]
        pool = under_hard_cap or candidates

        def utility(row: ReadinessRecord) -> tuple[float, float, str]:
            projected_count = counts[row.player] + 1
            projected_burden = burdens[row.player] + row.burden
            count_penalty = 7 * max(0.0, projected_count - soft_target)
            burden_penalty = 6 * max(0.0, projected_burden - mean_burden)
            family_penalty = 12 if row.family in families[row.player] else 0
            value = (
                float(row.score)
                - count_penalty
                - burden_penalty
                - family_penalty
                - _confidence_penalty(row.confidence)
            )
            return value, float(row.score), row.player

        chosen = max(pool, key=utility)

        primary_by_tile[tile_id] = chosen
        counts[chosen.player] += 1
        burdens[chosen.player] += chosen.burden
        families[chosen.player].add(chosen.family)

    assignments: list[dict[str, Any]] = []
    tile_order = {tile.tile_id: index for index, tile in enumerate(GRID_TILE_SPECS)}
    for tile_id in sorted(primary_by_tile, key=tile_order.get):
        primary = primary_by_tile[tile_id]
        ranked = grouped[tile_id]
        backups = [row for row in ranked if row.score is not None and row.player != primary.player]
        backup = backups[0] if backups else None
        squad = (
            [primary.player] + [row.player for row in backups[:2]]
            if primary.raid
            else []
        )
        label = primary.tile
        if tile_id == "final_corrupted_gauntlet":
            label += " (OPEN FROM START)"
        assignments.append(
            {
                "Tile ID": tile_id,
                "Section": primary.section,
                "Tile": label,
                "Primary": primary.player,
                "Readiness": primary.score,
                "Band": primary.band,
                "Confidence": primary.confidence,
                "Backup": backup.player if backup else "N/A",
                "Backup Score": backup.score if backup else None,
                "Backup Confidence": backup.confidence if backup else "N/A",
                "Recommended Squad": ", ".join(squad),
                "Raw WOM Evidence": primary.evidence,
                "Why": primary.why,
                "Burden": primary.burden,
            }
        )

    workload: list[dict[str, Any]] = []
    for player in scored_players:
        player_assignments = [row for row in assignments if row["Primary"] == player]
        scores = [float(row["Readiness"]) for row in player_assignments]
        workload.append(
            {
                "Player": player,
                "Primary Tiles": len(player_assignments),
                "Burden": round(sum(float(row["Burden"]) for row in player_assignments), 2),
                "Average Readiness": round(sum(scores) / len(scores), 1) if scores else None,
                "Assignments": ", ".join(row["Tile"] for row in player_assignments) or "Unassigned",
            }
        )
    workload.sort(key=lambda row: (-row["Primary Tiles"], row["Player"]))
    return assignments, workload


def tile_mapping_rows(tiles: tuple[TileSpec, ...] = GRID_TILE_SPECS) -> list[dict[str, Any]]:
    rows = []
    for tile in tiles:
        model_parts = []
        metric_keys = []
        for component in tile.components:
            model_parts.append(f"{component.label} {component.weight:.0%}")
            metric_keys.extend(component.metrics)
            metric_keys.extend(skill for skill, _target in component.targets)
        rows.append(
            {
                "Section": tile.section,
                "Tile": tile.label,
                "Readiness Model": " + ".join(model_parts),
                "WOM Keys": ", ".join(dict.fromkeys(metric_keys)) or "player EHB / skill levels",
                "Evidence Confidence": tile.confidence_cap or "High when direct KC is positive",
                "Planning Note": tile.note,
            }
        )
    return rows


def roster_coverage_rows(players: list[PlayerStats]) -> list[dict[str, Any]]:
    return [
        {
            "Draft Name": player.draft_name,
            "WOM Match": player.matched_name or "No match",
            "Combat": player.combat_level if player.available else None,
            "EHB": round(player.ehb, 1) if player.available and player.ehb is not None else None,
            "Snapshot Updated": player.updated_at or "Unavailable",
            "Status": "Matched" if player.available else "Unscored",
        }
        for player in players
    ]
