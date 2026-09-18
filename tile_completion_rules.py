"""Declarative Summer Bingo 2026 tile-completion rules.

This module is intentionally independent from the Streamlit and board-progression
code.  It translates one event-log submission at a time into a small per-tile
state, evaluates the planner's completion routes, and exposes structured and
human-readable progress.

The 24 grid rules mirror ``fiddlstcks-tile-planner/tile_checklists.py``.  The six
hallway rules mirror the goals in ``tile_difficulty.py``.  Corporeal Beast is the
board's bonus tile; its event-log recipe is Spirit shield + Holy elixir + any
sigil.  Unlock timing is deliberately out of scope here: callers decide whether
a submission was eligible before applying it (including the one starting-CG-
chest exception).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Iterable, Literal, Mapping
import re


RequirementMode = Literal["occurrences", "distinct", "players"]


class CompletionRulesError(ValueError):
    """Raised for an unknown/ambiguous tile or malformed state request."""


@dataclass(frozen=True, slots=True)
class RequirementSpec:
    label: str
    item_keys: tuple[str, ...]
    target: int
    mode: RequirementMode = "occurrences"
    caps: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True, slots=True)
class RouteSpec:
    route_id: str
    label: str
    requirements: tuple[RequirementSpec, ...]


@dataclass(frozen=True, slots=True)
class TileRule:
    tile_id: str
    label: str
    section: str
    routes: tuple[RouteSpec, ...]
    notes: tuple[str, ...] = ()

    @property
    def accepted_item_keys(self) -> frozenset[str]:
        return frozenset(
            key
            for route in self.routes
            for requirement in route.requirements
            for key in requirement.item_keys
        )

    @property
    def player_item_keys(self) -> frozenset[str]:
        return frozenset(
            key
            for route in self.routes
            for requirement in route.requirements
            if requirement.mode == "players"
            for key in requirement.item_keys
        )


@dataclass(slots=True)
class TileState:
    """Mutable, JSON-friendly-in-spirit aggregate for one physical tile."""

    tile_id: str
    counts: dict[str, int] = field(default_factory=dict)
    players_by_item: dict[str, set[str]] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SubmissionOutcome:
    accepted: bool
    item_key: str | None
    amount: int
    reason: str


@dataclass(frozen=True, slots=True)
class RequirementProgress:
    label: str
    current: int
    target: int
    satisfied: bool


@dataclass(frozen=True, slots=True)
class RouteProgress:
    route_id: str
    label: str
    complete: bool
    blocked: bool
    requirements: tuple[RequirementProgress, ...]


@dataclass(frozen=True, slots=True)
class CompletionEvaluation:
    tile_id: str
    complete: bool
    routes: tuple[RouteProgress, ...]


def _req(
    label: str,
    item_keys: Iterable[str],
    target: int,
    mode: RequirementMode = "occurrences",
    *,
    caps: Mapping[str, int] | None = None,
) -> RequirementSpec:
    return RequirementSpec(
        label,
        tuple(item_keys),
        target,
        mode,
        tuple((key, value) for key, value in (caps or {}).items()),
    )


def _route(route_id: str, label: str, *requirements: RequirementSpec) -> RouteSpec:
    return RouteSpec(route_id, label, tuple(requirements))


def _tile(
    tile_id: str,
    label: str,
    section: str,
    *routes: RouteSpec,
    notes: Iterable[str] = (),
) -> TileRule:
    return TileRule(tile_id, label, section, tuple(routes), tuple(notes))


# Shared item families -------------------------------------------------------

GRAARDOR_UNIQUES = (
    "bandos_hilt",
    "bandos_chestplate",
    "bandos_tassets",
    "bandos_boots",
    "pet_general_graardor",
)
ZILYANA_UNIQUES = (
    "saradomin_hilt",
    "armadyl_crossbow",
    "saradomin_sword",
    "pet_zilyana",
)
KREE_UNIQUES = (
    "armadyl_hilt",
    "armadyl_helmet",
    "armadyl_chestplate",
    "armadyl_chainskirt",
    "pet_kreearra",
)
KRIL_UNIQUES = (
    "zamorak_hilt",
    "zamorakian_spear",
    "steam_battlestaff",
    "staff_of_the_dead",
    "pet_kril_tsutsaroth",
)
NON_ANCIENT_HILTS = (
    "bandos_hilt",
    "saradomin_hilt",
    "armadyl_hilt",
    "zamorak_hilt",
)
GODSWORD_SHARDS = ("godsword_shard_1", "godsword_shard_2", "godsword_shard_3")

TOA_ACCEPTED = (
    "osmumtens_fang",
    "lightbearer",
    "elidinis_ward",
    "masori_mask",
    "masori_body",
    "masori_chaps",
    "tumekens_shadow",
    "tumekens_guardian",
)
NEX_ACCEPTED = (
    "ancient_hilt",
    "nihil_horn",
    "zaryte_vambraces",
    "torva_full_helm",
    "torva_platebody",
    "torva_platelegs",
    "nexling",
)
HUEY_ACCEPTED = (
    "dragon_hunter_wand",
    "hueycoatl_hide",
    "tome_of_earth",
    "huberte",
)
NIGHTMARE_ACCEPTED = (
    "nightmare_staff",
    "inquisitors_great_helm",
    "inquisitors_hauberk",
    "inquisitors_plateskirt",
    "eldritch_orb",
    "volatile_orb",
    "harmonised_orb",
    "little_nightmare",
    "jar_of_dreams",
)
COX_ACCEPTED = (
    "arcane_prayer_scroll",
    "dexterous_prayer_scroll",
    "dinhs_bulwark",
    "dragon_hunter_crossbow",
    "dragon_claws",
    "twisted_buckler",
    "ancestral_hat",
    "ancestral_robe_top",
    "ancestral_robe_bottom",
    "kodai_insignia",
    "elder_maul",
    "twisted_bow",
    "olmlet",
)
TOB_ACCEPTED = (
    "avernic_defender_hilt",
    "ghrazi_rapier",
    "sanguinesti_staff",
    "justiciar_faceguard",
    "justiciar_chestguard",
    "justiciar_legguards",
    "scythe_of_vitur",
    "lil_zik",
)

ANCIENT_ARTEFACTS = (
    "ancient_emblem",
    "ancient_totem",
    "ancient_statuette",
    "ancient_medallion",
    "ancient_effigy",
    "ancient_relic",
)
REVENANT_WEAPONS = ("craws_bow", "thammarons_sceptre", "viggoras_chainmace")
COLOSSEUM_REWARDS = (
    "sunfire_fanatic_helm",
    "sunfire_fanatic_cuirass",
    "sunfire_fanatic_chausses",
    "echo_crystal",
    "smol_heredit",
)
CERBERUS_CRYSTALS = ("primordial_crystal", "pegasian_crystal", "eternal_crystal")
DT2_AXE_PIECES = (
    "leviathans_lure",
    "sirens_staff",
    "executioners_axe_head",
    "eye_of_the_duke",
)
DT2_VIRTUS = ("virtus_mask", "virtus_robe_top", "virtus_robe_bottom")
DT2_PETS = ("baron", "butch", "lilviathan", "wisp")
ANCESTRAL = ("ancestral_hat", "ancestral_robe_top", "ancestral_robe_bottom")
MASORI = ("masori_mask", "masori_body", "masori_chaps")
CORP_SIGILS = ("arcane_sigil", "elysian_sigil", "spectral_sigil")

BARROWS_SETS: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "ahrim": ("ahrims_hood", "ahrims_robetop", "ahrims_robeskirt", "ahrims_staff"),
        "dharok": (
            "dharoks_helm",
            "dharoks_platebody",
            "dharoks_platelegs",
            "dharoks_greataxe",
        ),
        "guthan": (
            "guthans_helm",
            "guthans_platebody",
            "guthans_chainskirt",
            "guthans_warspear",
        ),
        "karil": (
            "karils_coif",
            "karils_leathertop",
            "karils_leatherskirt",
            "karils_crossbow",
        ),
        "torag": (
            "torags_helm",
            "torags_platebody",
            "torags_platelegs",
            "torags_hammers",
        ),
        "verac": (
            "veracs_helm",
            "veracs_brassard",
            "veracs_plateskirt",
            "veracs_flail",
        ),
    }
)


# Rules ----------------------------------------------------------------------

_RULES = (
    # Opening hallway.
    _tile(
        "opening_hallway_toa",
        "TOA (start tile)",
        "Opening hallway",
        _route("any_two", "Obtain any two accepted TOA uniques", _req("Accepted unique drops", TOA_ACCEPTED, 2)),
        notes=("Duplicates count; pre-event chests do not count.",),
    ),
    _tile(
        "opening_hallway_nex",
        "NEX",
        "Opening hallway",
        _route("any_two", "Obtain any two accepted Nex uniques", _req("Accepted unique drops", NEX_ACCEPTED, 2)),
        notes=("Duplicates count.",),
    ),
    _tile(
        "opening_hallway_hueycoatl",
        "HUEYCOATL",
        "Opening hallway",
        _route("any_two", "Obtain any two accepted Hueycoatl uniques", _req("Accepted unique drops", HUEY_ACCEPTED, 2)),
        notes=("Duplicates count; a multi-hide drop is one occurrence.",),
    ),
    # First grid.
    _tile(
        "grid_gwd",
        "GWD",
        "First grid",
        _route(
            "all_requirements",
            "Complete every GWD requirement",
            _req("Distinct Godsword shards", GODSWORD_SHARDS, 3, "distinct"),
            _req("Non-Ancient godsword hilt", NON_ANCIENT_HILTS, 1, "distinct"),
            _req("General Graardor unique", GRAARDOR_UNIQUES, 1),
            _req("Commander Zilyana unique", ZILYANA_UNIQUES, 1),
            _req("Kree'arra unique", KREE_UNIQUES, 1),
            _req("K'ril Tsutsaroth unique", KRIL_UNIQUES, 1),
        ),
        notes=("A qualifying hilt also provides its general's credit; shards do not.",),
    ),
    _tile(
        "grid_venator_shards",
        "VENATOR SHARDS",
        "First grid",
        _route("five_shards", "Collect five shards", _req("Venator shards", ("venator_shard",), 5)),
        notes=("Muphin does not count; cache descendants require a post-unlock root cache.",),
    ),
    _tile(
        "grid_vorkath",
        "VORKATH",
        "First grid",
        _route("team_kills", "Reach 300 team kills", _req("Post-unlock team kills", ("vorkath_kills",), 300)),
        _route("any_visage", "Obtain either visage", _req("Vorkath visage", ("draconic_visage", "skeletal_visage"), 1, "distinct")),
        notes=("Visages must come from Vorkath; Vorki does not count.",),
    ),
    _tile(
        "grid_dks",
        "DKS",
        "First grid",
        _route(
            "all_drops",
            "Collect all seven drops",
            _req(
                "All required DKS drops",
                (
                    "archers_ring",
                    "warrior_ring",
                    "seers_ring",
                    "berserker_ring",
                    "dragon_axe",
                    "mud_battlestaff",
                    "seercull",
                ),
                7,
                "distinct",
            ),
        ),
        notes=("The Dragon axe must come from a Dagannoth King; pets do not count.",),
    ),
    _tile(
        "grid_revs",
        "REVS",
        "First grid",
        _route(
            "artefacts_and_weapon",
            "Three artefacts and one weapon",
            _req("Ancient artefact drops", ANCIENT_ARTEFACTS, 3),
            _req("Revenant weapon", REVENANT_WEAPONS, 1, "distinct"),
        ),
        notes=("Artefact duplicates count; Ancient crystals do not.",),
    ),
    _tile(
        "grid_tob",
        "TOB",
        "First grid",
        _route("any_two", "Obtain any two accepted uniques", _req("Accepted unique drops", TOB_ACCEPTED, 2)),
        notes=("Duplicates count; pre-stacked chests do not. Using Scythe blocks final TOB's Scythe route.",),
    ),
    _tile(
        "grid_colosseum",
        "COLOSSEUM",
        "First grid",
        _route("five_rewards", "Collect five reward occurrences", _req("Qualifying reward occurrences", COLOSSEUM_REWARDS, 5)),
        _route("tonalztics", "Obtain Tonalztics of Ralos", _req("Tonalztics of Ralos", ("tonalztics_of_ralos",), 1, "distinct")),
        notes=("Reward duplicates count; an Echo stack is one occurrence; Smol must be from the final chest.",),
    ),
    _tile(
        "grid_zenytes",
        "ZENYTES",
        "First grid",
        _route("six_shards", "Collect six shards", _req("Zenyte shards", ("zenyte_shard",), 6)),
    ),
    _tile(
        "grid_royal_titans",
        "ROYAL TITANS",
        "First grid",
        _route(
            "two_staves",
            "Collect both pairs of crowns",
            _req("Fire crowns", ("fire_element_staff_crown",), 2),
            _req("Ice crowns", ("ice_element_staff_crown",), 2),
        ),
    ),
    _tile(
        "grid_barrows",
        "BARROWS",
        "First grid",
        *(
            _route(
                f"{brother}_set",
                f"Complete {brother.title()}'s set",
                _req(f"{brother.title()}'s four distinct pieces", items, 4, "distinct"),
            )
            for brother, items in BARROWS_SETS.items()
        ),
    ),
    _tile(
        "grid_cerberus",
        "CERBERUS",
        "First grid",
        _route("three_crystals", "Collect all three crystals", _req("Distinct crystals", CERBERUS_CRYSTALS, 3, "distinct")),
        _route(
            "stone_substitution",
            "Use one Smouldering stone substitution",
            _req("Distinct crystals", CERBERUS_CRYSTALS, 2, "distinct"),
            _req("Smouldering stone", ("smouldering_stone",), 1, "distinct"),
        ),
        notes=("Cerberus must be killed on a Slayer task; the stone substitutes once.",),
    ),
    _tile(
        "grid_the_mad_angel",
        "THE MAD ANGEL",
        "First grid",
        _route("three_hallowfells", "Collect three Hallowfells", _req("Hallowfell drops", ("hallowfell",), 3)),
        _route("jar", "Obtain Jar of light", _req("Jar of light", ("jar_of_light",), 1, "distinct")),
        _route("pet", "Obtain Aggy", _req("Aggy", ("aggy",), 1, "distinct")),
    ),
    # Middle hallway.
    _tile(
        "middle_hallway_voidwaker",
        "VOIDWAKER",
        "Middle hallway",
        _route(
            "all_pieces",
            "Complete the Voidwaker",
            _req("Hilt, gem, and blade", ("voidwaker_hilt", "voidwaker_gem", "voidwaker_blade"), 3, "distinct"),
        ),
    ),
    _tile(
        "middle_hallway_nightmare",
        "NIGHTMARE / PNM",
        "Middle hallway",
        _route("any_two", "Obtain any two accepted Nightmare or PNM uniques", _req("Accepted unique drops", NIGHTMARE_ACCEPTED, 2)),
        notes=("Duplicates count.",),
    ),
    _tile(
        "middle_hallway_cox",
        "COX",
        "Middle hallway",
        _route("any_two", "Obtain any two accepted COX uniques", _req("Accepted unique drops", COX_ACCEPTED, 2)),
        notes=("Duplicates count; megascales, dust, and twisted kits do not. Using Tbow blocks final COX's Tbow route.",),
    ),
    # Final grid.
    _tile(
        "final_corrupted_gauntlet",
        "CORRUPTED GAUNTLET",
        "Final grid",
        _route("five_armour_seeds", "Collect five armour seeds", _req("Crystal armour seeds", ("crystal_armour_seed",), 5)),
        _route("enhanced_seed", "Obtain an enhanced seed", _req("Enhanced weapon seed", ("enhanced_crystal_weapon_seed",), 1, "distinct")),
        notes=("Corrupted Gauntlet only; exactly one prepared starting chest is the sole pre-unlock exception.",),
    ),
    _tile(
        "final_araxxor",
        "ARAXXOR",
        "Final grid",
        _route(
            "halberd",
            "Complete the Noxious halberd set",
            _req("Three distinct halberd parts", ("noxious_point", "noxious_blade", "noxious_pommel"), 3, "distinct"),
        ),
        _route("two_fangs", "Collect two Araxyte fangs", _req("Araxyte fang drops", ("araxyte_fang",), 2)),
        _route("pet", "Obtain Nid", _req("Nid", ("nid",), 1, "distinct")),
        notes=("Araxxor must be killed on a Slayer task; partial routes do not combine.",),
    ),
    _tile(
        "final_dt2",
        "DT2",
        "Final grid",
        _route("axe", "Complete the Soulreaper axe pieces", _req("Four distinct axe pieces", DT2_AXE_PIECES, 4, "distinct")),
        _route("virtus", "Complete the Virtus set", _req("Three distinct Virtus pieces", DT2_VIRTUS, 3, "distinct")),
        _route("pets", "Obtain any two DT2 pets", _req("DT2 pet drops", DT2_PETS, 2)),
        notes=("The same pet may repeat; vestiges do not count; partial routes do not combine.",),
    ),
    _tile(
        "final_tob",
        "TOB",
        "Final grid",
        _route(
            "justiciar",
            "Complete the Justiciar set",
            _req("Three distinct Justiciar pieces", ("justiciar_faceguard", "justiciar_chestguard", "justiciar_legguards"), 3, "distinct"),
        ),
        _route("scythe", "Obtain an eligible Scythe", _req("Eligible Scythe", ("scythe_of_vitur",), 1, "distinct")),
        notes=("Block route 'scythe' if a Scythe completed First-grid TOB; Lil' zik does not count.",),
    ),
    _tile(
        "final_doom",
        "DOOM",
        "Final grid",
        _route(
            "all_three",
            "Claim all three distinct uniques",
            _req("Claimed distinct items", ("eye_of_ayak", "mokhaiotl_cloth", "avernic_treads"), 3, "distinct"),
        ),
        notes=("Items count only when claimed; duplicates and Dom do not count.",),
    ),
    _tile(
        "final_inferno",
        "INFERNO",
        "Final grid",
        _route("three_players", "Three different players claim capes", _req("Distinct cape claimants", ("infernal_cape",), 3, "players")),
        notes=("Each signed player counts at most once; Jal-nib-rek does not count.",),
    ),
    _tile(
        "final_yama",
        "YAMA",
        "Final grid",
        _route(
            "any_three",
            "Obtain any three qualifying drops",
            _req(
                "Qualifying occurrences",
                ("oathplate_helm", "oathplate_chest", "oathplate_legs", "yami"),
                3,
                caps={"yami": 1},
            ),
        ),
        notes=("Armour duplicates count; Yami counts once; Soulflame Horn does not count.",),
    ),
    _tile(
        "final_cox",
        "COX",
        "Final grid",
        _route("ancestral_set", "Complete the Ancestral set", _req("Three distinct Ancestral pieces", ANCESTRAL, 3, "distinct")),
        _route(
            "wildcard",
            "Two Ancestral pieces and one wildcard",
            _req("Distinct Ancestral pieces", ANCESTRAL, 2, "distinct"),
            _req("Kodai or Elder maul wildcard", ("kodai_insignia", "elder_maul"), 1, "distinct"),
        ),
        _route("twisted_bow", "Obtain an eligible Twisted bow", _req("Eligible Twisted bow", ("twisted_bow",), 1, "distinct")),
        notes=("Megascales do not count. Block route 'twisted_bow' if a bow completed the CoX hallway.",),
    ),
    _tile(
        "final_tormented_demons",
        "TORMENTED DEMONS",
        "Final grid",
        _route("any_six", "Obtain any six qualifying uniques", _req("Synapse and claw drops", ("tormented_synapse", "burning_claw"), 6)),
        notes=("Duplicates count; at most one qualifying result per kill.",),
    ),
    _tile(
        "final_zulrah",
        "ZULRAH",
        "Final grid",
        _route("six_main", "Collect six main-table uniques", _req("Main-table unique drops", ("tanzanite_fang", "magic_fang", "serpentine_visage"), 6)),
        _route("mutagen", "Obtain either mutagen", _req("Tanzanite or Magma mutagen", ("tanzanite_mutagen", "magma_mutagen"), 1, "distinct")),
        _route("pet", "Obtain Snakeling", _req("Snakeling", ("snakeling",), 1, "distinct")),
        notes=("Main-table duplicates and both eligible rolls from one kill count; uncut onyx does not.",),
    ),
    _tile(
        "final_maggot_king",
        "MAGGOT KING",
        "Final grid",
        _route(
            "four_items",
            "Collect two fangs and two kistens",
            _req("Elder venator fangs", ("elder_venator_fang",), 2),
            _req("Crimson kistens", ("crimson_kisten",), 2),
        ),
        _route(
            "pet_substitution",
            "Use the pet as one of each item",
            _req("Fang progress including pet", ("elder_venator_fang", "maggot_marquess"), 2, caps={"maggot_marquess": 1}),
            _req("Kisten progress including pet", ("crimson_kisten", "maggot_marquess"), 2, caps={"maggot_marquess": 1}),
        ),
    ),
    _tile(
        "final_toa",
        "TOA",
        "Final grid",
        _route("masori_set", "Complete the Masori set", _req("Three distinct Masori pieces", MASORI, 3, "distinct")),
        _route("shadow", "Obtain an eligible Shadow", _req("Eligible Tumeken's shadow", ("tumekens_shadow",), 1, "distinct")),
        notes=("Block route 'shadow' if a Shadow completed opening TOA; Guardian does not count.",),
    ),
    # Bonus tile inferred from the event-log recipe.
    _tile(
        "bonus_corp_beast",
        "CORPOREAL BEAST",
        "Bonus",
        _route(
            "components",
            "Collect all Spirit shield components",
            _req("Spirit shield", ("spirit_shield",), 1, "distinct"),
            _req("Holy elixir", ("holy_elixir",), 1, "distinct"),
            _req("Any sigil", CORP_SIGILS, 1, "distinct"),
        ),
        notes=(
            "Bonus recipe inferred from the supplied event log; the planner does not define a Corp checklist.",
        ),
    ),
)

TILE_RULES: Mapping[str, TileRule] = MappingProxyType({rule.tile_id: rule for rule in _RULES})
if len(TILE_RULES) != 31:
    raise RuntimeError("The Summer board rule catalog must contain 30 race tiles plus Corp")


# Input normalization --------------------------------------------------------

def normalize_item_name(value: object) -> str:
    """Return the punctuation-insensitive lookup form used by ITEM_ALIASES."""

    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _slug(value: str) -> str:
    # Possessive apostrophes are part of an item's spelling, not a word break:
    # ``Ahrim's hood`` should become ``ahrims_hood``.
    without_apostrophes = value.casefold().replace("'", "").replace("’", "")
    return re.sub(r"[^a-z0-9]+", "_", without_apostrophes).strip("_")


# Every distinct Item Received value in the supplied Summer CSV appears here.
# Extra labels cover checklist routes that did not happen to appear in the log.
_ITEM_LABELS = (
    "Aggy", "Ahrim's Hood", "Ahrim's Robeskirt", "Ahrim's Robetop", "Ahrim's Staff",
    "Ancestral Hat", "Ancestral Robe Bottom", "Ancestral Robe Top",
    "Ancient Effigy", "Ancient Emblem", "Ancient Hilt", "Ancient Medallion", "Ancient Relic", "Ancient Statuette", "Ancient Totem",
    "Araxyte Fang", "Arcane Prayer Scroll", "Archers Ring", "Armadyl Chestplate", "Armadyl Chainskirt", "Armadyl Crossbow", "Armadyl Helmet", "Armadyl Hilt",
    "Avernic Treads", "Bandos Boots", "Bandos Chestplate", "Bandos Hilt", "Bandos Tassets", "Baron", "Berserker Ring", "Burning Claw", "Butch",
    "Craw's Bow", "Crimson Kisten", "Crystal Armour Seed", "Dexterous Prayer Scroll",
    "Dharok's Greataxe", "Dharok's Helm", "Dharok's Platebody", "Dharok's Platelegs", "Dinh's Bulwark", "Dragon Axe", "Dragon Claws", "Dragon Hunter Crossbow", "Dragon Hunter Wand", "Draconic Visage",
    "Echo Crystal", "Elder Maul", "Elder Venator Fang", "Eldritch Orb", "Elidinis' Ward", "Elysian Sigil", "Enhanced Crystal Weapon Seed", "Eternal Crystal", "Executioner's Axe Head", "Eye of Ayak", "Eye of the Duke",
    "Fire Element Staff Crown", "Ghrazi Rapier", "Godsword Shard 1", "Godsword Shard 2", "Godsword Shard 3",
    "Guthan's Chainskirt", "Guthan's Helm", "Guthan's Platebody", "Guthan's Warspear",
    "Hallowfell", "Harmonised Orb", "Holy Elixir", "Huberte", "Hueycoatl Hide",
    "Ice Element Staff Crown", "Infernal Cape", "Inquisitor's Great Helm", "Inquisitor's Hauberk", "Inquisitor's Plateskirt",
    "Jar of Dreams", "Jar of Light", "Justiciar Chestguard", "Justiciar Faceguard", "Justiciar Legguards",
    "Karil's Coif", "Karil's Crossbow", "Karil's Leatherskirt", "Karil's Leathertop", "Kodai Insignia",
    "Leviathan's Lure", "Lightbearer", "Lil'viathan", "Lil'Zik", "Little Nightmare",
    "Magic Fang", "Magma Mutagen", "Maggot Marquess", "Masori Body", "Masori Chaps", "Masori Mask", "Mokhaiotl Cloth", "Mud Battlestaff",
    "Nexling", "Nid", "Nightmare Staff", "Nihil Horn", "Noxious blade", "Noxious point", "Noxious pommel",
    "Oathplate Chest", "Oathplate Helm", "Oathplate Legs", "Olmlet", "Osmumten's Fang",
    "Pegasian Crystal", "Pet General Graardor", "Pet Kree'arra", "Pet K'ril Tsutsaroth", "Pet Zilyana", "Primordial Crystal",
    "Sanguinesti Staff", "Saradomin Hilt", "Saradomin Sword", "Scythe of Vitur", "Seercull", "Seers Ring", "Serpentine Visage", "Siren's Staff", "Skeletal Visage", "Smol Heredit", "Smouldering Stone", "Snakeling", "Spectral Sigil", "Spirit Shield", "Staff of the Dead", "Steam Battlestaff",
    "Sunfire Fanatic Chausses", "Sunfire Fanatic Cuirass", "Sunfire Fanatic Helm",
    "Tanzanite Fang", "Tanzanite Mutagen", "Thammaron's Sceptre", "Tome of Earth", "Tonalztics of Ralos", "Torag's Hammers", "Torag's Helm", "Torag's Platebody", "Torag's Platelegs", "Tormented Synapse", "Torva Full Helm", "Torva Platebody", "Torva Platelegs", "Tumeken's Guardian", "Tumeken's Shadow", "Twisted Bow", "Twisted Buckler",
    "Venator Shard", "Verac's Brassard", "Verac's Flail", "Verac's Helm", "Verac's Plateskirt", "Viggora's Chainmace", "Virtus Mask", "Virtus Robe Bottom", "Virtus Robe Top", "Voidwaker Blade", "Voidwaker Gem", "Voidwaker Hilt", "Volatile Orb",
    "Warrior Ring", "Wisp", "Yami", "Zamorak Hilt", "Zamorakian Spear", "Zaryte Vambraces", "Zenyte Shard",
    "Arcane Sigil",
)

_ITEM_KEY_OVERRIDES: Mapping[str, str | None] = MappingProxyType(
    {
        "Avernic Defender": "avernic_defender_hilt",
        "Avernic Defender Hilt": "avernic_defender_hilt",
        "Lil'Zik": "lil_zik",
        "Lil' zik": "lil_zik",
        "Lil'viathan": "lilviathan",
        "Smol Heredit": "smol_heredit",
        # Known submitted nonqualifiers. They remain recognized so they are not
        # reported as spelling/alias failures, but apply_submission rejects them.
        "Soulflame Horn": None,
        "Twisted Ancestral colour kit": None,
        "Twisted Ancestral color kit": None,
    }
)


def _build_item_aliases() -> tuple[Mapping[str, str | None], Mapping[str, int]]:
    aliases: dict[str, str | None] = {}
    amounts: dict[str, int] = {}
    for label in _ITEM_LABELS:
        aliases[normalize_item_name(label)] = _ITEM_KEY_OVERRIDES.get(label, _slug(label))
    for label, key in _ITEM_KEY_OVERRIDES.items():
        aliases[normalize_item_name(label)] = key

    aliases[normalize_item_name("300 Kill Count")] = "vorkath_kills"
    amounts[normalize_item_name("300 Kill Count")] = 300
    aliases[normalize_item_name("Vorkath kill")] = "vorkath_kills"
    aliases[normalize_item_name("Vorkath kills")] = "vorkath_kills"
    return MappingProxyType(aliases), MappingProxyType(amounts)


ITEM_ALIASES, ITEM_ALIAS_AMOUNTS = _build_item_aliases()


def normalize_tile_name(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


_TILE_ALIAS_ROWS = (
    ("Tombs of Amascut", ("opening_hallway_toa",)),
    ("Tombs of Amascut 2", ("final_toa",)),
    ("Nex", ("opening_hallway_nex",)),
    ("Hueycoatl", ("opening_hallway_hueycoatl",)),
    ("God Wars Dungeon", ("grid_gwd",)),
    ("GWD", ("grid_gwd",)),
    ("Muspah", ("grid_venator_shards",)),
    ("Venator Shards", ("grid_venator_shards",)),
    ("Vorkath", ("grid_vorkath",)),
    ("Dagannoth Kings", ("grid_dks",)),
    ("DKS", ("grid_dks",)),
    ("Revenants", ("grid_revs",)),
    ("Theatre of Blood", ("grid_tob", "final_tob")),
    ("TOB", ("grid_tob", "final_tob")),
    ("Fortis Colosseum", ("grid_colosseum",)),
    ("Colosseum", ("grid_colosseum",)),
    ("Zenyte Shards", ("grid_zenytes",)),
    ("Royal Titans", ("grid_royal_titans",)),
    ("Barrows", ("grid_barrows",)),
    ("Cerberus", ("grid_cerberus",)),
    ("Mad Angel", ("grid_the_mad_angel",)),
    ("The Mad Angel", ("grid_the_mad_angel",)),
    ("Voidwaker", ("middle_hallway_voidwaker",)),
    ("Nightmare / PNM", ("middle_hallway_nightmare",)),
    ("Nightmare", ("middle_hallway_nightmare",)),
    ("Chambers of Xeric", ("middle_hallway_cox",)),
    ("Chambers of Xeric 2", ("final_cox",)),
    ("Gauntlet", ("final_corrupted_gauntlet",)),
    ("Corrupted Gauntlet", ("final_corrupted_gauntlet",)),
    ("Araxxor", ("final_araxxor",)),
    ("Desert Treasure 2", ("final_dt2",)),
    ("DT2", ("final_dt2",)),
    ("Doom of Mokhaiotl", ("final_doom",)),
    ("Doom", ("final_doom",)),
    ("Inferno", ("final_inferno",)),
    ("Yama", ("final_yama",)),
    ("Tormented Demons", ("final_tormented_demons",)),
    ("Zulrah", ("final_zulrah",)),
    ("Maggot King", ("final_maggot_king",)),
    ("Corporeal Beast", ("bonus_corp_beast",)),
    ("Corp Beast", ("bonus_corp_beast",)),
)

TILE_ALIASES: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {normalize_tile_name(label): ids for label, ids in _TILE_ALIAS_ROWS}
)


def resolve_tile_ids(value: object) -> tuple[str, ...]:
    """Resolve a CSV/display tile label to one or more physical board IDs."""

    raw = str(value or "").strip()
    if raw in TILE_RULES:
        return (raw,)
    return TILE_ALIASES.get(normalize_tile_name(raw), ())


def _one_tile_id(value: object) -> str:
    candidates = resolve_tile_ids(value)
    if not candidates:
        raise CompletionRulesError(f"Unknown tile: {value!r}")
    if len(candidates) != 1:
        raise CompletionRulesError(
            f"Ambiguous tile {value!r}; choose a physical ID from {', '.join(candidates)}"
        )
    return candidates[0]


# Public state/evaluation API ------------------------------------------------

def create_empty_state(tile: object) -> TileState:
    """Create an empty aggregate for a canonical ID or unambiguous tile label."""

    return TileState(tile_id=_one_tile_id(tile))


def apply_submission(
    state: TileState,
    item_received: object,
    player_name: object = "",
    blocked_route_ids: Iterable[str] = (),
) -> SubmissionOutcome:
    """Apply one eligible event-log row to ``state``.

    Unknown items, explicit nonqualifiers, items irrelevant to this physical
    tile, and items that can only advance a blocked route do not mutate state.
    Player-based routes (currently Inferno) also require a non-empty player
    name and deduplicate it case-insensitively.
    """

    if state.tile_id not in TILE_RULES:
        raise CompletionRulesError(f"Unknown state tile ID: {state.tile_id!r}")
    alias = normalize_item_name(item_received)
    if alias not in ITEM_ALIASES:
        return SubmissionOutcome(False, None, 0, "unknown item alias")
    item_key = ITEM_ALIASES[alias]
    if item_key is None:
        return SubmissionOutcome(False, None, 0, "known nonqualifying item")

    rule = TILE_RULES[state.tile_id]
    if item_key not in rule.accepted_item_keys:
        return SubmissionOutcome(False, item_key, 0, "item does not advance this tile")

    blocked = {
        str(route_id).strip()
        for route_id in blocked_route_ids
        if str(route_id).strip()
    }
    routes_for_item = tuple(
        route
        for route in rule.routes
        if any(item_key in requirement.item_keys for requirement in route.requirements)
    )
    if routes_for_item and all(
        route.route_id in blocked or f"{state.tile_id}:{route.route_id}" in blocked
        for route in routes_for_item
    ):
        return SubmissionOutcome(False, item_key, 0, "item only advances blocked route")

    amount = ITEM_ALIAS_AMOUNTS.get(alias, 1)
    if item_key in rule.player_item_keys:
        player_key = str(player_name or "").strip().casefold()
        if not player_key:
            return SubmissionOutcome(False, item_key, 0, "player name is required")
        players = state.players_by_item.setdefault(item_key, set())
        before = len(players)
        players.add(player_key)
        if len(players) == before:
            return SubmissionOutcome(False, item_key, 0, "player already counted")

    state.counts[item_key] = state.counts.get(item_key, 0) + amount
    return SubmissionOutcome(True, item_key, amount, "accepted")


def _requirement_current(state: TileState, requirement: RequirementSpec) -> int:
    if requirement.mode == "players":
        players: set[str] = set()
        for item_key in requirement.item_keys:
            players.update(state.players_by_item.get(item_key, set()))
        return len(players)

    if requirement.mode == "distinct":
        return sum(1 for item_key in requirement.item_keys if state.counts.get(item_key, 0) > 0)

    caps = dict(requirement.caps)
    return sum(
        min(state.counts.get(item_key, 0), caps[item_key])
        if item_key in caps
        else state.counts.get(item_key, 0)
        for item_key in requirement.item_keys
    )


def evaluate_completion(
    state: TileState,
    blocked_route_ids: Iterable[str] = (),
) -> CompletionEvaluation:
    """Evaluate all routes, optionally disabling cross-stage-dependent routes."""

    if state.tile_id not in TILE_RULES:
        raise CompletionRulesError(f"Unknown state tile ID: {state.tile_id!r}")
    blocked = {str(route_id).strip() for route_id in blocked_route_ids if str(route_id).strip()}
    route_progress: list[RouteProgress] = []
    for route in TILE_RULES[state.tile_id].routes:
        requirements = tuple(
            RequirementProgress(
                label=requirement.label,
                current=(current := _requirement_current(state, requirement)),
                target=requirement.target,
                satisfied=current >= requirement.target,
            )
            for requirement in route.requirements
        )
        is_blocked = route.route_id in blocked or f"{state.tile_id}:{route.route_id}" in blocked
        route_progress.append(
            RouteProgress(
                route_id=route.route_id,
                label=route.label,
                complete=not is_blocked and all(item.satisfied for item in requirements),
                blocked=is_blocked,
                requirements=requirements,
            )
        )
    routes = tuple(route_progress)
    return CompletionEvaluation(state.tile_id, any(route.complete for route in routes), routes)


def format_route_progress(
    state: TileState,
    blocked_route_ids: Iterable[str] = (),
) -> tuple[str, ...]:
    """Return concise display lines for each completion route."""

    result = evaluate_completion(state, blocked_route_ids)
    lines: list[str] = []
    for route in result.routes:
        status = "blocked" if route.blocked else ("complete" if route.complete else "in progress")
        details = "; ".join(
            f"{requirement.label} {requirement.current}/{requirement.target}"
            for requirement in route.requirements
        )
        lines.append(f"{route.label} [{status}] — {details}")
    return tuple(lines)


__all__ = (
    "CompletionEvaluation",
    "CompletionRulesError",
    "ITEM_ALIASES",
    "RequirementProgress",
    "RouteProgress",
    "SubmissionOutcome",
    "TILE_ALIASES",
    "TILE_RULES",
    "TileRule",
    "TileState",
    "apply_submission",
    "create_empty_state",
    "evaluate_completion",
    "format_route_progress",
    "normalize_item_name",
    "resolve_tile_ids",
)
