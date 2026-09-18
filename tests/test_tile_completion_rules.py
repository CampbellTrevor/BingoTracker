import csv
import sys
import unittest
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR))

from tile_completion_rules import (  # noqa: E402
    CompletionRulesError,
    ITEM_ALIASES,
    TILE_RULES,
    apply_submission,
    create_empty_state,
    evaluate_completion,
    format_route_progress,
    normalize_item_name,
    resolve_tile_ids,
)


EXPECTED_TILE_IDS = {
    "opening_hallway_toa",
    "opening_hallway_nex",
    "opening_hallway_hueycoatl",
    "grid_gwd",
    "grid_venator_shards",
    "grid_vorkath",
    "grid_dks",
    "grid_revs",
    "grid_tob",
    "grid_colosseum",
    "grid_zenytes",
    "grid_royal_titans",
    "grid_barrows",
    "grid_cerberus",
    "grid_the_mad_angel",
    "middle_hallway_voidwaker",
    "middle_hallway_nightmare",
    "middle_hallway_cox",
    "final_corrupted_gauntlet",
    "final_araxxor",
    "final_dt2",
    "final_tob",
    "final_doom",
    "final_inferno",
    "final_yama",
    "final_cox",
    "final_tormented_demons",
    "final_zulrah",
    "final_maggot_king",
    "final_toa",
    "bonus_corp_beast",
}


class TileCompletionRulesTests(unittest.TestCase):
    def test_catalog_contains_30_race_tiles_and_corp(self):
        self.assertEqual(set(TILE_RULES), EXPECTED_TILE_IDS)
        self.assertEqual(len(TILE_RULES), 31)
        self.assertTrue(all(rule.routes for rule in TILE_RULES.values()))

    def test_every_live_csv_item_has_a_known_alias(self):
        with (PROJECT_DIR / "Summer Bingo 2026 Event Log.csv").open(
            encoding="utf-8-sig", newline=""
        ) as handle:
            item_names = {row["Item Received"] for row in csv.DictReader(handle)}

        missing = sorted(
            item for item in item_names if normalize_item_name(item) not in ITEM_ALIASES
        )
        self.assertEqual(missing, [])

        accepted_keys = {
            item_key
            for rule in TILE_RULES.values()
            for item_key in rule.accepted_item_keys
        }
        alias_keys = {item_key for item_key in ITEM_ALIASES.values() if item_key}
        self.assertEqual(accepted_keys - alias_keys, set())

    def test_tile_aliases_preserve_physical_duplicates(self):
        self.assertEqual(resolve_tile_ids("Chambers of Xeric"), ("middle_hallway_cox",))
        self.assertEqual(resolve_tile_ids("Chambers of Xeric 2"), ("final_cox",))
        self.assertEqual(resolve_tile_ids("Tombs of Amascut"), ("opening_hallway_toa",))
        self.assertEqual(resolve_tile_ids("Tombs of Amascut 2"), ("final_toa",))
        self.assertEqual(resolve_tile_ids("Theatre of Blood"), ("grid_tob", "final_tob"))
        with self.assertRaises(CompletionRulesError):
            create_empty_state("Theatre of Blood")

    def test_gwd_requires_a_godsword_and_credit_from_every_general(self):
        state = create_empty_state("grid_gwd")
        for item in (
            "Godsword Shard 1",
            "Godsword Shard 2",
            "Godsword Shard 3",
            "Armadyl Hilt",  # also supplies Kree'arra credit
            "Bandos Chestplate",
            "Saradomin Sword",
            "Zamorakian Spear",
        ):
            self.assertTrue(apply_submission(state, item).accepted)
        self.assertTrue(evaluate_completion(state).complete)

        without_hilt = create_empty_state("grid_gwd")
        for item in (
            "Godsword Shard 1",
            "Godsword Shard 2",
            "Godsword Shard 3",
            "Armadyl Chestplate",
            "Bandos Chestplate",
            "Saradomin Sword",
            "Zamorakian Spear",
        ):
            apply_submission(without_hilt, item)
        self.assertFalse(evaluate_completion(without_hilt).complete)
        self.assertFalse(apply_submission(without_hilt, "Ancient Hilt").accepted)

    def test_occurrences_and_distinct_items_follow_different_rules(self):
        revs = create_empty_state("grid_revs")
        for item in ("Ancient Emblem", "Ancient Emblem", "Ancient Emblem"):
            apply_submission(revs, item)
        self.assertFalse(evaluate_completion(revs).complete)
        apply_submission(revs, "Craw's Bow")
        self.assertTrue(evaluate_completion(revs).complete)

        barrows = create_empty_state("grid_barrows")
        for _ in range(4):
            apply_submission(barrows, "Ahrim's Hood")
        self.assertFalse(evaluate_completion(barrows).complete)
        for item in ("Ahrim's Robetop", "Ahrim's Robeskirt", "Ahrim's Staff"):
            apply_submission(barrows, item)
        self.assertTrue(evaluate_completion(barrows).complete)

    def test_synthetic_vorkath_kill_count_submission_adds_300(self):
        state = create_empty_state("grid_vorkath")
        outcome = apply_submission(state, "300 Kill Count", "Player One")
        self.assertTrue(outcome.accepted)
        self.assertEqual(outcome.amount, 300)
        self.assertTrue(evaluate_completion(state).complete)

    def test_hallway_goals_support_duplicates_but_voidwaker_is_distinct(self):
        toa = create_empty_state("opening_hallway_toa")
        apply_submission(toa, "Osmumten's Fang")
        apply_submission(toa, "Osmumten's Fang")
        self.assertTrue(evaluate_completion(toa).complete)

        voidwaker = create_empty_state("middle_hallway_voidwaker")
        for _ in range(3):
            apply_submission(voidwaker, "Voidwaker Hilt")
        self.assertFalse(evaluate_completion(voidwaker).complete)
        apply_submission(voidwaker, "Voidwaker Gem")
        apply_submission(voidwaker, "Voidwaker Blade")
        self.assertTrue(evaluate_completion(voidwaker).complete)

        nightmare = create_empty_state("middle_hallway_nightmare")
        apply_submission(nightmare, "Jar of Dreams")
        apply_submission(nightmare, "Inquisitor's Great Helm")
        self.assertTrue(evaluate_completion(nightmare).complete)

    def test_inferno_requires_three_distinct_players(self):
        state = create_empty_state("final_inferno")
        self.assertFalse(apply_submission(state, "Infernal Cape").accepted)
        self.assertTrue(apply_submission(state, "Infernal Cape", "Ace").accepted)
        self.assertFalse(apply_submission(state, "Infernal Cape", "ace").accepted)
        apply_submission(state, "Infernal Cape", "Bee")
        self.assertFalse(evaluate_completion(state).complete)
        apply_submission(state, "Infernal Cape", "Cee")
        self.assertTrue(evaluate_completion(state).complete)

    def test_known_nonqualifiers_are_recognized_but_never_advance(self):
        yama = create_empty_state("final_yama")
        for _ in range(7):
            result = apply_submission(yama, "Soulflame Horn")
            self.assertFalse(result.accepted)
            self.assertEqual(result.reason, "known nonqualifying item")
        self.assertFalse(evaluate_completion(yama).complete)

        cox = create_empty_state("middle_hallway_cox")
        self.assertFalse(
            apply_submission(cox, "Twisted Ancestral colour kit").accepted
        )
        self.assertFalse(evaluate_completion(cox).complete)

    def test_yami_is_capped_at_one_occurrence(self):
        state = create_empty_state("final_yama")
        for _ in range(3):
            apply_submission(state, "Yami")
        self.assertFalse(evaluate_completion(state).complete)
        apply_submission(state, "Oathplate Chest")
        self.assertFalse(evaluate_completion(state).complete)
        apply_submission(state, "Oathplate Chest")
        self.assertTrue(evaluate_completion(state).complete)

    def test_blocked_route_cannot_complete_but_other_routes_can(self):
        rejected = create_empty_state("final_cox")
        outcome = apply_submission(
            rejected,
            "Twisted Bow",
            blocked_route_ids={"twisted_bow"},
        )
        self.assertFalse(outcome.accepted)
        self.assertEqual(outcome.reason, "item only advances blocked route")
        self.assertFalse(evaluate_completion(rejected).complete)

        state = create_empty_state("final_cox")
        apply_submission(state, "Twisted Bow")
        self.assertTrue(evaluate_completion(state).complete)

        blocked = evaluate_completion(state, {"twisted_bow"})
        self.assertFalse(blocked.complete)
        bow_route = next(route for route in blocked.routes if route.route_id == "twisted_bow")
        self.assertTrue(bow_route.blocked)
        self.assertFalse(bow_route.complete)
        self.assertIn("[blocked]", "\n".join(format_route_progress(state, {"twisted_bow"})))

        for item in ("Ancestral Hat", "Ancestral Robe Top", "Ancestral Robe Bottom"):
            apply_submission(state, item)
        self.assertTrue(evaluate_completion(state, {"twisted_bow"}).complete)

    def test_corp_requires_shield_elixir_and_any_sigil(self):
        state = create_empty_state("bonus_corp_beast")
        apply_submission(state, "Spirit Shield")
        apply_submission(state, "Holy Elixir")
        self.assertFalse(evaluate_completion(state).complete)
        apply_submission(state, "Elysian Sigil")
        self.assertTrue(evaluate_completion(state).complete)


if __name__ == "__main__":
    unittest.main()
