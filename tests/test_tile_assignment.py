import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR))

from tile_assignment import (  # noqa: E402
    FIDDLSTCKS_ROSTER,
    GRID_TILE_SPECS,
    PlayerStats,
    RosterMember,
    build_balanced_assignments,
    calculate_combat_level,
    parse_bulk_hiscores,
    rankings_by_tile,
    score_grid_tiles,
)


COMBAT_SKILLS = {
    "attack": 99,
    "strength": 99,
    "defence": 99,
    "hitpoints": 99,
    "ranged": 99,
    "magic": 99,
    "prayer": 99,
    "slayer": 99,
}


def all_boss_keys():
    return {
        metric
        for tile in GRID_TILE_SPECS
        for component in tile.components
        if component.kind in {"boss", "achievement"}
        for metric in component.metrics
    }


def make_player(name, *, ehb=100, boss_overrides=None, missing_bosses=(), available=True):
    bosses = {metric: 0.0 for metric in all_boss_keys()}
    bosses.update(boss_overrides or {})
    for metric in missing_bosses:
        bosses.pop(metric, None)
    skills = dict(COMBAT_SKILLS) if available else {}
    return PlayerStats(
        draft_name=name,
        wom_name=name,
        matched_name=name if available else None,
        player_id=1 if available else None,
        updated_at="2026-08-29T12:00:00.000Z" if available else None,
        available=available,
        ehb=float(ehb) if available else 0.0,
        combat_level=calculate_combat_level(skills),
        skills=skills,
        bosses=bosses if available else {},
        activities={"colosseum_glory": 0.0} if available else {},
    )


class TileAssignmentTests(unittest.TestCase):
    def test_roster_and_grid_slot_configuration_are_complete(self):
        self.assertEqual(len(FIDDLSTCKS_ROSTER), 20)
        self.assertEqual(len({member.draft_name for member in FIDDLSTCKS_ROSTER}), 20)
        self.assertIn("FIDDLSTCKS", {member.draft_name for member in FIDDLSTCKS_ROSTER})
        self.assertIn("MELDYNOIR", {member.draft_name for member in FIDDLSTCKS_ROSTER})
        self.assertIn("GIM RETRO J", {member.draft_name for member in FIDDLSTCKS_ROSTER})
        self.assertEqual(len(GRID_TILE_SPECS), 24)
        self.assertTrue(
            all(abs(sum(component.weight for component in tile.components) - 1.0) < 1e-9 for tile in GRID_TILE_SPECS)
        )

    def test_bulk_parser_matches_configured_alias_and_derives_combat(self):
        payload = [
            {
                "player": {
                    "id": 2780967,
                    "username": "fiddlestcks",
                    "displayName": "Fiddlestcks",
                    "ehb": 248.62,
                    "updatedAt": "2026-08-29T04:25:18.504Z",
                },
                "data": {
                    "data": {
                        "skills": {
                            key: {"level": value} for key, value in COMBAT_SKILLS.items()
                        },
                        "bosses": {"vorkath": {"kills": 42}},
                        "activities": {"colosseum_glory": {"score": 1000}},
                    }
                },
            }
        ]
        players = parse_bulk_hiscores(
            payload,
            roster=(RosterMember("FIDDLSTCKS", "Fiddlestcks"),),
        )

        self.assertEqual(len(players), 1)
        self.assertTrue(players[0].available)
        self.assertEqual(players[0].matched_name, "Fiddlestcks")
        self.assertEqual(players[0].combat_level, 126)
        self.assertEqual(players[0].bosses["vorkath"], 42)

    def test_direct_kc_drives_tile_ranking_and_raw_values_remain_visible(self):
        players = [
            make_player("Experienced", boss_overrides={"vorkath": 500}),
            make_player("No KC", boss_overrides={"vorkath": 0}),
        ]
        records = score_grid_tiles(players, now=datetime(2026, 8, 29, tzinfo=timezone.utc))
        rankings = rankings_by_tile(records)["grid_vorkath"]

        self.assertEqual(rankings[0].player, "Experienced")
        self.assertGreater(rankings[0].score, rankings[1].score)
        self.assertIn("Vorkath KC 500", rankings[0].evidence)
        self.assertEqual(rankings[0].confidence, "High")

    def test_proxy_tiles_are_always_low_confidence(self):
        records = score_grid_tiles(
            [make_player("Player")],
            now=datetime(2026, 8, 29, tzinfo=timezone.utc),
        )
        rankings = rankings_by_tile(records)

        self.assertEqual(rankings["grid_revs"][0].confidence, "Low")
        self.assertEqual(rankings["grid_zenytes"][0].confidence, "Low")

    def test_missing_high_weight_direct_metric_caps_confidence_low(self):
        records = score_grid_tiles(
            [make_player("Player", missing_bosses=("vorkath",))],
            now=datetime(2026, 8, 29, tzinfo=timezone.utc),
        )
        ranking = rankings_by_tile(records)["grid_vorkath"][0]

        self.assertEqual(ranking.confidence, "Low")
        self.assertIn("Vorkath KC: missing", ranking.evidence)

    def test_unmatched_profile_is_unscored_and_excluded_from_assignment(self):
        players = [make_player("Matched"), make_player("Missing", available=False)]
        records = score_grid_tiles(players, now=datetime(2026, 8, 29, tzinfo=timezone.utc))
        missing_rows = [record for record in records if record.player == "Missing"]
        assignments, _workload = build_balanced_assignments(records)

        self.assertTrue(all(record.score is None for record in missing_rows))
        self.assertTrue(all(record.confidence == "Unavailable" for record in missing_rows))
        self.assertNotIn("Missing", {row["Primary"] for row in assignments})

    def test_assignment_covers_all_slots_balances_load_and_splits_tob(self):
        players = [make_player(f"Player {index:02d}") for index in range(20)]
        records = score_grid_tiles(players, now=datetime(2026, 8, 29, tzinfo=timezone.utc))
        assignments, workload = build_balanced_assignments(records)

        self.assertEqual(len(assignments), 24)
        self.assertLessEqual(max(row["Primary Tiles"] for row in workload), 3)
        self.assertGreaterEqual(len({row["Primary"] for row in assignments}), 18)
        tob_owners = [row["Primary"] for row in assignments if row["Tile"] == "TOB"]
        self.assertEqual(len(tob_owners), 2)
        self.assertEqual(len(set(tob_owners)), 2)
        for raid in (row for row in assignments if row["Recommended Squad"]):
            self.assertIn(raid["Primary"], raid["Recommended Squad"].split(", "))
            self.assertIn("Backup Confidence", raid)
        cg = next(row for row in assignments if row["Tile"].startswith("CORRUPTED GAUNTLET"))
        self.assertIn("OPEN FROM START", cg["Tile"])


if __name__ == "__main__":
    unittest.main()
