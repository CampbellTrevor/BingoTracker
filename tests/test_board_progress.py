import sys
import unittest
from collections import Counter
from pathlib import Path

import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR))

from board_progress import (  # noqa: E402
    BOARD_TILES,
    calculate_team_progress,
    load_tile_rules,
    match_tile_key,
    render_board_html,
)


GRID_ONE_SEQUENCE = [
    "GWD",
    "Venator Shards",
    "Vorkath",
    "DKS",
    "Revs",
    "TOB",
    "Colosseum",
    "Zenytes",
    "Royal Titans",
    "Barrows",
    "Cerberus",
    "The Mad Angel",
]
FINAL_GRID_AFTER_CG = [
    "Araxxor",
    "DT2",
    "TOB",
    "Doom",
    "Inferno",
    "Yama",
    "COX",
    "Moons of Peril",
    "Zulrah",
    "Maggot King",
    "TOA",
]
FULL_SEQUENCE = (
    ["CG", "TOA", "Nex", "Hueycoatl"]
    + GRID_ONE_SEQUENCE
    + ["Voidwaker", "PNM/Nightmare", "COX"]
    + FINAL_GRID_AFTER_CG
    + ["Corp Beast"]
)


def make_rows(events, team="AOCL"):
    rows = []
    for source_row, event in enumerate(events):
        if isinstance(event, str):
            category = event
            item = f"DROP_{source_row}_{category}"
            player = f"Player {source_row % 4 + 1}"
            date = pd.Timestamp("2026-08-29") + pd.Timedelta(minutes=source_row)
        else:
            category = event[0]
            item = event[1]
            player = event[2] if len(event) > 2 else f"Player {source_row % 4 + 1}"
            date = event[3] if len(event) > 3 else pd.Timestamp("2026-08-29")
        rows.append(
            {
                "Date": date,
                "Submission_Order": source_row,
                "Source_Row": source_row,
                "Player": player,
                "Team": team,
                "Category": category,
                "Item": item,
            }
        )
    return rows


class BoardProgressTests(unittest.TestCase):
    def test_board_has_31_slots_and_only_expected_duplicate_labels(self):
        self.assertEqual(len(BOARD_TILES), 31)
        key_counts = Counter(tile.canonical_key for tile in BOARD_TILES)
        self.assertEqual(len(key_counts), 28)
        self.assertEqual(
            {key: count for key, count in key_counts.items() if count > 1},
            {"toa": 2, "tob": 2, "cox": 2},
        )

    def test_aliases_are_conservative(self):
        self.assertEqual(match_tile_key("Tombs of Amascut"), "toa")
        self.assertEqual(match_tile_key("CG"), "corrupted_gauntlet")
        self.assertEqual(match_tile_key("Nightmare / PNM"), "pnm_nightmare")
        self.assertIsNone(match_tile_key("Barrows / Moons"))
        self.assertIsNone(match_tile_key("Colo / Inferno"))

    def test_opening_hallway_is_ordered_and_early_rows_are_not_banked(self):
        events = [
            ("Nex", "EARLY_NEX"),
            ("TOA", "START_TOA"),
            ("Hueycoatl", "EARLY_HUEY"),
            ("Nex", "VALID_NEX"),
            ("Hueycoatl", "VALID_HUEY"),
        ]
        progress = calculate_team_progress(pd.DataFrame(make_rows(events)), "AOCL")

        self.assertEqual(progress["completed_count"], 3)
        self.assertEqual(progress["ignored_locked_count"], 2)
        self.assertEqual(progress["current_section"], "First grid")
        self.assertEqual(progress["states"]["start_toa"]["completion"]["Item"], "START_TOA")
        self.assertEqual(progress["states"]["start_nex"]["completion"]["Item"], "VALID_NEX")
        self.assertEqual(progress["states"]["start_hueycoatl"]["completion"]["Item"], "VALID_HUEY")
        self.assertTrue(
            all(progress["states"][tile.tile_id]["status"] == "available" for tile in BOARD_TILES if tile.stage == "grid_one")
        )

    def test_first_grid_is_any_order_and_unlocks_only_voidwaker(self):
        events = ["TOA", "Nex", "Hueycoatl"] + list(reversed(GRID_ONE_SEQUENCE))
        progress = calculate_team_progress(pd.DataFrame(make_rows(events)), "AOCL")

        self.assertEqual(progress["completed_count"], 15)
        self.assertEqual(progress["current_section"], "Second hallway")
        self.assertEqual(progress["states"]["path_voidwaker"]["status"], "available")
        self.assertEqual(progress["states"]["path_pnm_nightmare"]["status"], "locked")
        self.assertEqual(progress["states"]["path_cox"]["status"], "locked")

    def test_cg_completes_at_start_without_unlocking_the_final_grid(self):
        events = [
            ("CG", "OPENING_CHEST"),
            ("Araxxor", "EARLY_ARAXXOR"),
            ("TOA", "START_TOA"),
        ]
        progress = calculate_team_progress(pd.DataFrame(make_rows(events)), "AOCL")

        self.assertEqual(progress["completed_count"], 2)
        self.assertTrue(progress["cg_complete"])
        self.assertEqual(progress["states"]["final_corrupted_gauntlet"]["status"], "complete")
        self.assertEqual(progress["states"]["final_araxxor"]["status"], "locked")
        self.assertEqual(len(progress["states"]["final_araxxor"]["locked_attempts"]), 1)
        self.assertEqual(progress["states"]["start_nex"]["status"], "available")

    def test_duplicate_labels_are_assigned_to_distinct_physical_slots(self):
        progress = calculate_team_progress(pd.DataFrame(make_rows(FULL_SEQUENCE)), "AOCL")
        states = progress["states"]

        self.assertIn("DROP_1_TOA", states["start_toa"]["completion"]["Item"])
        self.assertIn("DROP_29_TOA", states["final_toa"]["completion"]["Item"])
        self.assertIn("DROP_9_TOB", states["grid_tob"]["completion"]["Item"])
        self.assertIn("DROP_21_TOB", states["final_tob"]["completion"]["Item"])
        self.assertIn("DROP_18_COX", states["path_cox"]["completion"]["Item"])
        self.assertIn("DROP_25_COX", states["final_cox"]["completion"]["Item"])

    def test_full_sequence_completes_all_31_slots(self):
        progress = calculate_team_progress(pd.DataFrame(make_rows(FULL_SEQUENCE)), "AOCL")

        self.assertEqual(len(FULL_SEQUENCE), 31)
        self.assertEqual(progress["completed_count"], 31)
        self.assertEqual(len(progress["accepted"]), 31)
        self.assertEqual(progress["ignored_locked_count"], 0)
        self.assertEqual(progress["unmatched_count"], 0)
        self.assertEqual(progress["current_section"], "Board complete")
        self.assertEqual(progress["available_count"], 0)
        self.assertTrue(all(state["status"] == "complete" for state in progress["states"].values()))

    def test_submission_order_not_date_controls_unlocks(self):
        events = [
            ("Nex", "EARLY_NEX", "Early", pd.NaT),
            ("TOA", "VALID_TOA", "Later", pd.Timestamp("2026-08-01")),
        ]
        progress = calculate_team_progress(pd.DataFrame(make_rows(events)), "AOCL")

        self.assertEqual(progress["completed_count"], 1)
        self.assertEqual(progress["ignored_locked_count"], 1)
        self.assertEqual(progress["states"]["start_toa"]["completion"]["Item"], "VALID_TOA")
        self.assertEqual(progress["states"]["start_nex"]["status"], "available")

    def test_teams_are_isolated(self):
        rows = make_rows(["TOA", "Nex", "Hueycoatl"], team="AOCL")
        rows += make_rows(["Nex"], team="MASELF")
        df = pd.DataFrame(rows)

        aocl = calculate_team_progress(df, "AOCL")
        maself = calculate_team_progress(df, "MASELF")

        self.assertEqual(aocl["completed_count"], 3)
        self.assertEqual(maself["completed_count"], 0)
        self.assertEqual(maself["ignored_locked_count"], 1)
        self.assertEqual(maself["states"]["start_toa"]["status"], "available")

    def test_render_uses_progress_states_and_escapes_submission_html(self):
        team = "<img src=x onerror=alert(1)>"
        events = [
            ("CG", "Armour seed & pet", "<script>alert(1)</script>"),
            ("Nex", "Early <svg onload=alert(2)>", "Early Player"),
        ]
        progress = calculate_team_progress(pd.DataFrame(make_rows(events, team=team)), team)
        rules = load_tile_rules(PROJECT_DIR / "tile_rules.json")
        self.assertEqual(rules["start_toa"]["completion"]["required"], 1)

        markup = render_board_html(
            PROJECT_DIR / "assets" / "bingoboard.png",
            progress,
            team,
        )

        self.assertEqual(markup.count('data-tile-id="'), 31)
        self.assertIn(
            'data-tile-id="final_corrupted_gauntlet" data-status="complete"',
            markup,
        )
        self.assertIn('data-tile-id="start_toa" data-status="available"', markup)
        self.assertIn('data-tile-id="start_nex" data-status="locked"', markup)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", markup)
        self.assertIn("Armour seed &amp; pet", markup)
        self.assertIn("&lt;img src=x onerror=alert(1)&gt;", markup)
        self.assertNotIn("<script>alert(1)</script>", markup)
        self.assertNotIn("<svg onload=alert(2)>", markup)


if __name__ == "__main__":
    unittest.main()
