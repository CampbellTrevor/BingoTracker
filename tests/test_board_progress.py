import sys
import unittest
from collections import Counter
from pathlib import Path

import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR))

from board_progress import (  # noqa: E402
    BOARD_TILES,
    index_team_submissions,
    load_tile_rules,
    match_tile_key,
    render_board_html,
)


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

    def test_team_index_preserves_matches_and_surfaces_unknowns(self):
        df = pd.DataFrame(
            [
                {
                    "Date": pd.Timestamp("2026-08-29 12:00"),
                    "Submission_Order": 2,
                    "Player": "Later Player",
                    "Team": "AOCL",
                    "Category": "CG",
                    "Item": "Later drop",
                },
                {
                    "Date": pd.Timestamp("2026-08-29 11:00"),
                    "Submission_Order": 1,
                    "Player": "Earlier Player",
                    "Team": "AOCL",
                    "Category": "Corrupted Gauntlet",
                    "Item": "Earlier drop",
                },
                {
                    "Date": pd.Timestamp("2026-08-29 13:00"),
                    "Submission_Order": 3,
                    "Player": "Mystery Player",
                    "Team": "AOCL",
                    "Category": "Unknown Future Tile",
                    "Item": "Mystery drop",
                },
                {
                    "Date": pd.Timestamp("2026-08-29 10:00"),
                    "Submission_Order": 1,
                    "Player": "Other Team",
                    "Team": "MASELF",
                    "Category": "CG",
                    "Item": "Other drop",
                },
            ]
        )

        grouped, unmatched = index_team_submissions(df, "AOCL")

        self.assertEqual(
            [row["Player"] for row in grouped["corrupted_gauntlet"]],
            ["Earlier Player", "Later Player"],
        )
        self.assertEqual(unmatched["Category"].tolist(), ["Unknown Future Tile"])

    def test_render_is_rules_pending_and_escapes_submission_html(self):
        rules = load_tile_rules(PROJECT_DIR / "tile_rules.json")
        grouped = {
            "corrupted_gauntlet": [
                {
                    "Player": "<script>alert(1)</script>",
                    "Item": "Armour seed & pet",
                    "Date": pd.Timestamp("2026-08-29 14:30"),
                }
            ],
            "toa": [
                {
                    "Player": "Shared Player",
                    "Item": "Shared drop",
                    "Date": pd.Timestamp("2026-08-29 15:00"),
                }
            ],
        }

        markup = render_board_html(
            PROJECT_DIR / "assets" / "bingoboard.png",
            grouped,
            rules,
            "AOCL",
        )

        self.assertEqual(markup.count('data-tile-id="'), 31)
        self.assertIn(
            'data-tile-id="final_corrupted_gauntlet" '
            'data-status="submitted-rules-pending" data-submission-count="1"',
            markup,
        )
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", markup)
        self.assertNotIn("<script>alert(1)</script>", markup)
        self.assertIn("Armour seed &amp; pet", markup)
        self.assertEqual(markup.count('data-status="shared-submissions-rules-pending"'), 2)
        self.assertNotIn("state-complete", markup)


if __name__ == "__main__":
    unittest.main()
