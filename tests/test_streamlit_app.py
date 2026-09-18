import csv
import io
import sys
import unittest
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parents[1]
DEPENDENCY_CANDIDATES = (
    Path.home() / "bingo_stats_test_deps",
    Path.home() / "AppData" / "Local" / "Temp" / "bingo-stats-test-deps",
)
for dependency_path in DEPENDENCY_CANDIDATES:
    if (dependency_path / "streamlit" / "testing" / "v1" / "__init__.py").exists():
        sys.path.insert(0, str(dependency_path))
        break
sys.path.insert(0, str(PROJECT_DIR))

from streamlit.testing.v1 import AppTest  # noqa: E402


def board_markup(app):
    return next(
        markdown.value
        for markdown in app.markdown
        if 'data-tile-id="start_toa"' in markdown.value
    )


def metric(app, label):
    return next(item for item in app.metric if item.label == label)


def example_csv_through(entry_number, team="FIDDLSTCKS"):
    source = (PROJECT_DIR / "example_tile_race.csv").read_text(encoding="utf-8")
    reader = csv.DictReader(io.StringIO(source))
    rows = [
        row
        for row in reader
        if row["Team"] == team and int(row["Entry #"]) <= entry_number
    ]
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=reader.fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


class StreamlitAppTests(unittest.TestCase):
    def test_default_data_renders_authoritative_race_and_bonus_metrics(self):
        app = AppTest.from_file(
            str(PROJECT_DIR / "bingostats.py"), default_timeout=30
        ).run()

        self.assertEqual(len(app.exception), 0)
        self.assertEqual(app.metric[0].label, "Total Drops")
        self.assertEqual(app.metric[0].value, "545")
        self.assertEqual(app.metric[1].label, "Completed Race Tiles")
        self.assertEqual(app.metric[1].value, "125")
        self.assertEqual(
            list(app.selectbox(key="board_team").options),
            [
                "Team 1 - Fiddlestcks Flight Attendants",
                "Team 2 - The Bapstreet Boys",
                "Team 3 - Gnome Ball Ballers Kittens uwu",
                "Team 4 - Team Lizard",
                "Team 5 - The Rextards",
            ],
        )
        self.assertEqual(metric(app, "Race Tiles").value, "27/30")
        self.assertEqual(metric(app, "Corp Bonus").value, "Not complete")
        self.assertFalse(
            any(item.label == "Ignored While Locked" for item in app.metric)
        )
        self.assertEqual(metric(app, "Nonqualifying").value, "0")
        self.assertEqual(metric(app, "Unmatched Submissions").value, "0")
        self.assertNotEqual(metric(app, "WoM KC (Event)").value, "No WoM Data")
        self.assertFalse(
            any(
                expander.label == "Wise Old Man cache notes"
                for expander in app.expander
            )
        )
        self.assertTrue(app.tabs[0].label.endswith("Board Progress"))
        self.assertTrue(
            any(
                expander.label == "Completion rules (30 race tiles + Corp bonus)"
                for expander in app.expander
            )
        )

        markup = board_markup(app)
        self.assertEqual(markup.count('data-tile-id="'), 31)
        self.assertIn("TORMENTED DEMONS", markup)
        self.assertIn(
            'data-tile-id="final_yama" data-status="complete" '
            'data-submission-count="3" data-nonqualifying-count="0"',
            markup,
        )
        self.assertIn("Soulflame Horn", markup)

    def test_uploaded_csv_completes_starting_cg_and_reports_diagnostics(self):
        csv_text = """Entry #,Date,Player Name,Team,Tile,Item Received
1,29/08/2026 14:30,<script>alert(1)</script>,AOCL,Gauntlet,Enhanced Crystal Weapon Seed
2,29/08/2026 14:31,Rule Tester,AOCL,Tombs of Amascut,Soulflame Horn
3,29/08/2026 14:32,Unknown Tester,AOCL,Mystery Tile,Mystery Drop
4,29/08/2026 14:33,Second Player,MASELF,Nex,Nihil Horn
"""
        app = AppTest.from_file(
            str(PROJECT_DIR / "bingostats.py"), default_timeout=30
        ).run()
        app.file_uploader[0].set_value(
            ("summer.csv", csv_text.encode("utf-8"), "text/csv")
        ).run()

        self.assertEqual(len(app.exception), 0)
        self.assertEqual(app.selectbox(key="board_team").value, "AOCL")
        self.assertEqual(metric(app, "Race Tiles").value, "1/30")
        self.assertEqual(metric(app, "Corp Bonus").value, "Not complete")
        self.assertEqual(metric(app, "Nonqualifying").value, "1")
        self.assertEqual(metric(app, "Unmatched Submissions").value, "1")
        self.assertFalse(
            any(item.label == "Ignored While Locked" for item in app.metric)
        )

        markup = board_markup(app)
        self.assertIn(
            'data-tile-id="final_corrupted_gauntlet" '
            'data-status="complete" data-submission-count="1" '
            'data-nonqualifying-count="0"',
            markup,
        )
        self.assertIn(
            'data-tile-id="start_toa" data-status="available" '
            'data-submission-count="0" data-nonqualifying-count="1"',
            markup,
        )
        self.assertIn('data-tile-id="final_araxxor" data-status="locked"', markup)
        self.assertIn("Nonqualifying submissions (1)", markup)
        self.assertIn("Soulflame Horn", markup)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", markup)
        self.assertNotIn("<script>alert(1)</script>", markup)
        self.assertTrue(
            any(
                expander.label == "Submission diagnostics"
                for expander in app.expander
            )
        )

    def test_partial_entry_numbers_use_source_order_without_rejecting_early_rows(self):
        csv_text = """Entry #,Date,Player Name,Team,Tile,Item Received
100,29/08/2026 10:00,Early Nex,AOCL,Nex,Nihil Horn
,29/08/2026 10:01,Starter One,AOCL,Tombs of Amascut,Lightbearer
101,29/08/2026 10:02,Starter Two,AOCL,Tombs of Amascut,Masori Mask
102,29/08/2026 10:03,Valid Nex One,AOCL,Nex,Nihil Horn
103,29/08/2026 10:04,Valid Nex Two,AOCL,Nex,Torva Platebody
"""
        app = AppTest.from_file(
            str(PROJECT_DIR / "bingostats.py"), default_timeout=30
        ).run()
        app.file_uploader[0].set_value(
            ("partial_entries.csv", csv_text.encode("utf-8"), "text/csv")
        ).run()

        self.assertEqual(len(app.exception), 0)
        self.assertEqual(metric(app, "Race Tiles").value, "2/30")
        self.assertEqual(metric(app, "Nonqualifying").value, "0")
        self.assertFalse(
            any(item.label == "Ignored While Locked" for item in app.metric)
        )
        markup = board_markup(app)
        self.assertIn(
            'data-tile-id="start_toa" data-status="complete" '
            'data-submission-count="2" data-nonqualifying-count="0"',
            markup,
        )
        self.assertIn(
            'data-tile-id="start_nex" data-status="complete" '
            'data-submission-count="2" data-nonqualifying-count="0" '
            'data-extra-count="1"',
            markup,
        )
        self.assertIn(
            'data-tile-id="start_hueycoatl" data-status="available"', markup
        )
        self.assertIn("Early Nex", markup)
        self.assertIn("Valid Nex One", markup)
        self.assertIn("Valid Nex Two", markup)
        self.assertIn("Additional submissions (no extra progress)", markup)
        self.assertNotIn("data-ignored-count", markup)
        self.assertTrue(
            any(
                expander.label == "Submission diagnostics"
                for expander in app.expander
            )
        )
        self.assertEqual(metric(app, "Additional After Completion").value, "1")

    def test_corp_unlocks_with_final_grid_without_showing_empty_diagnostics(self):
        app = AppTest.from_file(
            str(PROJECT_DIR / "bingostats.py"), default_timeout=30
        ).run()
        app.file_uploader[0].set_value(
            (
                "through_second_hallway.csv",
                example_csv_through(59),
                "text/csv",
            )
        ).run()

        self.assertEqual(len(app.exception), 0)
        self.assertEqual(app.selectbox(key="board_team").value, "FIDDLSTCKS")
        self.assertEqual(metric(app, "Race Tiles").value, "19/30")
        self.assertEqual(metric(app, "Corp Bonus").value, "Not complete")
        markup = board_markup(app)
        self.assertIn(
            'data-tile-id="bonus_corp_beast" data-status="available"',
            markup,
        )
        self.assertFalse(
            any(
                expander.label == "Submission diagnostics"
                for expander in app.expander
            )
        )

    def test_player_rankings_show_board_contributions_not_item_filters(self):
        csv_text = """Entry #,Date,Player Name,Team,Tile,Item Received
1,29/08/2026 10:00,Alice,AOCL,Tombs of Amascut,Lightbearer
2,29/08/2026 10:01,Bob,AOCL,Tombs of Amascut,Masori Mask
3,29/08/2026 10:02,Alice,AOCL,Gauntlet,Enhanced Crystal Weapon Seed
4,29/08/2026 10:03,Alice,AOCL,Mystery Tile,Mystery Drop
5,29/08/2026 10:04,Cara,AOCL,Corporeal Beast,Spirit Shield
6,29/08/2026 10:05,Cara,AOCL,Corporeal Beast,Holy Elixir
7,29/08/2026 10:06,Cara,AOCL,Corporeal Beast,Arcane Sigil
8,29/08/2026 10:07,Dave,AOCL,Mystery Tile,Another Mystery Drop
"""
        app = AppTest.from_file(
            str(PROJECT_DIR / "bingostats.py"), default_timeout=30
        ).run()
        app.file_uploader[0].set_value(
            ("contributions.csv", csv_text.encode("utf-8"), "text/csv")
        ).run()

        self.assertEqual(len(app.exception), 0)
        contribution_columns = [
            "Rank",
            "Player",
            "Team",
            "Race Tiles Finished",
            "Corp Tile Finished",
            "Qualifying Drops",
            "Total Submissions",
        ]
        contribution_table = next(
            dataframe.value
            for dataframe in app.dataframe
            if list(dataframe.value.columns) == contribution_columns
        )
        self.assertEqual(
            contribution_table.to_dict("records"),
            [
                {
                    "Rank": 1,
                    "Player": "Alice",
                    "Team": "AOCL",
                    "Race Tiles Finished": 1,
                    "Corp Tile Finished": 0,
                    "Qualifying Drops": 2,
                    "Total Submissions": 3,
                },
                {
                    "Rank": 2,
                    "Player": "Bob",
                    "Team": "AOCL",
                    "Race Tiles Finished": 1,
                    "Corp Tile Finished": 0,
                    "Qualifying Drops": 1,
                    "Total Submissions": 1,
                },
                {
                    "Rank": 3,
                    "Player": "Cara",
                    "Team": "AOCL",
                    "Race Tiles Finished": 0,
                    "Corp Tile Finished": 1,
                    "Qualifying Drops": 3,
                    "Total Submissions": 3,
                },
                {
                    "Rank": 4,
                    "Player": "Dave",
                    "Team": "AOCL",
                    "Race Tiles Finished": 0,
                    "Corp Tile Finished": 0,
                    "Qualifying Drops": 0,
                    "Total Submissions": 1,
                },
            ],
        )
        self.assertIn(
            "Player Contributions",
            [subheader.value for subheader in app.subheader],
        )
        selectbox_keys = {getattr(item, "key", None) for item in app.selectbox}
        self.assertNotIn("rank_category", selectbox_keys)
        self.assertNotIn("rank_item", selectbox_keys)
        self.assertNotIn(
            "Top Players by Category",
            [subheader.value for subheader in app.subheader],
        )
        self.assertNotIn(
            "Top Players by Item",
            [subheader.value for subheader in app.subheader],
        )

        player_history = next(
            dataframe.value
            for dataframe in app.dataframe
            if list(dataframe.value.columns) == ["Date", "Category", "Item"]
        )
        self.assertEqual(
            player_history["Date"].tolist(),
            [
                "29 Aug 2026, 10:03",
                "29 Aug 2026, 10:02",
                "29 Aug 2026, 10:00",
            ],
        )

    def test_example_csv_renders_complete_race_bonus_and_partial_teams(self):
        app = AppTest.from_file(
            str(PROJECT_DIR / "bingostats.py"), default_timeout=30
        ).run()
        sample_bytes = (PROJECT_DIR / "example_tile_race.csv").read_bytes()
        app.file_uploader[0].set_value(
            ("example_tile_race.csv", sample_bytes, "text/csv")
        ).run()
        app.selectbox(key="board_team").set_value("FIDDLSTCKS").run()

        self.assertEqual(len(app.exception), 0)
        self.assertEqual(app.selectbox(key="board_team").value, "FIDDLSTCKS")
        self.assertEqual(metric(app, "Completed Race Tiles").value, "39")
        self.assertEqual(metric(app, "Race Tiles").value, "30/30")
        self.assertEqual(metric(app, "Corp Bonus").value, "Complete")
        self.assertEqual(metric(app, "Nonqualifying").value, "0")
        markup = board_markup(app)
        self.assertEqual(markup.count('data-tile-id="'), 31)
        self.assertEqual(markup.count('data-status="complete"'), 31)
        self.assertIn("Board progress for <strong>FIDDLSTCKS</strong>", markup)

        app.selectbox(key="board_team").set_value("REX IM").run()
        self.assertEqual(metric(app, "Race Tiles").value, "5/30")
        self.assertEqual(metric(app, "Corp Bonus").value, "Not complete")
        rex_markup = board_markup(app)
        self.assertEqual(rex_markup.count('data-status="complete"'), 5)
        self.assertIn(
            'data-tile-id="path_voidwaker" data-status="locked"', rex_markup
        )
        self.assertIn(
            'data-tile-id="final_corrupted_gauntlet" data-status="available"',
            rex_markup,
        )
        self.assertIn("Zenyte shards 1/6", rex_markup)


if __name__ == "__main__":
    unittest.main()
