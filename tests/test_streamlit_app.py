import sys
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT_DIR = Path(__file__).resolve().parents[1]
TEMP_DEPENDENCIES = Path.home() / "AppData" / "Local" / "Temp" / "bingo-stats-test-deps"
if TEMP_DEPENDENCIES.exists():
    sys.path.insert(0, str(TEMP_DEPENDENCIES))
sys.path.insert(0, str(PROJECT_DIR))

from streamlit.testing.v1 import AppTest  # noqa: E402


class FakeWomResponse:
    status_code = 200
    headers = {}

    @staticmethod
    def raise_for_status():
        return None

    @staticmethod
    def json():
        return []


class StreamlitAppTests(unittest.TestCase):
    def setUp(self):
        self.requests_get_patch = patch("requests.get", return_value=FakeWomResponse())
        self.requests_get_patch.start()

    def tearDown(self):
        self.requests_get_patch.stop()

    def test_default_data_renders_board_without_exception(self):
        app = AppTest.from_file(str(PROJECT_DIR / "bingostats.py"), default_timeout=30).run()

        self.assertEqual(len(app.exception), 0)
        self.assertEqual(app.tabs[1].label, "Tile Planner")
        self.assertTrue(
            any(subheader.value == "FIDDLSTCKS Grid Tile Planner" for subheader in app.subheader)
        )
        self.assertEqual(app.tabs[0].label, "🗺️ Board Progress")
        board_markup = next(markdown.value for markdown in app.markdown if 'data-tile-id="start_toa"' in markdown.value)
        self.assertEqual(board_markup.count('data-tile-id="'), 31)

    def test_uploaded_csv_completes_opening_cg_and_escapes_hover_content(self):
        csv_text = """Date,Player Name,Team,Tile,Item Received
29/08/2026 14:30,<script>alert(1)</script>,AOCL,CG,Armour seed & pet
29/08/2026 14:31,Second Player,MASELF,Nex,Nihil horn
"""
        app = AppTest.from_file(str(PROJECT_DIR / "bingostats.py"), default_timeout=30).run()
        app.file_uploader[0].set_value(("summer.csv", csv_text.encode("utf-8"), "text/csv")).run()

        self.assertEqual(len(app.exception), 0)
        self.assertEqual(app.selectbox(key="board_team").value, "AOCL")
        board_markup = next(markdown.value for markdown in app.markdown if 'data-tile-id="start_toa"' in markdown.value)
        self.assertIn(
            'data-tile-id="final_corrupted_gauntlet" '
            'data-status="complete" data-submission-count="1"',
            board_markup,
        )
        self.assertIn('data-tile-id="start_toa" data-status="available"', board_markup)
        self.assertIn('data-tile-id="final_araxxor" data-status="locked"', board_markup)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", board_markup)
        self.assertNotIn("<script>alert(1)</script>", board_markup)
        self.assertTrue(
            any("first eligible submission finishes a tile" in info.value for info in app.info)
        )

    def test_partial_entry_numbers_fall_back_to_source_order(self):
        csv_text = """Entry #,Date,Player Name,Team,Tile,Item Received
100,29/08/2026 10:00,Early Nex,AOCL,Nex,EARLY_NEX
,29/08/2026 10:01,Starter,AOCL,TOA,START_TOA
101,29/08/2026 10:02,Valid Nex,AOCL,Nex,VALID_NEX
"""
        app = AppTest.from_file(str(PROJECT_DIR / "bingostats.py"), default_timeout=30).run()
        app.file_uploader[0].set_value(
            ("partial_entries.csv", csv_text.encode("utf-8"), "text/csv")
        ).run()

        self.assertEqual(len(app.exception), 0)
        board_markup = next(
            markdown.value
            for markdown in app.markdown
            if 'data-tile-id="start_toa"' in markdown.value
        )
        self.assertIn(
            'data-tile-id="start_nex" data-status="complete" '
            'data-submission-count="1" data-extra-count="0" data-ignored-count="1"',
            board_markup,
        )
        self.assertIn("VALID_NEX", board_markup)

    def test_example_csv_can_select_fiddlstcks_and_render_all_hotspots(self):
        app = AppTest.from_file(str(PROJECT_DIR / "bingostats.py"), default_timeout=30).run()
        sample_bytes = (PROJECT_DIR / "example_tile_race.csv").read_bytes()
        app.file_uploader[0].set_value(
            ("example_tile_race.csv", sample_bytes, "text/csv")
        ).run()
        app.selectbox(key="board_team").set_value("FIDDLSTCKS").run()

        self.assertEqual(len(app.exception), 0)
        self.assertEqual(app.selectbox(key="board_team").value, "FIDDLSTCKS")
        board_markup = next(
            markdown.value
            for markdown in app.markdown
            if 'data-tile-id="start_toa"' in markdown.value
        )
        self.assertEqual(board_markup.count('data-tile-id="'), 31)
        self.assertEqual(board_markup.count('data-status="complete"'), 31)
        self.assertIn("Board progress for <strong>FIDDLSTCKS</strong>", board_markup)
        self.assertTrue(any("Everything is finished" in caption.value for caption in app.caption))

        app.selectbox(key="board_team").set_value("REX IM").run()
        rex_markup = next(
            markdown.value
            for markdown in app.markdown
            if 'data-tile-id="start_toa"' in markdown.value
        )
        self.assertEqual(rex_markup.count('data-status="complete"'), 15)
        self.assertIn('data-tile-id="path_voidwaker" data-status="available"', rex_markup)
        self.assertIn(
            'data-tile-id="final_corrupted_gauntlet" data-status="available"',
            rex_markup,
        )


if __name__ == "__main__":
    unittest.main()
