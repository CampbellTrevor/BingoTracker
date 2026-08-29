import sys
import unittest
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parents[1]
TEMP_DEPENDENCIES = Path.home() / "AppData" / "Local" / "Temp" / "bingo-stats-test-deps"
if TEMP_DEPENDENCIES.exists():
    sys.path.insert(0, str(TEMP_DEPENDENCIES))
sys.path.insert(0, str(PROJECT_DIR))

from streamlit.testing.v1 import AppTest  # noqa: E402


class StreamlitAppTests(unittest.TestCase):
    def test_default_data_renders_board_without_exception(self):
        app = AppTest.from_file(str(PROJECT_DIR / "bingostats.py"), default_timeout=30).run()

        self.assertEqual(len(app.exception), 0)
        self.assertEqual(app.tabs[0].label, "🗺️ Board Progress")
        board_markup = next(markdown.value for markdown in app.markdown if 'data-tile-id="start_toa"' in markdown.value)
        self.assertEqual(board_markup.count('data-tile-id="'), 31)

    def test_uploaded_csv_shows_raw_cg_drop_without_claiming_completion(self):
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
            'data-status="submitted-rules-pending" data-submission-count="1"',
            board_markup,
        )
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", board_markup)
        self.assertNotIn("<script>alert(1)</script>", board_markup)
        self.assertNotIn("state-complete", board_markup)

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
        self.assertIn("Board preview for <strong>FIDDLSTCKS</strong>", board_markup)


if __name__ == "__main__":
    unittest.main()
