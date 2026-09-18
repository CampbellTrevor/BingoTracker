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


TEAM = "FIDDLSTCKS"


def make_rows(events, team=TEAM):
    """Build the normalized event-log shape consumed by board_progress."""

    rows = []
    for source_row, event in enumerate(events, start=1):
        category, item = event[:2]
        player = event[2] if len(event) > 2 else f"Player {source_row % 5 + 1}"
        date = (
            event[3]
            if len(event) > 3
            else pd.Timestamp("2026-08-29") + pd.Timedelta(minutes=source_row)
        )
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


def example_rows():
    """Load the completed, authoritative example without Streamlit helpers."""

    raw = pd.read_csv(PROJECT_DIR / "example_tile_race.csv")
    entry_numbers = pd.to_numeric(raw["Entry #"], errors="raise")
    return pd.DataFrame(
        {
            "Date": pd.to_datetime(raw["Date"], dayfirst=True, errors="coerce"),
            "Submission_Order": entry_numbers,
            "Source_Row": entry_numbers,
            "Player": raw["Player Name"],
            "Team": raw["Team"],
            "Category": raw["Tile"],
            "Item": raw["Item Received"],
        }
    )


def example_events(first_entry=1, last_entry=90, replacements=None):
    """Return FIDDLSTCKS example rows as synthetic event tuples."""

    replacements = replacements or {}
    rows = example_rows()
    rows = rows[
        (rows["Team"] == TEAM)
        & (rows["Submission_Order"] >= first_entry)
        & (rows["Submission_Order"] <= last_entry)
    ]
    return [
        (
            row.Category,
            replacements.get(int(row.Submission_Order), row.Item),
            row.Player,
        )
        for row in rows.itertuples(index=False)
    ]


# The example deliberately finishes every rule on these exact submissions.
# Testing both sides of every boundary exercises the board replay as well as the
# standalone declarative rule evaluator.
COMPLETION_ENTRY_BY_TILE = {
    "final_corrupted_gauntlet": 1,
    "start_toa": 3,
    "start_nex": 5,
    "start_hueycoatl": 7,
    "grid_gwd": 14,
    "grid_venator_shards": 19,
    "grid_vorkath": 20,
    "grid_dks": 27,
    "grid_revs": 31,
    "grid_tob": 33,
    "grid_colosseum": 34,
    "grid_zenytes": 40,
    "grid_royal_titans": 44,
    "grid_barrows": 48,
    "grid_cerberus": 51,
    "grid_the_mad_angel": 52,
    "path_voidwaker": 55,
    "path_pnm_nightmare": 57,
    "path_cox": 59,
    "final_araxxor": 62,
    "final_dt2": 64,
    "final_tob": 65,
    "final_doom": 68,
    "final_inferno": 71,
    "final_yama": 74,
    "final_cox": 75,
    "final_tormented_demons": 81,
    "final_zulrah": 82,
    "final_maggot_king": 86,
    "final_toa": 87,
    "bonus_corp_beast": 90,
}


class BoardProgressTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.example = example_rows()
        cls.example_team = cls.example[cls.example["Team"] == TEAM].copy()

    def calculate(self, events, team=TEAM):
        return calculate_team_progress(pd.DataFrame(make_rows(events, team)), team)

    def example_through(self, entry_number):
        rows = self.example_team[
            self.example_team["Submission_Order"] <= entry_number
        ].copy()
        return calculate_team_progress(rows, TEAM)

    def test_board_catalog_aliases_and_authoritative_metadata(self):
        self.assertEqual(len(BOARD_TILES), 31)
        self.assertEqual(len({tile.tile_id for tile in BOARD_TILES}), 31)

        key_counts = Counter(tile.canonical_key for tile in BOARD_TILES)
        self.assertEqual(
            {key: count for key, count in key_counts.items() if count > 1},
            {"toa": 2, "tob": 2, "cox": 2},
        )
        self.assertEqual(match_tile_key("Tombs of Amascut"), "toa")
        self.assertEqual(match_tile_key("Tombs of Amascut 2"), "toa")
        self.assertEqual(match_tile_key("Chambers of Xeric 2"), "cox")
        self.assertEqual(match_tile_key("Nightmare / PNM"), "pnm_nightmare")
        self.assertEqual(match_tile_key("Muspah"), "venator_shards")
        self.assertEqual(match_tile_key("Zenyte Shards"), "zenytes")
        self.assertEqual(match_tile_key("Gauntlet"), "corrupted_gauntlet")
        self.assertEqual(match_tile_key("Tormented Demons"), "tormented_demons")
        self.assertEqual(match_tile_key("Moons of Peril"), "tormented_demons")
        self.assertIsNone(match_tile_key("Barrows / Moons"))
        self.assertIsNone(match_tile_key("Colo / Inferno"))

        tormented = next(
            tile for tile in BOARD_TILES if tile.tile_id == "final_tormented_demons"
        )
        self.assertEqual(tormented.label, "TORMENTED DEMONS")

        metadata = load_tile_rules(PROJECT_DIR / "tile_rules.json")
        self.assertEqual(set(metadata), {tile.tile_id for tile in BOARD_TILES})
        self.assertTrue(
            all("provisional" not in rule["status"].casefold() for rule in metadata.values())
        )
        self.assertIn(
            "not part of the 30-tile race",
            metadata["bonus_corp_beast"]["status"],
        )

    def test_example_completes_every_rule_only_on_its_exact_threshold(self):
        full = calculate_team_progress(self.example_team, TEAM)

        self.assertEqual(set(COMPLETION_ENTRY_BY_TILE), set(full["states"]))
        for tile_id, completion_entry in COMPLETION_ENTRY_BY_TILE.items():
            with self.subTest(tile_id=tile_id, boundary="before"):
                before = self.example_through(completion_entry - 1)
                self.assertNotEqual(before["states"][tile_id]["status"], "complete")
            with self.subTest(tile_id=tile_id, boundary="at"):
                self.assertEqual(full["states"][tile_id]["status"], "complete")
                self.assertEqual(
                    int(full["states"][tile_id]["completion"]["Submission_Order"]),
                    completion_entry,
                )

    def test_completed_example_is_a_30_tile_race_plus_corp_bonus(self):
        progress = calculate_team_progress(self.example_team, TEAM)

        self.assertEqual(progress["race_total"], 30)
        self.assertEqual(progress["race_completed_count"], 30)
        self.assertEqual(progress["completed_count"], 31)
        self.assertTrue(progress["bonus_complete"])
        self.assertEqual(progress["current_section"], "Race complete")
        self.assertEqual(progress["section_completed"], 30)
        self.assertEqual(progress["section_total"], 30)
        self.assertEqual(progress["next_objective"], "Everything is finished")
        self.assertEqual(progress["ignored_locked_count"], 0)
        self.assertEqual(progress["nonqualifying_count"], 0)
        self.assertEqual(progress["unmatched_count"], 0)
        self.assertEqual(progress["extra_submission_count"], 0)
        self.assertEqual(len(progress["accepted"]), len(self.example_team))
        self.assertTrue(
            all(state["status"] == "complete" for state in progress["states"].values())
        )

    def test_topology_waits_for_true_completions_and_opens_grids_in_any_order(self):
        before_last_first_grid_tile = self.example_through(51)
        self.assertEqual(before_last_first_grid_tile["current_section"], "First grid")
        self.assertEqual(before_last_first_grid_tile["section_completed"], 11)
        self.assertEqual(
            before_last_first_grid_tile["states"]["path_voidwaker"]["status"],
            "locked",
        )

        first_grid_complete = self.example_through(52)
        self.assertEqual(first_grid_complete["current_section"], "Second hallway")
        self.assertEqual(
            first_grid_complete["states"]["path_voidwaker"]["status"],
            "available",
        )
        self.assertEqual(
            first_grid_complete["states"]["path_pnm_nightmare"]["status"],
            "locked",
        )
        self.assertEqual(first_grid_complete["states"]["path_cox"]["status"], "locked")

        voidwaker_complete = self.example_through(55)
        self.assertEqual(
            voidwaker_complete["states"]["path_pnm_nightmare"]["status"],
            "available",
        )
        self.assertEqual(voidwaker_complete["states"]["path_cox"]["status"], "locked")

        nightmare_complete = self.example_through(57)
        self.assertEqual(
            nightmare_complete["states"]["path_cox"]["status"], "available"
        )

        final_grid_open = self.example_through(59)
        self.assertEqual(final_grid_open["current_section"], "Final grid")
        self.assertEqual(
            final_grid_open["states"]["bonus_corp_beast"]["status"], "locked"
        )
        self.assertTrue(
            all(
                final_grid_open["states"][tile.tile_id]["status"] == "available"
                for tile in BOARD_TILES
                if tile.stage == "final_grid"
                and tile.tile_id != "final_corrupted_gauntlet"
            )
        )

        # The first grid is available as a set, not as another ordered hallway.
        any_order = self.calculate(
            [
                ("Tombs of Amascut", "Lightbearer"),
                ("Tombs of Amascut", "Masori Body"),
                ("Nex", "Nihil Horn"),
                ("Nex", "Torva Platebody"),
                ("Hueycoatl", "Dragon Hunter Wand"),
                ("Hueycoatl", "Tome of Earth"),
                ("Zenyte Shards", "Zenyte Shard"),
                ("Mad Angel", "Jar of Light"),
            ]
        )
        self.assertEqual(any_order["states"]["grid_zenytes"]["status"], "available")
        self.assertEqual(
            len(any_order["states"]["grid_zenytes"]["submissions"]), 1
        )
        self.assertEqual(
            any_order["states"]["grid_the_mad_angel"]["status"], "complete"
        )
        self.assertEqual(any_order["states"]["grid_gwd"]["status"], "available")

    def test_partial_progress_does_not_unlock_and_locked_rows_never_bank(self):
        events = [
            ("Nex", "Ancient Hilt"),
            ("Tombs of Amascut", "Lightbearer"),
            ("Nex", "Torva Platebody"),
            ("Tombs of Amascut", "Masori Body"),
            ("Nex", "Zaryte Vambraces"),
            ("Hueycoatl", "Tome of Earth"),
            ("Nex", "Nihil Horn"),
        ]
        progress = self.calculate(events)
        states = progress["states"]

        self.assertEqual(states["start_toa"]["status"], "complete")
        self.assertEqual(len(states["start_nex"]["submissions"]), 2)
        self.assertEqual(len(states["start_nex"]["locked_attempts"]), 2)
        self.assertEqual(states["start_nex"]["completion"]["Item"], "Nihil Horn")
        self.assertEqual(states["start_hueycoatl"]["status"], "available")
        self.assertEqual(states["start_hueycoatl"]["submissions"], [])
        self.assertEqual(len(states["start_hueycoatl"]["locked_attempts"]), 1)
        self.assertEqual(progress["ignored_locked_count"], 3)

        one_real_huey = self.calculate(
            events + [("Hueycoatl", "Dragon Hunter Wand")]
        )
        self.assertEqual(
            one_real_huey["states"]["start_hueycoatl"]["status"], "available"
        )
        self.assertIn(
            "Accepted unique drops 1/2",
            "\n".join(
                one_real_huey["states"]["start_hueycoatl"]["rule_progress"]
            ),
        )
        self.assertTrue(
            all(
                one_real_huey["states"][tile.tile_id]["status"] == "locked"
                for tile in BOARD_TILES
                if tile.stage == "grid_one"
            )
        )

        completed_huey = self.calculate(
            events
            + [
                ("Hueycoatl", "Dragon Hunter Wand"),
                ("Hueycoatl", "Hueycoatl Hide"),
            ]
        )
        self.assertEqual(
            completed_huey["states"]["start_hueycoatl"]["status"], "complete"
        )
        self.assertEqual(
            len(completed_huey["states"]["start_hueycoatl"]["submissions"]), 2
        )

    def test_exactly_one_starting_cg_chest_can_contribute_before_final_grid(self):
        enhanced = self.calculate(
            [("Gauntlet", "Enhanced Crystal Weapon Seed", "Opening Chest")]
        )
        self.assertTrue(enhanced["cg_starting_chest_used"])
        self.assertTrue(enhanced["cg_complete"])
        self.assertEqual(enhanced["race_completed_count"], 1)
        self.assertEqual(
            enhanced["states"]["final_corrupted_gauntlet"]["status"], "complete"
        )
        self.assertEqual(enhanced["states"]["start_toa"]["status"], "available")

        events = [
            ("Gauntlet", "Crystal Armour Seed", "Opening Chest"),
            ("Gauntlet", "Crystal Armour Seed", "Too Early"),
            *example_events(2, 59),
        ]
        final_grid_open = self.calculate(events)
        cg = final_grid_open["states"]["final_corrupted_gauntlet"]
        self.assertTrue(final_grid_open["cg_starting_chest_used"])
        self.assertFalse(final_grid_open["cg_complete"])
        self.assertEqual(cg["status"], "available")
        self.assertEqual(len(cg["submissions"]), 1)
        self.assertEqual(len(cg["locked_attempts"]), 1)
        self.assertIn("Crystal armour seeds 1/5", "\n".join(cg["rule_progress"]))

        four_more = events + [
            ("Gauntlet", "Crystal Armour Seed", f"Final Grid {index}")
            for index in range(1, 5)
        ]
        completed = self.calculate(four_more)
        self.assertTrue(completed["cg_complete"])
        self.assertEqual(
            len(completed["states"]["final_corrupted_gauntlet"]["submissions"]),
            5,
        )
        self.assertEqual(
            completed["states"]["final_corrupted_gauntlet"]["completion"]["Player"],
            "Final Grid 4",
        )

    def test_yama_rejects_soulflame_horns_and_requires_three_qualifying_drops(self):
        gate_events = example_events(1, 59)
        horns_and_two_drops = gate_events + [
            *[("Yama", "Soulflame Horn", f"Horn {index}") for index in range(7)],
            ("Yama", "Oathplate Helm", "Yama One"),
            ("Yama", "Oathplate Helm", "Yama Two"),
        ]
        partial = self.calculate(horns_and_two_drops)
        yama = partial["states"]["final_yama"]

        self.assertEqual(yama["status"], "available")
        self.assertEqual(len(yama["nonqualifying"]), 7)
        self.assertEqual(len(yama["submissions"]), 2)
        self.assertEqual(partial["nonqualifying_count"], 7)
        self.assertTrue(
            all(row["Item"] == "Soulflame Horn" for row in yama["nonqualifying"])
        )
        self.assertIn("Qualifying occurrences 2/3", "\n".join(yama["rule_progress"]))

        complete = self.calculate(
            horns_and_two_drops + [("Yama", "Oathplate Helm", "Yama Three")]
        )
        self.assertEqual(complete["states"]["final_yama"]["status"], "complete")
        self.assertEqual(
            complete["states"]["final_yama"]["completion"]["Player"],
            "Yama Three",
        )

    def test_long_toa_and_cox_names_target_only_their_physical_slots(self):
        early_final_rows = [
            ("Tombs of Amascut 2", "Tumeken's Shadow", "Early Final TOA"),
            ("Chambers of Xeric 2", "Twisted Bow", "Early Final COX"),
        ]
        through_final_gate = early_final_rows + example_events(1, 59)
        events = through_final_gate + [
            ("Tombs of Amascut", "Masori Mask", "Regular Long TOA"),
            ("Chambers of Xeric", "Elder Maul", "Regular Long COX"),
            ("Chambers of Xeric 2", "Twisted Bow", "Explicit Final COX"),
            ("Tombs of Amascut 2", "Tumeken's Shadow", "Explicit Final TOA"),
        ]
        progress = self.calculate(events)
        states = progress["states"]

        self.assertEqual(progress["ignored_locked_count"], 2)
        self.assertEqual(
            [row["Player"] for row in states["final_toa"]["locked_attempts"]],
            ["Early Final TOA"],
        )
        self.assertEqual(
            [row["Player"] for row in states["final_cox"]["locked_attempts"]],
            ["Early Final COX"],
        )
        self.assertEqual(
            [row["Player"] for row in states["start_toa"]["extras"]],
            ["Regular Long TOA"],
        )
        self.assertEqual(
            [row["Player"] for row in states["path_cox"]["extras"]],
            ["Regular Long COX"],
        )
        self.assertEqual(
            states["final_cox"]["completion"]["Player"], "Explicit Final COX"
        )
        self.assertEqual(
            states["final_toa"]["completion"]["Player"], "Explicit Final TOA"
        )

    def test_megarare_used_in_earlier_stage_blocks_only_that_final_route(self):
        through_final_gate = example_events(
            1,
            59,
            replacements={
                2: "Tumeken's Shadow",
                32: "Scythe of Vitur",
                58: "Twisted Bow",
            },
        )
        blocked_megas = through_final_gate + [
            ("Theatre of Blood", "Scythe of Vitur", "Blocked Scythe"),
            ("Chambers of Xeric 2", "Twisted Bow", "Blocked Bow"),
            ("Tombs of Amascut 2", "Tumeken's Shadow", "Blocked Shadow"),
        ]
        blocked = self.calculate(blocked_megas)

        for tile_id in ("final_tob", "final_cox", "final_toa"):
            with self.subTest(tile_id=tile_id):
                self.assertEqual(blocked["states"][tile_id]["status"], "available")
                self.assertIn(
                    "[blocked]",
                    "\n".join(blocked["states"][tile_id]["rule_progress"]),
                )
                self.assertEqual(blocked["states"][tile_id]["submissions"], [])
                self.assertEqual(len(blocked["states"][tile_id]["nonqualifying"]), 1)
                self.assertIn(
                    "permanently blocked",
                    blocked["states"][tile_id]["nonqualifying"][0]["Reason"],
                )
        self.assertEqual(blocked["nonqualifying_count"], 3)

        alternate_routes = blocked_megas + [
            ("Theatre of Blood", "Justiciar Faceguard"),
            ("Theatre of Blood", "Justiciar Chestguard"),
            ("Theatre of Blood", "Justiciar Legguards"),
            ("Chambers of Xeric 2", "Ancestral Hat"),
            ("Chambers of Xeric 2", "Ancestral Robe Top"),
            ("Chambers of Xeric 2", "Ancestral Robe Bottom"),
            ("Tombs of Amascut 2", "Masori Mask"),
            ("Tombs of Amascut 2", "Masori Body"),
            ("Tombs of Amascut 2", "Masori Chaps"),
        ]
        completed = self.calculate(alternate_routes)
        self.assertEqual(completed["states"]["final_tob"]["status"], "complete")
        self.assertEqual(completed["states"]["final_cox"]["status"], "complete")
        self.assertEqual(completed["states"]["final_toa"]["status"], "complete")

    def test_race_finishes_before_corp_and_corp_remains_a_separate_bonus(self):
        race_complete = self.example_through(87)

        self.assertEqual(race_complete["race_completed_count"], 30)
        self.assertEqual(race_complete["race_total"], 30)
        self.assertEqual(race_complete["completed_count"], 30)
        self.assertFalse(race_complete["bonus_complete"])
        self.assertEqual(race_complete["current_section"], "Race complete")
        self.assertEqual(
            race_complete["states"]["bonus_corp_beast"]["status"], "available"
        )
        self.assertIn("Corp Beast bonus is available", race_complete["next_objective"])

        corp_partial = self.example_through(89)
        self.assertEqual(corp_partial["race_completed_count"], 30)
        self.assertEqual(corp_partial["completed_count"], 30)
        self.assertFalse(corp_partial["bonus_complete"])
        self.assertEqual(
            len(corp_partial["states"]["bonus_corp_beast"]["submissions"]), 2
        )

        corp_complete = self.example_through(90)
        self.assertEqual(corp_complete["race_completed_count"], 30)
        self.assertEqual(corp_complete["completed_count"], 31)
        self.assertTrue(corp_complete["bonus_complete"])
        self.assertEqual(corp_complete["current_section"], "Race complete")
        self.assertEqual(corp_complete["next_objective"], "Everything is finished")

    def test_submission_order_not_date_controls_unlocks(self):
        events = [
            ("Nex", "Ancient Hilt", "Early", pd.NaT),
            (
                "Tombs of Amascut",
                "Lightbearer",
                "Later",
                pd.Timestamp("2026-08-01"),
            ),
            (
                "Tombs of Amascut",
                "Masori Body",
                "Later",
                pd.Timestamp("2026-08-01"),
            ),
        ]
        progress = self.calculate(events)

        self.assertEqual(progress["states"]["start_toa"]["status"], "complete")
        self.assertEqual(progress["states"]["start_nex"]["status"], "available")
        self.assertEqual(progress["states"]["start_nex"]["submissions"], [])
        self.assertEqual(
            len(progress["states"]["start_nex"]["locked_attempts"]), 1
        )

    def test_team_replays_are_isolated(self):
        aocl_events = [
            ("Tombs of Amascut", "Lightbearer"),
            ("Tombs of Amascut", "Masori Body"),
        ]
        rows = make_rows(aocl_events, team="AOCL")
        rows += make_rows([("Tombs of Amascut", "Lightbearer")], team="MASELF")
        frame = pd.DataFrame(rows)

        aocl = calculate_team_progress(frame, "AOCL")
        maself = calculate_team_progress(frame, "MASELF")

        self.assertEqual(aocl["states"]["start_toa"]["status"], "complete")
        self.assertEqual(maself["states"]["start_toa"]["status"], "available")
        self.assertEqual(len(maself["states"]["start_toa"]["submissions"]), 1)

    def test_render_escapes_all_submission_surfaces_and_shows_rule_progress(self):
        team = '<img src=x onerror="alert(1)">'
        events = [
            ("Gauntlet", "Crystal Armour Seed", "<script>alert(2)</script>"),
            ("Gauntlet", "Crystal Armour Seed", "Locked <svg onload=alert(3)>"),
            ("Tombs of Amascut", "Osmumten's Fang", "Alice & Bob"),
            (
                "Tombs of Amascut",
                "Mystery <img src=x onerror=alert(4)>",
                "Unknown Item",
            ),
            ("Nex", "Ancient Hilt", "Early <b>Player</b>"),
        ]
        progress = self.calculate(events, team=team)
        markup = render_board_html(
            PROJECT_DIR / "assets" / "bingoboard.png",
            progress,
            team,
        )

        self.assertEqual(markup.count('data-tile-id="'), 31)
        self.assertIn(
            'data-tile-id="final_corrupted_gauntlet" data-status="locked"',
            markup,
        )
        self.assertIn('data-tile-id="start_toa" data-status="available"', markup)
        self.assertIn('data-tile-id="start_nex" data-status="locked"', markup)
        self.assertIn("Crystal armour seeds 1/5", markup)
        self.assertIn("Accepted unique drops 1/2", markup)
        self.assertIn("Nonqualifying submissions (1)", markup)
        self.assertIn("Ignored while locked (1)", markup)
        self.assertIn("&lt;script&gt;alert(2)&lt;/script&gt;", markup)
        self.assertIn("Locked &lt;svg onload=alert(3)&gt;", markup)
        self.assertIn("Alice &amp; Bob", markup)
        self.assertIn("Mystery &lt;img src=x onerror=alert(4)&gt;", markup)
        self.assertIn("Early &lt;b&gt;Player&lt;/b&gt;", markup)
        self.assertIn("&lt;img src=x onerror=&quot;alert(1)&quot;&gt;", markup)
        self.assertNotIn("<script>alert(2)</script>", markup)
        self.assertNotIn("<svg onload=alert(3)>", markup)
        self.assertNotIn("<img src=x onerror=alert(4)>", markup)


if __name__ == "__main__":
    unittest.main()
