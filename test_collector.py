"""
Tests for the data collector.
Run with: python test_collector.py

Uses mock API responses — no real Meta credentials needed to run tests.
"""

import json
import sqlite3
import sys
import os
import unittest
from unittest.mock import patch, MagicMock

# Allow import from parent dir
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_collector.collector import (
    MetaCollector, _parse_insight, _extract_action,
    _extract_action_value, init_db
)


# ── Sample raw insight (mimics Meta API response) ─────────────────────────────
SAMPLE_INSIGHT = {
    "ad_id":        "120123456789",
    "ad_name":      "Summer Sale Video v1",
    "adset_id":     "220123456789",
    "adset_name":   "Lookalike 18-35",
    "campaign_id":  "320123456789",
    "campaign_name":"Q3 Acquisition",
    "spend":        "45.23",
    "impressions":  "12400",
    "reach":        "9800",
    "clicks":       "248",
    "ctr":          "2.0",
    "cpm":          "3.65",
    "cpc":          "0.18",
    "date_start":   "2025-07-01",
    "date_stop":    "2025-07-07",
    "actions": [
        {"action_type": "purchase",     "value": "7"},
        {"action_type": "link_click",   "value": "248"},
        {"action_type": "post_reaction","value": "34"},
    ],
    "action_values": [
        {"action_type": "purchase", "value": "315.00"},
    ],
    "video_play_actions": [
        {"action_type": "video_view", "value": "4200"},
    ],
    "video_p25_watched_actions": [{"action_type": "video_view", "value": "3100"}],
    "video_p50_watched_actions": [{"action_type": "video_view", "value": "1800"}],
    "video_p75_watched_actions": [{"action_type": "video_view", "value": "900"}],
    "video_p100_watched_actions":[{"action_type": "video_view", "value": "340"}],
}

SAMPLE_LOSER = {
    **SAMPLE_INSIGHT,
    "ad_id":    "120999999999",
    "ad_name":  "Bad Static Image v3",
    "spend":    "22.50",
    "impressions": "6000",
    "reach":    "5000",
    "clicks":   "12",
    "ctr":      "0.2",
    "cpm":      "3.75",
    "cpc":      "1.88",
    "actions":  [],
    "action_values": [],
    "video_play_actions": [],
    "video_p25_watched_actions": [],
    "video_p50_watched_actions": [],
    "video_p75_watched_actions": [],
    "video_p100_watched_actions": [],
}


class TestParseInsight(unittest.TestCase):

    def test_basic_fields(self):
        parsed = _parse_insight(SAMPLE_INSIGHT)
        self.assertEqual(parsed["ad_id"],   "120123456789")
        self.assertEqual(parsed["ad_name"], "Summer Sale Video v1")
        self.assertEqual(parsed["spend"],    45.23)
        self.assertEqual(parsed["impressions"], 12400)
        self.assertEqual(parsed["clicks"],   248)

    def test_roas_calculated(self):
        parsed = _parse_insight(SAMPLE_INSIGHT)
        # 315.00 revenue / 45.23 spend ≈ 6.97
        self.assertAlmostEqual(parsed["roas"], 315.0 / 45.23, places=2)

    def test_roas_zero_spend(self):
        insight = {**SAMPLE_INSIGHT, "spend": "0"}
        parsed = _parse_insight(insight)
        self.assertEqual(parsed["roas"], 0.0)

    def test_frequency_computed(self):
        parsed = _parse_insight(SAMPLE_INSIGHT)
        # 12400 imp / 9800 reach ≈ 1.27
        self.assertAlmostEqual(parsed["frequency"], 12400 / 9800, places=2)

    def test_conversions_extracted(self):
        parsed = _parse_insight(SAMPLE_INSIGHT)
        self.assertEqual(parsed["conversions"], 7)
        self.assertEqual(parsed["conversion_value"], 315.0)

    def test_video_views_extracted(self):
        parsed = _parse_insight(SAMPLE_INSIGHT)
        self.assertEqual(parsed["video_plays"], 4200)
        self.assertEqual(parsed["video_p25"],   3100)
        self.assertEqual(parsed["video_p50"],   1800)
        self.assertEqual(parsed["video_p75"],    900)
        self.assertEqual(parsed["video_p100"],   340)

    def test_loser_zero_conversions(self):
        parsed = _parse_insight(SAMPLE_LOSER)
        self.assertEqual(parsed["conversions"],      0)
        self.assertEqual(parsed["conversion_value"], 0.0)
        self.assertEqual(parsed["roas"],             0.0)

    def test_no_crash_on_missing_fields(self):
        parsed = _parse_insight({})
        self.assertEqual(parsed["spend"], 0.0)
        self.assertEqual(parsed["roas"],  0.0)


class TestDatabase(unittest.TestCase):

    def setUp(self):
        self.db_path = "/tmp/test_metrics.db"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_init_creates_tables(self):
        conn = init_db(self.db_path)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        names = {t[0] for t in tables}
        self.assertIn("ad_metrics", names)
        self.assertIn("collection_runs", names)

    def test_save_and_retrieve(self):
        conn = init_db(self.db_path)
        collector = MetaCollector(db_path=self.db_path)

        parsed = _parse_insight(SAMPLE_INSIGHT)
        collector._save([parsed])

        rows = collector.latest(limit=10)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ad_id"], "120123456789")
        self.assertAlmostEqual(rows[0]["roas"], 315.0 / 45.23, places=2)

    def test_upsert_no_duplicates(self):
        collector = MetaCollector(db_path=self.db_path)
        parsed = _parse_insight(SAMPLE_INSIGHT)
        collector._save([parsed])
        collector._save([parsed])   # save same row twice
        rows = collector.latest(limit=100)
        self.assertEqual(len(rows), 1)

    def test_winners_and_losers(self):
        collector = MetaCollector(db_path=self.db_path)
        winner = _parse_insight(SAMPLE_INSIGHT)
        loser  = _parse_insight(SAMPLE_LOSER)
        collector._save([winner, loser])

        winners = collector.winners(min_spend=10, min_roas=2.0, days=9999)
        losers  = collector.losers(min_spend=10,  max_roas=0.5, days=9999)

        self.assertTrue(any(w["ad_id"] == "120123456789" for w in winners),
                        "Winner should appear in winners")
        self.assertTrue(any(l["ad_id"] == "120999999999" for l in losers),
                        "Loser should appear in losers")

    def test_summary(self):
        collector = MetaCollector(db_path=self.db_path)
        collector._save([_parse_insight(SAMPLE_INSIGHT), _parse_insight(SAMPLE_LOSER)])
        s = collector.summary(days=9999)
        self.assertEqual(s["ads"], 2)
        self.assertAlmostEqual(s["total_spend"], 45.23 + 22.50, places=2)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)


class TestExtractHelpers(unittest.TestCase):

    def test_extract_action(self):
        actions = [
            {"action_type": "purchase",   "value": "5"},
            {"action_type": "link_click", "value": "100"},
        ]
        self.assertEqual(_extract_action(actions, "purchase"),   5)
        self.assertEqual(_extract_action(actions, "link_click"), 100)
        self.assertEqual(_extract_action(actions, "missing"),    0)

    def test_extract_action_empty(self):
        self.assertEqual(_extract_action([], "purchase"), 0)
        self.assertEqual(_extract_action(None, "purchase"), 0)

    def test_extract_action_value(self):
        values = [{"action_type": "purchase", "value": "249.99"}]
        self.assertAlmostEqual(_extract_action_value(values, "purchase"), 249.99)
        self.assertEqual(_extract_action_value(values, "missing"), 0.0)


if __name__ == "__main__":
    print("Running data collector tests...\n")
    loader  = unittest.TestLoader()
    suite   = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestParseInsight))
    suite.addTests(loader.loadTestsFromTestCase(TestDatabase))
    suite.addTests(loader.loadTestsFromTestCase(TestExtractHelpers))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    if result.wasSuccessful():
        print("\n✓ All tests passed")
    else:
        print(f"\n✗ {len(result.failures)} failure(s), {len(result.errors)} error(s)")
        sys.exit(1)
