import importlib.util
import sys
import unittest
from datetime import datetime
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

@unittest.skipUnless(importlib.util.find_spec('google_play_scraper') and importlib.util.find_spec('kafka') and importlib.util.find_spec('pyspark'),'Run collector tests in project runtime')
class LiveCollectorTests(unittest.TestCase):
    def test_version_id_ignores_poll_time_but_tracks_review_edits(self):
        from jobs.collect_live_reviews import build_event
        row={'reviewId':'real-id','content':'Giao hàng chậm','score':1,'at':datetime(2026,10,13,12)}
        first=build_event('com.shopee.vn',row,'poll-a','2026-10-15T00:00:00+00:00')
        repeated=build_event('com.shopee.vn',row,'poll-b','2026-10-15T00:05:00+00:00')
        edited=build_event('com.shopee.vn',{**row,'score':5,'content':'Đã giao hàng'},'poll-c','2026-10-15T00:10:00+00:00')
        self.assertEqual(first['event_id'],repeated['event_id'])
        self.assertNotEqual(first['event_id'],edited['event_id'])
        self.assertEqual(first['review_id'],edited['review_id'])
        self.assertEqual(first['event_time'],'2026-10-13 12:00:00')
        self.assertEqual(first['source_mode'],'google_play_live')
        self.assertNotIn('userName',first)
