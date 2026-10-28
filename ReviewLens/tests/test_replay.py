import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from jobs import replay_months

class ReplayProgressTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.manifest={'source_generation':'verified-source','total_reviews':5,'month_min':'2013-06','month_max':'2013-07','months':[
            {'month':'2013-06','reviews':2,'platforms':{'Lazada':{'reviews':2,'low':1,'rating_sum':6}},'hdfs_source':'hdfs://prepared/month=2013-06','files':1,'bytes':100},
            {'month':'2013-07','reviews':3,'platforms':{'Lazada':{'reviews':3,'low':0,'rating_sum':15}},'hdfs_source':'hdfs://prepared/month=2013-07','files':1,'bytes':150}]}
    def tearDown(self):self.tmp.cleanup()
    def test_frames_advance_only_after_successful_hdfs_copy(self):
        state=replay_months.new_run(self.manifest)
        with patch.object(replay_months,'OUT',self.root),patch.object(replay_months,'copy_month',side_effect=RuntimeError('copy failed')):
            with self.assertRaises(RuntimeError):replay_months.step(state,self.manifest)
        self.assertEqual((state['cursor'],state['loaded_reviews'],state['frames']),(0,0,[]))
    def test_resume_counts_each_month_once_and_reset_is_isolated(self):
        state=replay_months.new_run(self.manifest)
        with patch.object(replay_months,'OUT',self.root),patch.object(replay_months,'copy_month') as copy:
            replay_months.step(state,self.manifest)
            restored=json.loads((self.root/'status.json').read_text())
            replay_months.step(restored,self.manifest)
        self.assertEqual(restored['loaded_reviews'],5)
        self.assertEqual([f['month'] for f in restored['frames']],['2013-06','2013-07'])
        self.assertEqual(copy.call_count,2)
        reset=replay_months.new_run(self.manifest,'request',2)
        self.assertNotEqual(reset['run_id'],restored['run_id'])
        self.assertEqual(reset['loaded_reviews'],0)
        self.assertTrue((self.root/'runs'/restored['run_id']/'status.json').exists())
        self.assertIn('/ecommerce/replay/runs/',reset['hdfs_loaded_source'])
        self.assertEqual(self.manifest['total_reviews'],5)
if __name__=='__main__':unittest.main()
