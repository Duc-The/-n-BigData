"""Prove paused checkpoint survives a container restart and resumes without duplicate months."""
import json,subprocess,time,urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];BASE='http://127.0.0.1:8090'

def get():return json.load(urllib.request.urlopen(BASE+'/api/replay',timeout=10))
def control(action):
    req=urllib.request.Request(BASE+'/api/replay/control',data=json.dumps({'action':action,'interval_seconds':2}).encode(),headers={'Content-Type':'application/json'},method='POST')
    return json.load(urllib.request.urlopen(req,timeout=10))
def until(predicate,timeout=50):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        data=get()
        if predicate(data):return data
        time.sleep(.5)
    raise TimeoutError('Replay state did not reach expected checkpoint')
def main():
    current=get()
    if current['cursor']>=current['total_batches']:
        until(lambda s:s['phase'] in ['completed','error'],timeout=600)
        old=current['run_id'];control('reset');until(lambda s:s['run_id']!=old and s['cursor']>=2)
    control('pause');before=until(lambda s:s['phase']=='paused')
    subprocess.run(['docker','compose','restart','replay-runner'],cwd=ROOT,check=True)
    time.sleep(3);after=get()
    assert before['run_id']==after['run_id'] and before['cursor']==after['cursor'] and before['loaded_reviews']==after['loaded_reviews']
    control('resume')
    resumed=until(lambda s:s['cursor']>before['cursor'])
    assert resumed['run_id']==before['run_id']
    assert len(resumed['frames'])==len({f['month'] for f in resumed['frames']})==resumed['cursor']
    assert resumed['loaded_reviews']==sum(f['reviews'] for f in resumed['frames'])
    result={'verification':'passed','run_id':resumed['run_id'],'before_cursor':before['cursor'],'resumed_cursor':resumed['cursor'],'restart_preserves_checkpoint':True,'unique_months':True,'loaded_reviews':resumed['loaded_reviews']}
    (ROOT/'outputs/replay_restart_validation.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
if __name__=='__main__':main()
