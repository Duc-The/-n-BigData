"""Copy real HDFS month batches, then atomically publish progress. Never alter production sources."""
import fcntl,json,subprocess,time,uuid
from datetime import datetime,timezone
from pathlib import Path
from review_analytics.io import atomic_write_text
OUT=Path('/exports/replay');BASE='hdfs://namenode:9000/ecommerce/replay/runs'

def now():return datetime.now(timezone.utc).isoformat()
def read(path,default=None):return json.loads(path.read_text()) if path.exists() else default

_client=None

def copy_month(item,destination):
    global _client
    if _client is None:
        import os
        from pyspark.java_gateway import launch_gateway
        os.environ['PYSPARK_SUBMIT_ARGS']='--driver-memory 512m pyspark-shell'
        gateway=launch_gateway()
        conf=gateway.jvm.org.apache.hadoop.conf.Configuration()
        for name in ['core-site.xml','hdfs-site.xml']:
            conf.addResource(gateway.jvm.org.apache.hadoop.fs.Path('/opt/hadoop/etc/hadoop/'+name))
        fs=gateway.jvm.org.apache.hadoop.fs.FileSystem.get(gateway.jvm.java.net.URI(BASE),conf)
        _client=(gateway,conf,fs)
    gateway,conf,fs=_client
    PathClass=gateway.jvm.org.apache.hadoop.fs.Path
    target=PathClass(destination)
    fs.mkdirs(target.getParent())
    if not fs.exists(target):
        temporary=PathClass(destination.rsplit('/',1)[0]+'/_pending_'+uuid.uuid4().hex)
        if not gateway.jvm.org.apache.hadoop.fs.FileUtil.copy(fs,PathClass(item['hdfs_source']),fs,temporary,False,conf):
            raise ValueError('HDFS copy failed')
        summary=fs.getContentSummary(temporary)
        if (summary.getFileCount(),summary.getLength())!=(item['files'],item['bytes']):raise ValueError('Incomplete HDFS batch copy')
        if not fs.rename(temporary,target):raise ValueError('Could not commit HDFS month batch')
    summary=fs.getContentSummary(target)
    if (summary.getFileCount(),summary.getLength())!=(item['files'],item['bytes']):raise ValueError('Loaded HDFS batch differs from prepared files')

def new_run(manifest,control_id=None,speed=4):
    run=uuid.uuid4().hex
    return {'run_id':run,'source_mode':'historical_replay','source_generation':manifest['source_generation'],'phase':'running','cursor':0,'frames':[],'loaded_reviews':0,'total_reviews':manifest['total_reviews'],'month_min':manifest['month_min'],'month_max':manifest['month_max'],'total_batches':len(manifest['months']),'interval_seconds':speed,'last_control':control_id,'hdfs_loaded_source':BASE+'/'+run+'/loaded','started_utc':now(),'updated_utc':now()}

def save(state):
    state['updated_utc']=now();atomic_write_text(OUT/'status.json',json.dumps(state,indent=2))
    if state.get('run_id'):atomic_write_text(OUT/'runs'/state['run_id']/'status.json',json.dumps(state,indent=2))

def step(state,manifest):
    item=manifest['months'][state['cursor']];month=item['month'];destination=state['hdfs_loaded_source']+'/month='+month
    state.update(phase='copying',loading_month=month);save(state)
    copy_month(item,destination)
    state['loaded_reviews']+=item['reviews'];state['cursor']+=1
    frame={**item,'hdfs_loaded_source':destination,'loaded_utc':now(),'cumulative_reviews':state['loaded_reviews']}
    state['frames'].append(frame);state.update(phase='running',current_month=month,loading_month=None);save(state)
    atomic_write_text(OUT/'runs'/state['run_id']/('batch_'+month+'.json'),json.dumps(frame,indent=2))
    print(json.dumps({'run_id':state['run_id'],'batch':month,'loaded_reviews':state['loaded_reviews'],'hdfs':destination}),flush=True)

def main():
    OUT.mkdir(parents=True,exist_ok=True)
    with (OUT/'runner.lock').open('w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        state=read(OUT/'status.json',{})
        while True:
            manifest=read(OUT/'prepared.json')
            if not manifest:
                if not state:save({'phase':'preparing','loaded_reviews':0,'frames':[]})
                time.sleep(1);continue
            if not state.get('run_id') or state.get('source_generation')!=manifest['source_generation']:state=new_run(manifest);save(state)
            control=read(OUT/'control.json',{})
            if control.get('request_id') and control['request_id']!=state.get('last_control'):
                action=control['action'];speed=control.get('interval_seconds',state['interval_seconds'])
                if action=='reset':state=new_run(manifest,control['request_id'],speed)
                else:
                    state['last_control']=control['request_id'];state['interval_seconds']=speed
                    if action=='pause':state['phase']='paused'
                    if action=='resume' and state['phase'] not in ['completed']:state['phase']='running'
                save(state)
            if state['phase'] in ['paused','completed','error']:time.sleep(.5);continue
            try:
                started=time.monotonic()
                if state['cursor']<len(manifest['months']):step(state,manifest)
                if state['cursor']==len(manifest['months']):
                    state['phase']='verifying';save(state)
                    log=OUT/'runs'/state['run_id']/'spark_verification.log'
                    with log.open('w') as handle:
                        subprocess.run(['spark-submit','--master','spark://spark-master:7077','--driver-memory','1g','--executor-memory','1g','/workspace/jobs/prepare_replay.py','--verify-run',state['run_id']],stdout=handle,stderr=subprocess.STDOUT,check=True,timeout=600)
                    state['verification']=read(OUT/('validation_'+state['run_id']+'.json'));state['phase']='completed';save(state)
                while time.monotonic()-started<state['interval_seconds']:
                    if read(OUT/'control.json',{}).get('request_id')!=state.get('last_control'):break
                    time.sleep(.2)
            except Exception as error:
                state.update(phase='error',error=str(error));save(state);print(json.dumps({'error':str(error)}),flush=True)
if __name__=='__main__':main()
