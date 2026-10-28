"""Fetch actual public Google Play reviews; publish only new/changed versions to Kafka.
Original review time is preserved. No synthetic events or CSV replay fallback.
"""
import argparse
import fcntl
import hashlib
import json
import socket
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from google_play_scraper import reviews, Sort
from kafka import KafkaAdminClient,KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from review_analytics.io import atomic_write_text
from review_analytics.spark_ops import APP_MAP


def build_event(appid,row,poll_id,ingested_at):
    review_id=row['reviewId']
    content=row.get('content') or ''
    score=int(row['score'])
    at=row['at'].strftime('%Y-%m-%d %H:%M:%S')
    signature=hashlib.sha256(json.dumps([appid,review_id,content,score,at],ensure_ascii=False).encode()).hexdigest()
    return {'event_id':signature,'review_id':review_id,'appid':appid,'content':content,'score':score,
            'event_time':at,'ingested_at':ingested_at,'source_mode':'google_play_live',
            'source_url':'https://play.google.com/store/apps/details?id='+appid+'&hl=vi&gl=VN','poll_id':poll_id}


def collect(args):
    args.exports.mkdir(parents=True,exist_ok=True)
    with (args.exports/'collector.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        socket.setdefaulttimeout(20)
        started=datetime.now(timezone.utc).isoformat();poll_id=uuid.uuid4().hex
        admin=KafkaAdminClient(bootstrap_servers=args.bootstrap)
        try:
            try: admin.create_topics([NewTopic(args.topic,num_partitions=3,replication_factor=1)])
            except TopicAlreadyExistsError: pass
        finally: admin.close()
        producer=KafkaProducer(bootstrap_servers=args.bootstrap,acks='all',retries=3,
                 key_serializer=lambda x:x.encode(),value_serializer=lambda x:json.dumps(x,ensure_ascii=False).encode())
        db_path=args.exports/'collector_state.sqlite'
        connection=sqlite3.connect(db_path)
        connection.execute('CREATE TABLE IF NOT EXISTS seen(appid TEXT,review_id TEXT,signature TEXT,PRIMARY KEY(appid,review_id))')
        result={'poll_id':poll_id,'started_utc':started,'source_mode':'google_play_live','topic':args.topic,
                'sort':'NEWEST','language':'vi','country':'vn','max_per_app':args.count,
                'new_reviews':0,'updated_reviews':0,'unchanged_reviews':0,'apps':[],'errors':[]}
        try:
            for appid,platform in APP_MAP.items():
                item={'appid':appid,'platform':platform,'fetched':0,'new':0,'updated':0,'unchanged':0}
                try:
                    fetched,_=reviews(appid,lang='vi',country='vn',sort=Sort.NEWEST,count=args.count)
                    item['fetched']=len(fetched)
                    if not fetched: result['errors'].append({'platform':platform,'error':'Google Play returned no reviews'})
                    for row in fetched:
                        event=build_event(appid,row,poll_id,datetime.now(timezone.utc).isoformat())
                        previous=connection.execute('SELECT signature FROM seen WHERE appid=? AND review_id=?',(appid,event['review_id'])).fetchone()
                        if previous and previous[0]==event['event_id']:
                            item['unchanged']+=1;result['unchanged_reviews']+=1;continue
                        producer.send(args.topic,key=appid+':'+event['review_id'],value=event).get(timeout=30)
                        connection.execute('INSERT OR REPLACE INTO seen VALUES(?,?,?)',(appid,event['review_id'],event['event_id']))
                        connection.commit() # Remember a version only after Kafka acknowledges it.
                        kind='updated' if previous else 'new';item[kind]+=1;result[kind+'_reviews']+=1
                except Exception as e:
                    result['errors'].append({'platform':platform,'error':str(e)[:350]})
                result['apps'].append(item)
            producer.flush()
        finally:
            producer.close();connection.close();db_path.chmod(0o664)
        result['completed_utc']=datetime.now(timezone.utc).isoformat()
        atomic_write_text(args.exports/'collection_status.json',json.dumps(result,ensure_ascii=False,indent=2))
        logs=args.exports/'collection_runs';logs.mkdir(exist_ok=True);logs.chmod(0o2775)
        atomic_write_text(logs/(poll_id+'.json'),json.dumps(result,ensure_ascii=False,indent=2))
        print(json.dumps(result,ensure_ascii=False),flush=True)
        if not any(app['fetched'] for app in result['apps']): raise RuntimeError('All real review sources unavailable; no replay fallback')

if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--bootstrap',default='kafka:29092');p.add_argument('--topic',default='ecommerce-live-reviews')
    p.add_argument('--exports',type=Path,default=Path('/exports/live'));p.add_argument('--count',type=int,default=100)
    a=p.parse_args()
    if not 1<=a.count<=200: p.error('count must be 1..200')
    collect(a)
