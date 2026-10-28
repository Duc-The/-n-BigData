import json,socket
from google_play_scraper import reviews,Sort
socket.setdefaulttimeout(20)
for appid in ['com.shopee.vn','com.lazada.android','com.sendo','vn.tiki.app.tikiandroid']:
    try:
        data,_=reviews(appid,lang='vi',country='vn',sort=Sort.NEWEST,count=3)
        print(json.dumps({'appid':appid,'count':len(data),'examples':[{'id':r['reviewId'],'score':r['score'],'at':str(r['at']),'content':r['content'][:160]} for r in data]},ensure_ascii=False),flush=True)
    except Exception as e: print(json.dumps({'appid':appid,'error':str(e)},ensure_ascii=False),flush=True)
