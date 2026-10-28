"""Drive actual HDFS replay controls and verify the sliding chart; no fabricated input."""
import json,time
from pathlib import Path
from playwright.sync_api import sync_playwright
BASE='http://127.0.0.1:8090';OUT=Path('/workspace/outputs')

def main():
    errors=[];checks=[]
    with sync_playwright() as p:
        browser=p.chromium.launch();page=browser.new_page(viewport={'width':1440,'height':1050})
        page.on('pageerror',lambda e:errors.append(str(e)))
        before=page.request.get(BASE+'/api/overview?source=all').json()['stats']['reviews']
        live_before=page.request.get(BASE+'/api/overview?source=live').json()['stats']['reviews']
        page.goto(BASE+'/?source=all#replay',wait_until='networkidle')
        page.wait_for_function('ReplayView.view.history?.cursor>0',timeout=120000)
        assert page.locator('#replay-platforms [aria-pressed=true]').count()==4
        assert page.locator('#filter-form').is_hidden()
        checks.append('All four platforms selected; replay controls separate from global filters')
        page.locator('#replay-speed').select_option('2')
        page.wait_for_function('ReplayView.view.history.interval_seconds===2',timeout=30000)
        old_run=page.evaluate('ReplayView.view.history.run_id')
        page.locator('#replay-reset').click()
        page.wait_for_function('(id)=>ReplayView.view.history.run_id!==id && ReplayView.view.history.cursor>=3',arg=old_run,timeout=120000)
        initial=page.evaluate('ReplayView.view.history')
        assert initial['frames'][0]['month']=='2013-06'
        assert initial['loaded_reviews']==sum(f['reviews'] for f in initial['frames'])
        checks.append('Reset starts an isolated HDFS run at June 2013; cumulative count equals copied batches')
        page.wait_for_function('ReplayView.view.history.cursor>=14',timeout=180000)
        page.wait_for_timeout(1000)
        history=page.evaluate('ReplayView.view.history')
        cursor=history['cursor']
        assert len(history['frames'])==cursor
        assert [f['month'] for f in history['frames']]==sorted(set(f['month'] for f in history['frames']))
        assert page.locator('#replay-chart circle').count()<=48
        key='Lazada:'+history['frames'][-2]['month']
        old_x=page.locator('#replay-chart circle[data-key="'+key+'"]').get_attribute('cx')
        page.wait_for_function('(n)=>ReplayView.view.history.cursor>n',arg=cursor,timeout=30000)
        page.wait_for_timeout(1000)
        new_x=page.locator('#replay-chart circle[data-key="'+key+'"]').get_attribute('cx')
        assert float(new_x)<float(old_x),(old_x,new_x)
        checks.append('Actual newly copied batch moves existing points left; window retains at most twelve months per platform')
        page.locator('#replay-pause').click()
        page.wait_for_function('ReplayView.view.history.phase==="paused"',timeout=30000)
        paused=page.evaluate('ReplayView.view.history.cursor');page.wait_for_timeout(4500)
        assert page.evaluate('ReplayView.view.history.cursor')==paused
        page.screenshot(path=str(OUT/'screenshots/12-sliding-historical-replay-20261015.png'),full_page=True)
        page.get_by_role('button',name='Đồ thị Shopee',exact=True).click()
        assert page.locator('#replay-platforms [aria-pressed=true]').count()==3
        page.get_by_role('button',name='Đồ thị Shopee',exact=True).click()
        page.locator('#replay-metric').select_option('mean')
        page.wait_for_timeout(800)
        assert page.locator('#replay-chart').inner_text().find('5')>=0
        page.locator('#replay-metric').select_option('rate')
        checks.append('Pause holds the batch cursor; platform and metric selections redraw automatically')
        page.locator('#replay-switch').click()
        page.wait_for_function('ReplayView.view.mode==="live" && ReplayView.view.live.length>0',timeout=30000)
        timeline=page.request.get(BASE+'/api/live-timeline').json()
        assert page.locator('#replay-controls').is_hidden()
        assert page.evaluate('ReplayView.view.live.at(-1).total_reviews')==timeline['points'][-1]['total_reviews']==live_before
        page.screenshot(path=str(OUT/'screenshots/13-real-review-observation-timeline-20261015.png'),full_page=True)
        checks.append('Second graph uses verified real-review observation checkpoints; reset controls hidden')
        page.locator('#replay-switch').click()
        page.wait_for_function('ReplayView.view.mode==="history"')
        page.locator('#replay-pause').click()
        page.wait_for_function('ReplayView.view.history.phase!=="paused"',timeout=30000)
        after=page.request.get(BASE+'/api/overview?source=all').json()['stats']['reviews']
        assert after==before
        assert page.request.get(BASE+'/api/overview?source=live').json()['stats']['reviews']==live_before
        assert not errors,errors
        result={'verification':'passed','run_id':history['run_id'],'source_mode':'historical_replay','observed_months':paused,'window_months':12,'default_platforms':4,'production_reviews_unchanged':after,'real_reviews_unchanged':live_before,'points_move_left':True,'checks':checks,'browser_errors':errors}
        browser.close()
    (OUT/'replay_browser_validation.json').write_text(json.dumps(result,ensure_ascii=False,indent=2));print(json.dumps(result,ensure_ascii=False))
if __name__=='__main__':main()
