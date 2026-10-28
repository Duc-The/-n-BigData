"""Browser acceptance: real filtered results, debounce, stale requests and automatic refresh."""
import json,time
from pathlib import Path
from playwright.sync_api import sync_playwright
BASE='http://127.0.0.1:8090'
OUT=Path('/workspace/outputs')

def main():
    errors=[];checks=[]
    with sync_playwright() as p:
        browser=p.chromium.launch();page=browser.new_page(viewport={'width':1440,'height':1000})
        page.on('pageerror',lambda e:errors.append(str(e)))
        page.goto(BASE+'/?source=historical',wait_until='networkidle')
        page.wait_for_function('state.data && state.data.stats.reviews === 1299873')
        if page.get_by_role('button',name='Áp dụng',exact=True).count() or page.get_by_role('button',name='Tìm phản hồi',exact=True).count(): raise ValueError('Manual submit buttons remain')
        for platform in ['Lazada','Sendo','Tiki']: page.get_by_label('Chọn '+platform,exact=True).uncheck()
        page.wait_for_function("state.filters.platforms.length===1 && state.data.stats.reviews===519191")
        if not page.get_by_label('Chọn Shopee',exact=True).is_disabled(): raise ValueError('Last platform can be cleared')
        checks.append('Selecting platforms automatically recalculates actual counts; one platform minimum')
        page.locator('#from-month').fill('2021-01')
        page.wait_for_function("state.filters.from==='2021-01' && state.data.stats.reviews===260800")
        checks.append('Month change automatically recalculates counts without submitting')
        page.locator('#exclude-spam').check()
        page.wait_for_function('state.filters.exclude_spam===true')
        page.locator('[data-view=reviews]').click()
        page.locator('#review-aspect').select_option('app')
        page.locator('#review-query').fill('dang nhap')
        page.wait_for_function("state.reviewResult && state.reviewResult.total>0 && state.reviewResult.rows.every(r=>r.aspects.includes('app'))")
        expected=page.request.get(BASE+'/api/reviews?source=historical&platforms=Shopee&from=2021-01&to=2022-08&exclude_spam=1&aspect=app&q=dang%20nhap').json()
        page.wait_for_function('(n)=>state.reviewResult.total===n',arg=expected['total'])
        checks.append('Search input and aspect filter automatically match real API results')
        page.locator('#review-score').select_option('5')
        page.wait_for_function('state.reviewResult && state.reviewResult.rows.every(r=>r.score===5) && state.reviewResult.total>0')
        checks.append('Rating filter updates review list automatically')
        before=page.evaluate('({filters:state.filters,q:document.querySelector("#review-query").value,aspect:document.querySelector("#review-aspect").value,score:document.querySelector("#review-score").value})')
        calls=[]
        page.on('request',lambda r:calls.append(r.url) if '/api/bootstrap' in r.url else None)
        page.wait_for_timeout(11000)
        if not calls: raise ValueError('No automatic refresh poll')
        after=page.evaluate('({filters:state.filters,q:document.querySelector("#review-query").value,aspect:document.querySelector("#review-aspect").value,score:document.querySelector("#review-score").value})')
        if before!=after: raise ValueError('Auto refresh reset user filters')
        checks.append('Automatic ten-second refresh preserves active filters and search')
        page.locator('[data-view=overview]').click()
        page.evaluate("""window.actualFetch=window.fetch;window.fetch=async(...args)=>{const response=await actualFetch(...args);const u=new URL(args[0],location.origin);if(u.pathname==='/api/overview'&&u.searchParams.get('platforms')==='Shopee,Lazada')await new Promise(r=>setTimeout(r,1200));return response;}""")
        page.get_by_label('Chọn Lazada',exact=True).check()
        page.wait_for_timeout(350)
        page.get_by_label('Chọn Sendo',exact=True).check()
        page.wait_for_function('state.filters.platforms.length===3')
        page.wait_for_timeout(1500)
        expected=page.request.get(BASE+'/api/overview?source=historical&platforms=Shopee,Lazada,Sendo&from=2021-01&to=2022-08&exclude_spam=1').json()
        if page.evaluate('state.data.stats.reviews')!=expected['stats']['reviews']: raise ValueError('Stale overview response overwrote newest filters')
        checks.append('A delayed older API response cannot overwrite the newest filter selection')
        page.screenshot(path=str(OUT/'screenshots/07-auto-filters-20261015.png'),full_page=True)
        first_failure={'remaining':1}
        def temporary_failure(route):
            if first_failure['remaining']:
                first_failure['remaining']-=1
                route.fulfill(status=503,content_type='application/json',body='{"error":"Snapshot updating"}')
            else: route.continue_()
        page.route('**/api/bootstrap*',temporary_failure)
        page.reload(wait_until='networkidle')
        page.wait_for_function('state.boot && state.data && state.data.stats.reviews===1299873',timeout=20000)
        checks.append('Initial transient snapshot failure retries automatically without refresh button')
        if errors: raise ValueError(errors)
        browser.close()
    result={'verification':'passed','checks':checks,'browser_errors':errors,'source':'Actual historical HDFS-derived API; no new reviews fabricated'}
    (OUT/'auto_filters_validation.json').write_text(json.dumps(result,ensure_ascii=False,indent=2));print(json.dumps(result))
if __name__=='__main__': main()
