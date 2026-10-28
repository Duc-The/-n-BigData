"""Browser proof that Clear restores every filter and both real HDFS-derived sources."""
import json
from pathlib import Path
from playwright.sync_api import sync_playwright
BASE='http://127.0.0.1:8090';OUT=Path('/workspace/outputs')

def main():
    errors=[];checks=[]
    with sync_playwright() as p:
        browser=p.chromium.launch();page=browser.new_page(viewport={'width':1440,'height':1000})
        page.on('pageerror',lambda e:errors.append(str(e)))
        boot=page.request.get(BASE+'/api/bootstrap?source=all').json();meta=boot['metadata']
        expected=meta['historical_rows']+meta['live_rows']-meta['overlapping_review_ids']
        assert meta['processed_rows']==expected
        overview=page.request.get(BASE+'/api/overview?source=all').json()
        reviews=page.request.get(BASE+'/api/reviews?source=all&score=all').json()
        assert overview['stats']['reviews']==reviews['total']==expected
        page.goto(BASE,wait_until='networkidle')
        page.wait_for_function('(n)=>state.readyInitToken===state.initToken && state.source==="all" && state.data?.stats.reviews===n',arg=expected)
        checks.append('Default source covers historical and live; API cube and review counts agree')
        page.locator('#data-source').select_option('live')
        page.wait_for_function('state.readyInitToken===state.initToken && state.source==="live" && state.data?.stats.reviews===state.boot.metadata.processed_rows')
        page.get_by_label('Chọn Tiki',exact=True).uncheck()
        page.locator('#exclude-spam').check()
        page.locator('[data-view=reviews]').click()
        page.locator('#review-query').fill('khong')
        page.locator('#review-aspect').select_option('app')
        page.locator('#review-score').select_option('1')
        page.locator('#clear-filters').click()
        page.wait_for_function('(n)=>state.readyInitToken===state.initToken && state.source==="all" && state.data?.stats.reviews===n && state.reviewResult?.total===n',arg=expected)
        controls=page.evaluate('({source:document.querySelector("#data-source").value,from:document.querySelector("#from-month").value,to:document.querySelector("#to-month").value,spam:document.querySelector("#exclude-spam").checked,platforms:[...document.querySelectorAll("#platform-filters input:checked")].map(x=>x.value),q:document.querySelector("#review-query").value,score:document.querySelector("#review-score").value,aspect:document.querySelector("#review-aspect").value,reviewPlatform:document.querySelector("#review-platform").value})')
        assert controls=={'source':'all','from':meta['month_min'],'to':meta['month_max'],'spam':False,'platforms':['Shopee','Lazada','Sendo','Tiki'],'q':'','score':'all','aspect':'all','reviewPlatform':'all'},controls
        assert 'source=all' in page.url, page.url
        checks.append('Clear button resets source, dates, platforms, spam, text, score and aspect; both views show total')
        for month in ['from-month','to-month']:
            page.locator('#data-source').select_option('live')
            page.wait_for_function('state.readyInitToken===state.initToken && state.source==="live" && state.boot.metadata.source_mode==="google_play_live"')
            page.locator('#'+month).fill('')
            page.wait_for_function('(n)=>state.readyInitToken===state.initToken && state.source==="all" && state.data?.stats.reviews===n && state.reviewResult?.total===n',arg=expected)
        checks.append('Native Clear on either month restores the full dataset automatically')
        page.locator('[data-view=overview]').click()
        page.wait_for_function('document.querySelector("#loading").classList.contains("hidden")')
        page.wait_for_timeout(5500)
        page.screenshot(path=str(OUT/'screenshots/11-clear-all-data-20261015.png'),full_page=True)
        assert not errors,errors
        browser.close()
    result={'verification':'passed','generation':meta['generation'],'historical_reviews':meta['historical_rows'],'live_reviews':meta['live_rows'],'overlapping_review_ids':meta['overlapping_review_ids'],'total_reviews':expected,'month_min':meta['month_min'],'month_max':meta['month_max'],'checks':checks,'browser_errors':errors}
    (OUT/'clear_filters_validation.json').write_text(json.dumps(result,ensure_ascii=False,indent=2));print(json.dumps(result,ensure_ascii=False))
if __name__=='__main__':main()
