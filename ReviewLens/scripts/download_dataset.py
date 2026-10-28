"""Download the public Kaggle snapshot and verify its exact fingerprint."""
import hashlib
import shutil
import tempfile
import urllib.request
import zipfile
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
URL='https://www.kaggle.com/api/v1/datasets/download/hienbm/vietnamese-ecommerce-review'
SHA='9d333c550cb141aa0f3e9f12af43e8e407809031b073b046b61607f84b39bdf9'

def fingerprint(path):
    digest=hashlib.sha256()
    with path.open('rb') as handle:
        while chunk:=handle.read(1024*1024): digest.update(chunk)
    return digest.hexdigest()

def download():
    target=ROOT/'vietnamese_ecommerce_review.csv'
    if target.exists():
        if fingerprint(target)!=SHA: raise ValueError('Existing source differs; refusing to overwrite')
        print('Existing source checksum matches.'); return
    with tempfile.TemporaryDirectory(dir=ROOT,prefix='dataset-download-') as folder:
        archive=Path(folder)/'source.zip'
        with urllib.request.urlopen(URL,timeout=120) as response,archive.open('wb') as dest: shutil.copyfileobj(response,dest)
        with zipfile.ZipFile(archive) as zipped:
            candidates=[i for i in zipped.infolist() if i.filename.endswith('.csv')]
            if len(candidates)!=1: raise ValueError('Expected one CSV')
            partial=Path(folder)/'reviews.csv'
            with zipped.open(candidates[0]) as response,partial.open('wb') as dest: shutil.copyfileobj(response,dest)
        if fingerprint(partial)!=SHA: raise ValueError('Downloaded snapshot differs from documented source; audit before accepting')
        partial.replace(target)
    print('Downloaded 642967981 bytes; SHA-256 matches documented Kaggle snapshot.')

if __name__=='__main__': download()
