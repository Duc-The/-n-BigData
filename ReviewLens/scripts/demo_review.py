"""Predict a new review using the model exported by the HDFS/Spark job."""
import argparse
import json
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from review_analytics.sentiment import analyze_review
p=argparse.ArgumentParser()
p.add_argument('review')
p.add_argument('--model',type=Path,default=ROOT/'outputs/sentiment_model.json')
a=p.parse_args()
model=json.loads(a.model.read_text())
aspects=json.loads((ROOT/'config/aspects.json').read_text())
print(json.dumps(analyze_review(a.review,model,aspects),ensure_ascii=False,indent=2))
