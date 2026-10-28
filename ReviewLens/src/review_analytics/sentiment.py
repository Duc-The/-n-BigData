"""Shared model tokenization and portable inference, without Spark at serving time."""
import json
import math
import re
from pathlib import Path
from review_analytics.text import normalize_text

ALIASES = {'ko':'không','khong':'không','k':'không','hok':'không','dc':'được','đc':'được',
           'dk':'được','j':'gì','mik':'mình','mk':'mình','sp':'sản phẩm','shoppe':'shopee'}

def model_terms(text):
    value = normalize_text(text)
    value = re.sub(r'https?://\S+|www\.\S+', ' ', value)
    tokens = re.findall(r'[^\W_]+', value, flags=re.UNICODE)
    normalized = ' '.join(ALIASES.get(token, token) for token in tokens)
    tokens = [t for t in normalized.split() if len(t) >= 2]
    return tokens + [' '.join(tokens[i:i+2]) for i in range(len(tokens)-1)]

def predict(text, model):
    from collections import Counter
    counts = Counter(model_terms(text))
    values = [(i, counts[token] * model['idf'][i]) for i, token in enumerate(model['vocabulary']) if token in counts]
    norm = math.sqrt(sum(value * value for _, value in values))
    if norm == 0:
        return {'sentiment':'uncertain', 'reason':'Không có đặc trưng nằm trong từ vựng mô hình.'}
    margin = model['intercept'] + sum(model['coefficients'][i] * value / norm for i, value in values)
    result = {'sentiment':'negative' if margin > 0 else 'positive', 'margin':margin,
              'label_source':'star_rating_proxy', 'sentiment_scope':'whole_review', 'model':model['model']}
    if model['model'] == 'LogisticRegression':
        result['negative_probability'] = 1 / (1 + math.exp(-max(-700, min(700,margin))))
    return result

def analyze_review(text, model, aspects):
    result = predict(text, model)
    normalized = normalize_text(text)
    result['aspects_mentioned'] = [key for key,spec in aspects.items() if re.search(spec['pattern'],normalized)]
    result['limitations'] = 'Cảm xúc mức review học từ sao; khía cạnh là luật đề cập, chưa có polarity theo từng khía cạnh hay nhãn người kiểm chứng.'
    return result
