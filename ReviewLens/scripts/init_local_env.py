"""Generate local-only credentials without printing them. Never overwrite an existing .env."""
import secrets
from pathlib import Path
path = Path(__file__).resolve().parents[1] / '.env'
if path.exists():
    print('Local .env already exists; left unchanged.')
else:
    content = ''.join(f'{key}={secrets.token_hex(24)}\n' for key in
        ['POSTGRES_PASSWORD', 'AIRFLOW_SECRET_KEY', 'AIRFLOW_ADMIN_PASSWORD'])
    with path.open('x', encoding='utf-8') as handle:
        handle.write(content)
    path.chmod(0o600)
    print('Created local .env (excluded from Git and submission).')
