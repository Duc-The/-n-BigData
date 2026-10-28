"""Build a source-only ZIP without datasets, reports or local credentials."""
import hashlib
import json
import re
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FOLDERS = ('config', 'dags', 'dashboard', 'infra', 'jobs', 'scripts', 'src', 'tests')
FILES = ('README.md', '.env.example', '.gitignore', '.dockerignore',
         'compose.yaml', 'compose.low-memory.yaml', 'requirements-runtime.txt')
SKIP = {'__pycache__', '.pytest_cache', '.venv', 'node_modules'}
LIMIT = 20_000_000


def package():
    selected = [ROOT / name for name in FILES]
    for folder in FOLDERS:
        for path in (ROOT / folder).rglob('*'):
            if path.is_symlink():
                raise ValueError('Symbolic links are excluded')
            if path.is_file() and not SKIP.intersection(path.relative_to(ROOT).parts):
                if path.suffix not in {'.pyc', '.pyo'}:
                    selected.append(path)
    secrets = []
    if (ROOT / '.env').is_file():
        for line in (ROOT / '.env').read_text(encoding='utf-8-sig').splitlines():
            if '=' not in line or line.lstrip().startswith('#'):
                continue
            key, value = line.split('=', 1)
            value = value.strip().strip(chr(34)).strip(chr(39))
            if re.search(r'password|token|secret|(?:^|_)key(?:_|$)', key, re.I) and len(value) >= 8:
                secrets.append(value.encode())
    contents = {}
    entries = []
    for path in sorted(set(selected)):
        name = path.relative_to(ROOT).as_posix()
        if not path.is_file() or path.is_symlink():
            raise ValueError('Missing regular source file: ' + name)
        if path.name == '.env' or path.suffix.lower() in {'.pdf', '.docx', '.zip', '.sqlite', '.db', '.parquet'}:
            raise ValueError('Unexpected non-source file: ' + name)
        content = path.read_bytes()
        if any(secret in content for secret in secrets):
            raise ValueError('Local credential found; refusing archive: ' + name)
        contents[name] = content
        entries.append({'path': name, 'bytes': len(content), 'sha256': hashlib.sha256(content).hexdigest()})
    target = ROOT / 'submission'
    target.mkdir(exist_ok=True)
    archive = target / 'ReviewLens_Source.zip'
    candidate = target / 'ReviewLens_Source.zip.tmp'
    try:
        with zipfile.ZipFile(candidate, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as output:
            for name, content in contents.items():
                output.writestr('ReviewLens/' + name, content)
            output.writestr('ReviewLens/SOURCE_MANIFEST.json', json.dumps({
                'scope': 'Source only; local data, generated outputs, reports and Git excluded',
                'limit_bytes': LIMIT, 'entries': entries}, ensure_ascii=False, indent=2))
        with zipfile.ZipFile(candidate) as check:
            if check.testzip():
                raise ValueError('ZIP integrity check failed')
            for entry in entries:
                if hashlib.sha256(check.read('ReviewLens/' + entry['path'])).hexdigest() != entry['sha256']:
                    raise ValueError('Archive fingerprint mismatch')
        if candidate.stat().st_size >= LIMIT:
            raise ValueError('Source ZIP exceeds 20 MB')
        candidate.replace(archive)
    finally:
        candidate.unlink(missing_ok=True)
    result = {'archive': str(archive.relative_to(ROOT)), 'bytes': archive.stat().st_size,
              'files': len(entries) + 1, 'zip_integrity': 'passed', 'credential_scan': 'passed'}
    print(json.dumps(result))
    return result


if __name__ == '__main__':
    package()
