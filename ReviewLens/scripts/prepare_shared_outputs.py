"""Run as container root: allow Spark root and Airflow group 0 to share derived exports."""
import argparse
import os
from pathlib import Path
p=argparse.ArgumentParser()
p.add_argument('--outputs',type=Path,default=Path('/exports'))
a=p.parse_args()
a.outputs.mkdir(parents=True,exist_ok=True)
for folder in [a.outputs]+[p for p in a.outputs.rglob('*') if p.is_dir()]:
    os.chown(folder,-1,0)
    folder.chmod(0o2775)
print('Derived output directories use group 0 with setgid and group write.')
