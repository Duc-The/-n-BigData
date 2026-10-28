"""Create HDFS zones and upload an immutable source, refusing a changed source."""
import argparse
import hashlib
import subprocess
import tempfile
from pathlib import Path

BASE = "/ecommerce"


def hdfs(*args, check=True, capture=False):
    return subprocess.run(["hdfs", "dfs", *args], check=check, text=True,
        stdout=subprocess.PIPE if capture else None, stderr=subprocess.PIPE if capture else None)


def prepare():
    hdfs("-mkdir", "-p", *[f"{BASE}/{zone}" for zone in
        ["raw", "processed", "output", "quarantine", "models", "checkpoints", "streaming"]])


def upload(source):
    digest = hashlib.sha256()
    with source.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    fingerprint = digest.hexdigest()
    destination = f"{BASE}/raw/reviews.csv"
    manifest = f"{BASE}/raw/source_sha256.txt"
    exists = hdfs("-test", "-e", destination, check=False).returncode == 0
    if exists:
        stored = hdfs("-cat", manifest, check=False, capture=True)
        if stored.returncode != 0 or stored.stdout.strip() != fingerprint:
            raise ValueError("HDFS raw already exists with a different or missing fingerprint; use a versioned raw path")
        print("Source already uploaded with matching SHA-256", flush=True)
        return
    hdfs("-put", str(source), destination)
    with tempfile.TemporaryDirectory(prefix="bigdata-source-") as folder:
        local_manifest = Path(folder) / "source_sha256.txt"
        local_manifest.write_text(fingerprint + "\n", encoding="utf-8")
        hdfs("-put", str(local_manifest), manifest)
    print(f"Uploaded source; SHA-256 {fingerprint}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["prepare", "upload", "all"], default="all")
    parser.add_argument("--input", type=Path, default=Path("/input/reviews.csv"))
    args = parser.parse_args()
    if args.mode in ["prepare", "all"]:
        prepare()
    if args.mode in ["upload", "all"]:
        upload(args.input)

