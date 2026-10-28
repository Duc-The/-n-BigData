import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from review_analytics.io import atomic_text_writer, atomic_write_text


class ArtifactIOTests(unittest.TestCase):
    def test_failed_write_keeps_previous_artifact_and_cleans_temporary_file(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "quality.json"
            target.write_text("previous successful result", encoding="utf-8")
            with self.assertRaises(RuntimeError):
                with atomic_text_writer(target) as handle:
                    handle.write("incomplete next result")
                    raise RuntimeError("simulated interruption before commit")
            self.assertEqual(target.read_text(), "previous successful result")
            self.assertEqual([p.name for p in Path(directory).iterdir()], ["quality.json"])

    def test_replaces_existing_unicode_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "summary.txt"
            target.write_text("previous", encoding="utf-8")
            atomic_write_text(target, "Phản hồi: không hài lòng\n")
            self.assertEqual(target.read_text(encoding="utf-8"), "Phản hồi: không hài lòng\n")

