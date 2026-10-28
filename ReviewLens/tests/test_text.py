import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from review_analytics.text import normalize_text


class TextTests(unittest.TestCase):
    def test_preserves_negation_and_accents(self):
        self.assertEqual(normalize_text("  KHÔNG\n hài lòng\t "), "không hài lòng")

    def test_canonicalizes_combining_marks_and_styled_characters(self):
        self.assertEqual(normalize_text("a\u0301  𝑽𝒂𝒚"), "á vay")

    def test_removes_invisible_separators(self):
        self.assertEqual(normalize_text("app\u200b lỗi"), "app lỗi")


if __name__ == "__main__":
    unittest.main()

