"""Conservative normalization: retain Vietnamese accents and negation."""
import re
import unicodedata


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    value = unicodedata.normalize("NFKC", value).lower()
    value = re.sub(r"[\u200b-\u200d\ufeff]", "", value)
    return " ".join(value.split())

