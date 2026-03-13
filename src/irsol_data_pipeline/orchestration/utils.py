import string


def sanitize_artifact_title(title: str) -> str:
    """Sanitize a string to be used as a Prefect artifact title."""
    allowed_chars = string.ascii_lowercase + string.digits + "-"
    title = title.lower().replace("_", "-").replace("/", "-").replace(" ", "-")
    return "".join(c for c in title if c in allowed_chars)
