import hashlib


def unique_hash(string: str, length: int = 24) -> str:
    """
    Generates an alphanumeric hash for the given string of the given length (maximum: 56)
    """
    if not 0 < length <= 56:
        raise ValueError("Length must be an integer in the interval (0, 56].")

    return hashlib.sha224(string.encode("utf8"))[:length]
