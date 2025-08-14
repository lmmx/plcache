__all__ = ["_parse_size"]


def _parse_size(size: int | float | str) -> int:
    """Parse size specification into bytes.

    Examples:
        1073741824  -> 1073741824 bytes (unchanged)
        1.5         -> 1.5 bytes (converted to int)
        "1GB"       -> 1073741824 bytes
        "1.5GB"     -> 1610612736 bytes
        "500MB"     -> 524288000 bytes
        "2TB"       -> 2199023255552 bytes
    """
    if isinstance(size, (int, float)):
        return int(size)

    size = str(size).strip().upper()

    # Order matters! Check longer units first to avoid substring matching
    units = [
        ("TB", 1024**4),
        ("GB", 1024**3),
        ("MB", 1024**2),
        ("KB", 1024),
        ("B", 1),
    ]

    for unit, multiplier in units:
        if size.endswith(unit):
            number = float(size[: -len(unit)])
            return int(number * multiplier)

    # If no unit specified, assume bytes
    return int(float(size))
