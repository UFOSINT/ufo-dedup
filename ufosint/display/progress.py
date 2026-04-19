"""
Progress bar and time formatting utilities.

Usage:
    from ufosint.display.progress import progress_bar, format_eta, format_duration

    print(progress_bar(0.75))          # ████████████████████████████████░░░░░░░░░░░░
    print(format_eta(3725))            # 1h 2m
    print(format_duration(125.4))      # 2.1m
"""


def progress_bar(pct, width=44):
    """Render a progress bar string.

    Args:
        pct: float 0.0 to 1.0
        width: character width of the bar
    """
    pct = max(0.0, min(1.0, pct))
    filled = int(width * pct)
    return "\u2588" * filled + "\u2591" * (width - filled)


def format_duration(seconds):
    """Format elapsed seconds into a human-readable string."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        return f"{h}h {m}m"


def format_eta(seconds):
    """Format remaining seconds into an ETA string."""
    if seconds <= 0:
        return "done"
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        m = int(seconds // 60)
        s = int(seconds % 60)
        return f"{m}m {s}s"
    else:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        return f"{h}h {m}m"


def format_count(n):
    """Format a number with commas."""
    return f"{n:,}"


def format_pct(n, total):
    """Format n/total as a percentage string."""
    if total == 0:
        return "0.0%"
    return f"{100 * n / total:.1f}%"
