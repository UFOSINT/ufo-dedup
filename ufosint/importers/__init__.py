"""
Source importer registry.

Usage:
    from ufosint.importers import get_importer, IMPORTERS

    importer = get_importer("nuforc")
    importer.run()

    # Or run all:
    for name, cls in IMPORTERS.items():
        cls().run()
"""

from ufosint.importers.nuforc import NuforcImporter
from ufosint.importers.mufon import MufonImporter
from ufosint.importers.ufocat import UfocatImporter
from ufosint.importers.updb import UpdbImporter
from ufosint.importers.geldreich import GeldreichImporter
from ufosint.importers.reddit import RedditImporter

# Ordered dict — import order matters (UFOCAT before NUFORC for enrichment)
IMPORTERS = {
    "ufocat": UfocatImporter,
    "nuforc": NuforcImporter,
    "mufon": MufonImporter,
    "updb": UpdbImporter,
    "geldreich": GeldreichImporter,
    "reddit": RedditImporter,
}

# Aliases for user convenience
IMPORT_ALIASES = {
    "ufo-search": "geldreich",
    "ufosearch": "geldreich",
    "r/ufos": "reddit",
    "rufos": "reddit",
}


def get_importer(name):
    """Get an importer instance by name (case-insensitive, with aliases).

    Args:
        name: source name (e.g., "nuforc", "mufon", "ufo-search", "reddit")

    Returns:
        Importer instance

    Raises:
        KeyError: if name not found
    """
    key = name.lower().strip()
    key = IMPORT_ALIASES.get(key, key)
    if key not in IMPORTERS:
        available = ", ".join(IMPORTERS.keys())
        raise KeyError(f"Unknown importer '{name}'. Available: {available}")
    return IMPORTERS[key]()