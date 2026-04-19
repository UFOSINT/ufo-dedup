"""
Processor registry — ordered list of analysis steps.

Usage:
    from ufosint.processors import PROCESSORS, get_processor

    # Run all in order:
    for name, cls in PROCESSORS.items():
        cls().run()

    # Run one:
    get_processor("shapes").run()

Adding a new processor = one file + one entry in PROCESSORS.
"""

from ufosint.processors.shapes import ShapeNormalizer
from ufosint.processors.movement import MovementClassifier
from ufosint.processors.colors import ColorExtractor
from ufosint.processors.sentiment import SentimentDeriver
from ufosint.processors.duration import DurationProcessor
from ufosint.processors.public_fields import PublicFieldDeriver
from ufosint.processors.quality import QualityScorer
from ufosint.processors.hoax import HoaxFlagger
from ufosint.processors.topic import TopicModeler

# Ordered — execution order matters (quality depends on shapes, movement, etc.)
PROCESSORS = {
    "shapes": ShapeNormalizer,
    "movement": MovementClassifier,
    "colors": ColorExtractor,
    "sentiment_derive": SentimentDeriver,
    "duration": DurationProcessor,
    "public_fields": PublicFieldDeriver,
    "quality": QualityScorer,
    "hoax": HoaxFlagger,
    "topic": TopicModeler,
}


def get_processor(name):
    """Get a processor instance by name.

    Raises KeyError if not found.
    """
    key = name.lower().strip()
    if key not in PROCESSORS:
        available = ", ".join(PROCESSORS.keys())
        raise KeyError(f"Unknown processor '{name}'. Available: {available}")
    return PROCESSORS[key]()