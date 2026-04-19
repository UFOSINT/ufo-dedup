"""
Public field denormalization processor.

Copies lat/lng from location table, builds sighting_datetime,
sets has_description and has_media flags.
"""

from ufosint.processors.base import Processor


class PublicFieldDeriver(Processor):
    name = "public_fields"
    label = "Deriving public fields"

    def process(self, conn):
        import sys, os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        from analyze import derive_public_fields
        derive_public_fields(conn)
