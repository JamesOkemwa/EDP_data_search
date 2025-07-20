from typing import Optional, Dict, Any
from dataclasses import dataclass
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import logging

@dataclass
class BoundingBox:
    "Represents a geographic bounding box"
    north: float
    south: float
    east: float
    west: float

    def to_dict(self) -> Dict[str, float]:
        """Convert the bounding box to a dictionary"""
        return {
            "north": self.north,
            "south": self.south,
            "east": self.east,
            "west": self.west
        }