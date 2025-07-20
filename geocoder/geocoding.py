from typing import Optional, Dict, Optional
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
    

class GeocodingService:
    """Service for geocoding locations and retrieving bounding boxes"""
    
    def __init__(self, user_agent: str = "spatial_data_search_app", timeout: int = 3):
        """Initialize the geocoding service with a user agent and timeout"""
        self.geocoder = Nominatim(user_agent=user_agent, timeout=timeout)
        self.logger = logging.getLogger(__name__)

    def get_bounding_box(self, location_query: str, retry_attempts: int = 3) -> Optional[BoundingBox]:
        """Get the bounding box from a given location query"""
        for attempt in range(retry_attempts):
            try:
                location = self.geocoder.geocode(location_query, exactly_one=True)
                if location and location.raw.get('boundingbox'):
                    bbox = location.raw['boundingbox']
                    return BoundingBox(
                        south=float(bbox[0]),
                        north=float(bbox[1]),
                        west=float(bbox[2]),
                        east=float(bbox[3])
                    )
                else:
                    self.logger.warning(f"No bounding box found for location: {location_query}")
                    return None
            except (GeocoderTimedOut, GeocoderServiceError) as e:
                self.logger.error(f"Geocoding service error: {e}. Retrying {attempt + 1}/{retry_attempts}...")
                if attempt == retry_attempts - 1:
                    self.logger.error("Max retry attempts reached. Could not retrieve bounding box.")
                    return None
        return None