import os
import psycopg2
from typing import List, Optional
from dotenv import load_dotenv
from ..parsers.rdf_parser import RDFParser
from ..models.dataset import Dataset
import logging


class PostGISService:
    """Service for managing DCAT metadata in PostGIS database."""

    def __init__(self):
        load_dotenv()
        self.user = os.getenv("POSTGRES_USER", "postgres")
        self.password = os.getenv("POSTGRES_PASSWORD", "postgres")
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = os.getenv("POSTGRES_PORT", "5432")
        self.database = os.getenv("POSTGRES_DB", "spatial_data_search")
        self.logger = logging.getLogger(__name__)
        self.connection = None

    def connect(self):
        """Establish database connection."""
        try:
            self.connection = psycopg2.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database
            )
            self.logger.info("Connected to PostGIS database successfully.")
        except Exception as e:
            self.logger.error(f"Failed to connect to PostGIS database: {e}")
            raise
