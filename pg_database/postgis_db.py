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

    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.logger.info("Disconnected from PostGIS database.")

    def initialize_schema(self):
        """Create the necessary tables and indexes."""
        if not self.connection:
            raise ValueError("No database connection established.")
        
        cur = self.connection.cursor()

        try:
            # create table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dcat_metadata (
                    dataset_id UUID PRIMARY KEY,
                    title TEXT,
                    geom geometry(Polygon,4326)
                );
            """)

            # create spatial index
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_dcat_metadata_geom ON dcat_metadata USING GIST (geom);
            """)

            self.connection.commit()
            self.logger.info("Database schema initialized successfully.")

        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Failed to initialized database schema: {e}")
            raise
        
        finally:
            cur.close()