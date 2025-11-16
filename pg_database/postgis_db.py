import os
import psycopg2
from typing import List
from dotenv import load_dotenv
from models.dataset import Dataset
from geocoder.geocoding import BoundingBox
import logging


class PostGISService:
    """Service for managing DCAT metadata in PostGIS database."""

    def __init__(self):
        load_dotenv()
        self.user = os.getenv("POSTGRES_USER", "postgres")
        self.password = os.getenv("POSTGRES_PASSWORD", "postgres")
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = int(os.getenv("POSTGRES_PORT", "5432"))
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
            self.connection.autocommit = False
            self.logger.info("Connected to PostGIS database successfully.")
        except Exception as e:
            self.logger.error(f"Failed to connect to PostGIS database: {e}")
            raise

    def disconnect(self):
        """Close database connection."""
        if self.connection:
            try:
                self.connection.close()
                self.connection = None
                self.logger.info("Disconnected from PostGIS database.")
            except Exception as e:
                self.logger.warning(f"Error disconnecting from database: {e}")

    def initialize_schema(self):
        """Create the necessary tables and indexes."""
        if not self.connection:
            raise ValueError("No database connection established.")

        try:
            with self.connection.cursor() as cur:
                # create table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS dcat_metadata (
                        dataset_id TEXT PRIMARY KEY,
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

    def insert_dataset(self, dataset:Dataset) -> bool:
        """Insert a single dataset into the database"""

        if not self.connection:
            raise ValueError("No database connection established")
        
        if not dataset.spatial_extent_wkt:
            return False

        try:
            with self.connection.cursor() as cur:
                cur.execute("""
                    INSERT INTO dcat_metadata (dataset_id, title, geom)
                    VALUES (%s, %s, ST_GeomFromText(%s, 4326))
                    ON CONFLICT (dataset_id) DO NOTHING;
                """, (dataset.dataset_id, dataset.primary_title, dataset.spatial_extent_wkt))

                self.connection.commit()
                return True
        
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Failed to insert dataset {dataset.dataset_id}: {e}")
            return False
        
    def insert_datasets(self, datasets: List[Dataset]) -> int:
        """
        Instert multiple datasets into the database.

        Returns the number of datasets inserted
        """
        if not self.connection:
            raise ValueError("No database connection established")
        
        inserted_count = 0

        try:
            with self.connection.cursor() as cur:
                for dataset in datasets:
                    # skip datasets without spatial extent
                    if not dataset.spatial_extent_wkt:
                        self.logger.debug(f"Skipping dataset {dataset.dataset_id}: no spatial extent")
                        continue

                    try:
                        cur.execute("""
                        INSERT INTO dcat_metadata (dataset_id, title, geom)
                        VALUES (%s, %s, ST_GeomFromText(%s, 4326))
                        ON CONFLICT (dataset_id) DO NOTHING;
                        """, (dataset.dataset_id, dataset.primary_title, dataset.spatial_extent_wkt))

                        inserted_count += 1

                    except Exception as e:
                        self.logger.warning(f"Failed to insert dataset {dataset.dataset_id}: {e}")
                        continue
                # commit all insertions at once
                self.connection.commit()
                self.logger.info(f"Successfully inserted {inserted_count}/{len(datasets)} datasets")

        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error duing batch insert: {e}")
            raise

        return inserted_count

    def find_dataset_ids_by_bbox(self, bbox: BoundingBox) -> List[dict]:
        """Find all datasets that intersect with a boundingbox"""

        if not self.connection:
            raise ValueError("No database connection established")
        
        cur = self.connection.cursor()
        
        try:
            with self.connection.cursor() as cur:
                cur.execute("""
                    SELECT dataset_id
                    FROM dcat_metadata
                    WHERE ST_Intersects(
                        geom,
                        ST_MakeEnvelope(%s, %s, %s, %s, 4326)
                    );
                """, (bbox.west, bbox.south, bbox.east, bbox.north))

                results = []
                for row in cur.fetchall():
                    results.append(row[0])
                
                self.logger.info(f"Found {len(results)} datasets intersecting with bbox")
                return results
        
        except Exception as e:
            self.logger.error(f"Failed to query datasets by bbox: {e}")
            return []