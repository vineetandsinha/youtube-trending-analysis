import os
import json
import logging
from datetime import datetime
import pandas as pd

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    BigInteger,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError, OperationalError

# ------------------------------------------------------------------------------
# Config & Logging
# ------------------------------------------------------------------------------

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://vineetsinha@localhost:5432/youtube_db"
)

RAW_DATA_DIR = "data/raw"

# Enhanced Logging: Outputs to both Console and a dedicated Log File
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("ingestion.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("DatabaseLoader")
Base = declarative_base()

# ------------------------------------------------------------------------------
# ORM Models
# ------------------------------------------------------------------------------

class Category(Base):
    __tablename__ = "categories"
    id = Column(String, primary_key=True)
    category_title = Column(String, nullable=False, unique=True)
    videos = relationship("YouTubeVideo", back_populates="category_rel")

class YouTubeVideo(Base):
    __tablename__ = "videos"
    __table_args__ = (
        UniqueConstraint("video_id", "trending_date", name="uq_video_trending_day"),
    )
    id = Column(Integer, primary_key=True)
    video_id = Column(String, nullable=False)
    trending_date = Column(DateTime, nullable=False)
    title = Column(String)
    channel_title = Column(String)
    category_id = Column(String, ForeignKey("categories.id"))
    publish_time = Column(DateTime)
    views = Column(BigInteger)
    likes = Column(BigInteger)
    dislikes = Column(BigInteger)
    comment_count = Column(BigInteger)

    category_rel = relationship("Category", back_populates="videos")

# ------------------------------------------------------------------------------
# Ingestion Logic with Exception Handling
# ------------------------------------------------------------------------------

def load_categories(session):
    try:
        json_path = os.path.join(RAW_DATA_DIR, "GB_category_id.json")
        logger.info(f"Opening category file: {json_path}")
        
        with open(json_path) as f:
            raw = json.load(f)

        records = [
            {"id": item["id"], "category_title": item["snippet"]["title"]}
            for item in raw["items"]
        ]

        stmt = insert(Category).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=["id"])
        session.execute(stmt)
        logger.info(f"Successfully Inserted {len(records)} categories.")
        
    except FileNotFoundError:
        logger.error("Category JSON file not found. Skipping category load.")
        raise
    except json.JSONDecodeError:
        logger.error("Failed to decode Category JSON. Check for file corruption.")
        raise
    except SQLAlchemyError as e:
        logger.error(f"Database error during category load: {e}")
        raise

def load_videos(session):
    try:
        csv_path = os.path.join(RAW_DATA_DIR, "GBvideos.csv")
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Missing CSV: {csv_path}")

        logger.info("Reading video CSV...")
        df = pd.read_csv(csv_path)

        # Pre-processing
        df["category_id"] = df["category_id"].astype(str)
        df["trending_date"] = pd.to_datetime(df["trending_date"], format="%y.%d.%m")
        df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")

        # Incremental Load Logic
        last_date = session.execute(text("SELECT max(trending_date) FROM videos")).scalar()
        if last_date:
            df = df[df["trending_date"] > last_date]
            logger.info(f"Filtering for new data since {last_date}")

        if df.empty:
            logger.info("No new records to ingest.")
            return

        # Referential Integrity Check (Filter out IDs not in 'categories' table)
        valid_categories = {c[0] for c in session.execute(text("SELECT id FROM categories"))}
        df_filtered = df[df["category_id"].isin(valid_categories)].copy()
        
        dropped_count = len(df) - len(df_filtered)
        if dropped_count > 0:
            logger.warning(f"Dropped {dropped_count} rows due to missing Foreign Key (Category ID).")

        records = df_filtered[[
            "video_id", "trending_date", "title", "channel_title",
            "category_id", "publish_time", "views", "likes",
            "dislikes", "comment_count"
        ]].to_dict(orient="records")

        # Bulk Upsert
        stmt = insert(YouTubeVideo).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["video_id", "trending_date"],
            set_={
                "views": stmt.excluded.views,
                "likes": stmt.excluded.likes,
                "dislikes": stmt.excluded.dislikes,
                "comment_count": stmt.excluded.comment_count,
                "title": stmt.excluded.title # In case the title changed
            }
        )
        session.execute(stmt)
        logger.info(f"Successfully ingested {len(records)} video records.")

    except Exception as e:
        logger.error(f"Error during video ingestion: {e}")
        raise

def run_pipeline():
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(bind=engine)
    
    try:
        logger.info("Verifying database connection and schema...")
        Base.metadata.create_all(engine)
        
        with SessionLocal.begin() as session:
            load_categories(session)
            load_videos(session)
            
        logger.info("Pipeline completed successfully.")
        
    except OperationalError:
        logger.critical("Could not connect to PostgreSQL. Is the service running?")
    except Exception as e:
        logger.critical(f"Pipeline failed: {e}")

if __name__ == "__main__":
    run_pipeline()