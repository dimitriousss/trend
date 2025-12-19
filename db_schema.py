"""
Database initialization script for Allegro Poland Demand Sensing System
Creates tables, indexes, and inserts seed data
"""
import sqlite3
import logging
from pathlib import Path

import config

logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)

# SQL Schema as Python string
SCHEMA_SQL = """
-- Platforms table (TikTok, Instagram, Allegro)
CREATE TABLE IF NOT EXISTS platforms (
    platform_id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Hashtags tracked for social media signals
CREATE TABLE IF NOT EXISTS hashtags (
    hashtag_id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag VARCHAR(100) NOT NULL UNIQUE,
    category VARCHAR(50) DEFAULT 'desk_setup',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Products tracked on Allegro marketplace
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword VARCHAR(200) NOT NULL UNIQUE,
    category VARCHAR(50) DEFAULT 'desk_setup',
    market VARCHAR(10) DEFAULT 'PL',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily social media metrics (TikTok, Instagram)
CREATE TABLE IF NOT EXISTS social_metrics (
    metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    platform_id INTEGER NOT NULL,
    hashtag_id INTEGER NOT NULL,
    views BIGINT,
    videos INTEGER,
    likes BIGINT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (platform_id) REFERENCES platforms(platform_id),
    FOREIGN KEY (hashtag_id) REFERENCES hashtags(hashtag_id),
    UNIQUE(date, platform_id, hashtag_id)
);

-- Daily marketplace metrics (Allegro)
CREATE TABLE IF NOT EXISTS marketplace_metrics (
    metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    product_id INTEGER NOT NULL,
    avg_price DECIMAL(10,2),
    offer_count INTEGER,
    sales_proxy INTEGER,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    UNIQUE(date, product_id)
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_social_date ON social_metrics(date);
CREATE INDEX IF NOT EXISTS idx_social_platform ON social_metrics(platform_id);
CREATE INDEX IF NOT EXISTS idx_social_hashtag ON social_metrics(hashtag_id);
CREATE INDEX IF NOT EXISTS idx_marketplace_date ON marketplace_metrics(date);
CREATE INDEX IF NOT EXISTS idx_marketplace_product ON marketplace_metrics(product_id);
"""

# Seed data
SEED_PLATFORMS = [
    (1, 'TikTok'),
    (2, 'Instagram'),
    (3, 'Allegro')
]

SEED_HASHTAGS = [
    ('desksetup', 'desk_setup'),
    ('homeoffice', 'desk_setup'),
    ('desksetupinspo', 'desk_setup'),
    ('studiosetup', 'desk_setup'),
    ('workspaceaesthetic', 'desk_setup'),
    ('gadgetsdesk', 'desk_setup'),
    ('workspaceinspo', 'desk_setup'),
    ('homeofficeideas', 'desk_setup')
]

SEED_PRODUCTS = [
    ('monitor light bar', 'desk_setup', 'PL'),
    ('rgb desk light', 'desk_setup', 'PL'),
    ('headphone stand', 'desk_setup', 'PL'),
    ('desk shelf', 'desk_setup', 'PL'),
    ('cable organizer', 'desk_setup', 'PL'),
    ('magnetic phone stand', 'desk_setup', 'PL'),
    ('LED clock', 'desk_setup', 'PL'),
    ('desk mat', 'desk_setup', 'PL')
]


def initialize_database(db_path: str = None) -> bool:
    """
    Initialize database with schema and seed data

    Args:
        db_path: Path to SQLite database file (defaults to config.DB_PATH)

    Returns:
        True if successful, False otherwise
    """
    if db_path is None:
        db_path = str(config.DB_PATH)

    try:
        # Ensure data directory exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Initializing database at: {db_path}")

        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create tables and indexes
        logger.info("Creating tables and indexes...")
        cursor.executescript(SCHEMA_SQL)

        # Insert seed data for platforms
        logger.info("Inserting platform seed data...")
        cursor.executemany(
            "INSERT OR IGNORE INTO platforms (platform_id, name) VALUES (?, ?)",
            SEED_PLATFORMS
        )

        # Insert seed data for hashtags
        logger.info("Inserting hashtag seed data...")
        cursor.executemany(
            "INSERT OR IGNORE INTO hashtags (tag, category) VALUES (?, ?)",
            SEED_HASHTAGS
        )

        # Insert seed data for products
        logger.info("Inserting product seed data...")
        cursor.executemany(
            "INSERT OR IGNORE INTO products (keyword, category, market) VALUES (?, ?, ?)",
            SEED_PRODUCTS
        )

        # Commit changes
        conn.commit()

        # Verify tables exist
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row[0] for row in cursor.fetchall()]

        logger.info(f"Database initialized successfully with tables: {', '.join(tables)}")

        # Close connection
        conn.close()

        return True

    except Exception as e:
        logger.error(f"Error initializing database: {e}", exc_info=True)
        return False


def verify_database(db_path: str = None) -> dict:
    """
    Verify database structure and seed data

    Args:
        db_path: Path to SQLite database file

    Returns:
        Dictionary with verification results
    """
    if db_path is None:
        db_path = str(config.DB_PATH)

    results = {
        'exists': False,
        'tables': [],
        'platforms_count': 0,
        'hashtags_count': 0,
        'products_count': 0
    }

    try:
        if not Path(db_path).exists():
            logger.warning(f"Database does not exist: {db_path}")
            return results

        results['exists'] = True

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Get tables
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        results['tables'] = [row[0] for row in cursor.fetchall()]

        # Count platforms
        cursor.execute("SELECT COUNT(*) FROM platforms")
        results['platforms_count'] = cursor.fetchone()[0]

        # Count hashtags
        cursor.execute("SELECT COUNT(*) FROM hashtags")
        results['hashtags_count'] = cursor.fetchone()[0]

        # Count products
        cursor.execute("SELECT COUNT(*) FROM products")
        results['products_count'] = cursor.fetchone()[0]

        conn.close()

        logger.info(f"Database verification: {results}")

    except Exception as e:
        logger.error(f"Error verifying database: {e}")

    return results


def main():
    """Run database initialization"""
    logger.info("=" * 60)
    logger.info("DATABASE INITIALIZATION")
    logger.info("=" * 60)

    # Initialize database
    success = initialize_database()

    if success:
        # Verify
        results = verify_database()

        logger.info("=" * 60)
        logger.info("VERIFICATION RESULTS")
        logger.info("=" * 60)
        logger.info(f"Database exists: {results['exists']}")
        logger.info(f"Tables: {', '.join(results['tables'])}")
        logger.info(f"Platforms: {results['platforms_count']}")
        logger.info(f"Hashtags: {results['hashtags_count']}")
        logger.info(f"Products: {results['products_count']}")
        logger.info("=" * 60)

        if results['platforms_count'] == 3 and results['hashtags_count'] == 8 and results['products_count'] == 8:
            logger.info("✅ Database initialized successfully!")
        else:
            logger.warning("⚠️ Seed data counts don't match expected values")
    else:
        logger.error("❌ Database initialization failed")


if __name__ == "__main__":
    main()