"""
Database manager for storing scraped data
"""
import sqlite3
import logging
from datetime import date
from typing import Optional, Dict, List
from contextlib import contextmanager

import config

logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database operations for demand sensing system"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(config.DB_PATH)
        self._initialize_db()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            conn.close()
    
    def _initialize_db(self):
        """Initialize database with schema"""
        schema_path = config.BASE_DIR / "database_schema.sql"
        
        if schema_path.exists():
            with open(schema_path, 'r') as f:
                schema = f.read()
            
            with self.get_connection() as conn:
                conn.executescript(schema)
            logger.info("Database initialized successfully")
        else:
            logger.warning("Schema file not found, skipping initialization")
    
    def get_platform_id(self, platform_name: str) -> Optional[int]:
        """Get platform ID by name"""
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT platform_id FROM platforms WHERE name = ?",
                (platform_name,)
            )
            result = cursor.fetchone()
            return result['platform_id'] if result else None
    
    def get_hashtag_id(self, tag: str) -> Optional[int]:
        """Get hashtag ID by tag"""
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT hashtag_id FROM hashtags WHERE tag = ?",
                (tag,)
            )
            result = cursor.fetchone()
            return result['hashtag_id'] if result else None
    
    def get_product_id(self, keyword: str) -> Optional[int]:
        """Get product ID by keyword"""
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT product_id FROM products WHERE keyword = ?",
                (keyword,)
            )
            result = cursor.fetchone()
            return result['product_id'] if result else None
    
    def insert_social_metric(
        self,
        date_val: date,
        platform_name: str,
        hashtag: str,
        views: Optional[int] = None,
        videos: Optional[int] = None,
        likes: Optional[int] = None
    ) -> bool:
        """Insert or update social media metrics"""
        platform_id = self.get_platform_id(platform_name)
        hashtag_id = self.get_hashtag_id(hashtag)
        
        if not platform_id or not hashtag_id:
            logger.error(f"Invalid platform or hashtag: {platform_name}, {hashtag}")
            return False
        
        try:
            with self.get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO social_metrics 
                    (date, platform_id, hashtag_id, views, videos, likes)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(date, platform_id, hashtag_id) 
                    DO UPDATE SET
                        views = excluded.views,
                        videos = excluded.videos,
                        likes = excluded.likes,
                        scraped_at = CURRENT_TIMESTAMP
                    """,
                    (date_val, platform_id, hashtag_id, views, videos, likes)
                )
            logger.info(f"Inserted social metric: {platform_name}/{hashtag}/{date_val}")
            return True
        except Exception as e:
            logger.error(f"Error inserting social metric: {e}")
            return False
    
    def insert_marketplace_metric(
        self,
        date_val: date,
        keyword: str,
        avg_price: Optional[float] = None,
        offer_count: Optional[int] = None,
        sales_proxy: Optional[int] = None
    ) -> bool:
        """Insert or update marketplace metrics"""
        product_id = self.get_product_id(keyword)
        
        if not product_id:
            logger.error(f"Invalid product keyword: {keyword}")
            return False
        
        try:
            with self.get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO marketplace_metrics 
                    (date, product_id, avg_price, offer_count, sales_proxy)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(date, product_id) 
                    DO UPDATE SET
                        avg_price = excluded.avg_price,
                        offer_count = excluded.offer_count,
                        sales_proxy = excluded.sales_proxy,
                        scraped_at = CURRENT_TIMESTAMP
                    """,
                    (date_val, product_id, avg_price, offer_count, sales_proxy)
                )
            logger.info(f"Inserted marketplace metric: {keyword}/{date_val}")
            return True
        except Exception as e:
            logger.error(f"Error inserting marketplace metric: {e}")
            return False
    
    def get_social_metrics(
        self,
        start_date: date,
        end_date: date,
        platform_name: Optional[str] = None
    ) -> List[Dict]:
        """Retrieve social metrics for analysis"""
        query = """
            SELECT 
                sm.date,
                p.name as platform,
                h.tag as hashtag,
                sm.views,
                sm.videos,
                sm.likes
            FROM social_metrics sm
            JOIN platforms p ON sm.platform_id = p.platform_id
            JOIN hashtags h ON sm.hashtag_id = h.hashtag_id
            WHERE sm.date BETWEEN ? AND ?
        """
        params = [start_date, end_date]
        
        if platform_name:
            query += " AND p.name = ?"
            params.append(platform_name)
        
        query += " ORDER BY sm.date DESC, p.name, h.tag"
        
        with self.get_connection() as conn:
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_marketplace_metrics(
        self,
        start_date: date,
        end_date: date,
        keyword: Optional[str] = None
    ) -> List[Dict]:
        """Retrieve marketplace metrics for analysis"""
        query = """
            SELECT 
                mm.date,
                pr.keyword,
                mm.avg_price,
                mm.offer_count,
                mm.sales_proxy
            FROM marketplace_metrics mm
            JOIN products pr ON mm.product_id = pr.product_id
            WHERE mm.date BETWEEN ? AND ?
        """
        params = [start_date, end_date]
        
        if keyword:
            query += " AND pr.keyword = ?"
            params.append(keyword)
        
        query += " ORDER BY mm.date DESC, pr.keyword"
        
        with self.get_connection() as conn:
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]