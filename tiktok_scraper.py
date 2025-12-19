"""
TikTok scraper for hashtag-level engagement metrics
Uses TikTok's public web interface (no authentication required)
"""
import time
import logging
from datetime import date
from typing import Dict, Optional
import requests
from bs4 import BeautifulSoup
import json
import re

import config
from db_manager import DatabaseManager

logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)


class TikTokScraper:
    """Scrapes TikTok hashtag metrics"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': config.USER_AGENT})
    
    def _extract_hashtag_data(self, html: str) -> Optional[Dict]:
        """
        Extract hashtag data from TikTok page HTML
        TikTok embeds data in __UNIVERSAL_DATA_FOR_REHYDRATION__ script tag
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Method 1: Find script tag with embedded JSON data
            script_tags = soup.find_all('script', {'id': '__UNIVERSAL_DATA_FOR_REHYDRATION__'})
            
            if script_tags:
                data = json.loads(script_tags[0].string)
                challenge_info = data.get('__DEFAULT_SCOPE__', {}).get('webapp.challenge-detail', {})
                
                if challenge_info:
                    stats = challenge_info.get('challengeInfo', {}).get('stats', {})
                    return {
                        'views': stats.get('viewCount', 0),
                        'videos': stats.get('videoCount', 0),
                        'likes': None  # Not always available at hashtag level
                    }
            
            # Method 2: Look for meta tags (fallback)
            views_match = re.search(r'(\d+\.?\d*[KMB]?)\s+views?', html, re.IGNORECASE)
            videos_match = re.search(r'(\d+\.?\d*[KMB]?)\s+videos?', html, re.IGNORECASE)
            
            if views_match or videos_match:
                return {
                    'views': self._parse_count(views_match.group(1)) if views_match else None,
                    'videos': self._parse_count(videos_match.group(1)) if videos_match else None,
                    'likes': None
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error extracting TikTok data: {e}")
            return None
    
    def _parse_count(self, count_str: str) -> int:
        """Parse TikTok count format (e.g., '1.2M', '45.3K')"""
        count_str = count_str.upper().strip()
        
        if 'B' in count_str:
            return int(float(count_str.replace('B', '')) * 1_000_000_000)
        elif 'M' in count_str:
            return int(float(count_str.replace('M', '')) * 1_000_000)
        elif 'K' in count_str:
            return int(float(count_str.replace('K', '')) * 1_000)
        else:
            return int(float(count_str))
    
    def scrape_hashtag(self, hashtag: str) -> Optional[Dict]:
        """
        Scrape metrics for a single hashtag
        
        Args:
            hashtag: Hashtag without # symbol
            
        Returns:
            Dictionary with views, videos, likes counts
        """
        url = f"https://www.tiktok.com/tag/{hashtag}"
        
        try:
            logger.info(f"Scraping TikTok hashtag: #{hashtag}")
            
            response = self.session.get(
                url,
                timeout=config.REQUEST_TIMEOUT,
                allow_redirects=True
            )
            
            if response.status_code == 200:
                data = self._extract_hashtag_data(response.text)
                
                if data:
                    logger.info(
                        f"#{hashtag}: {data.get('views', 'N/A')} views, "
                        f"{data.get('videos', 'N/A')} videos"
                    )
                    return data
                else:
                    logger.warning(f"Could not extract data for #{hashtag}")
                    return None
            else:
                logger.error(f"HTTP {response.status_code} for #{hashtag}")
                return None
                
        except Exception as e:
            logger.error(f"Error scraping #{hashtag}: {e}")
            return None
    
    def scrape_all_hashtags(self, save_to_db: bool = True) -> Dict[str, Optional[Dict]]:
        """
        Scrape all configured TikTok hashtags
        
        Args:
            save_to_db: Whether to save results to database
            
        Returns:
            Dictionary mapping hashtags to their metrics
        """
        results = {}
        today = date.today()
        
        for hashtag in config.TIKTOK_HASHTAGS:
            metrics = self.scrape_hashtag(hashtag)
            results[hashtag] = metrics
            
            if save_to_db and metrics:
                self.db.insert_social_metric(
                    date_val=today,
                    platform_name='TikTok',
                    hashtag=hashtag,
                    views=metrics.get('views'),
                    videos=metrics.get('videos'),
                    likes=metrics.get('likes')
                )
            
            # Rate limiting
            time.sleep(config.RATE_LIMIT_DELAY)
        
        return results


def main():
    """Run TikTok scraper as standalone script"""
    logger.info("Starting TikTok scraper")
    
    db = DatabaseManager()
    scraper = TikTokScraper(db)
    
    results = scraper.scrape_all_hashtags(save_to_db=True)
    
    # Summary
    successful = sum(1 for v in results.values() if v is not None)
    logger.info(f"Scraping complete: {successful}/{len(results)} hashtags successful")
    
    return results


if __name__ == "__main__":
    main()