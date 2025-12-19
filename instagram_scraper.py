"""
Instagram scraper for hashtag post counts
Uses Instagram's public web interface
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


class InstagramScraper:
    """Scrapes Instagram hashtag metrics"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': config.USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })
    
    def _extract_hashtag_data(self, html: str) -> Optional[Dict]:
        """
        Extract hashtag data from Instagram page HTML
        Instagram embeds data in shared_data script tag
        """
        try:
            # Method 1: Look for embedded JSON in script tags
            script_pattern = re.compile(
                r'window\._sharedData\s*=\s*({.+?});</script>',
                re.DOTALL
            )
            match = script_pattern.search(html)
            
            if match:
                data = json.loads(match.group(1))
                edge_hashtag = data.get('entry_data', {}).get('TagPage', [{}])[0]
                
                if edge_hashtag:
                    graphql = edge_hashtag.get('graphql', {})
                    hashtag_info = graphql.get('hashtag', {})
                    
                    post_count = hashtag_info.get('edge_hashtag_to_media', {}).get('count', 0)
                    
                    return {
                        'videos': post_count,  # Maps to 'videos' field (total posts)
                        'views': None,  # Not available for hashtags
                        'likes': None
                    }
            
            # Method 2: Look for meta tags (fallback)
            soup = BeautifulSoup(html, 'html.parser')
            
            # Try to find post count in meta tags
            meta_content = soup.find('meta', {'property': 'og:description'})
            if meta_content:
                content = meta_content.get('content', '')
                match = re.search(r'([\d,]+)\s+posts?', content, re.IGNORECASE)
                if match:
                    post_count = int(match.group(1).replace(',', ''))
                    return {
                        'videos': post_count,
                        'views': None,
                        'likes': None
                    }
            
            # Method 3: Look in page text
            post_match = re.search(r'([\d,]+)\s+posts?', html, re.IGNORECASE)
            if post_match:
                post_count = int(post_match.group(1).replace(',', ''))
                return {
                    'videos': post_count,
                    'views': None,
                    'likes': None
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error extracting Instagram data: {e}")
            return None
    
    def scrape_hashtag(self, hashtag: str) -> Optional[Dict]:
        """
        Scrape metrics for a single hashtag
        
        Args:
            hashtag: Hashtag without # symbol
            
        Returns:
            Dictionary with post count (videos field)
        """
        url = f"https://www.instagram.com/explore/tags/{hashtag}/"
        
        try:
            logger.info(f"Scraping Instagram hashtag: #{hashtag}")
            
            response = self.session.get(
                url,
                timeout=config.REQUEST_TIMEOUT,
                allow_redirects=True
            )
            
            if response.status_code == 200:
                data = self._extract_hashtag_data(response.text)
                
                if data:
                    logger.info(
                        f"#{hashtag}: {data.get('videos', 'N/A')} posts"
                    )
                    return data
                else:
                    logger.warning(f"Could not extract data for #{hashtag}")
                    return None
            elif response.status_code == 429:
                logger.error(f"Rate limited by Instagram for #{hashtag}")
                return None
            else:
                logger.error(f"HTTP {response.status_code} for #{hashtag}")
                return None
                
        except Exception as e:
            logger.error(f"Error scraping #{hashtag}: {e}")
            return None
    
    def scrape_all_hashtags(self, save_to_db: bool = True) -> Dict[str, Optional[Dict]]:
        """
        Scrape all configured Instagram hashtags
        
        Args:
            save_to_db: Whether to save results to database
            
        Returns:
            Dictionary mapping hashtags to their metrics
        """
        results = {}
        today = date.today()
        
        for hashtag in config.INSTAGRAM_HASHTAGS:
            metrics = self.scrape_hashtag(hashtag)
            results[hashtag] = metrics
            
            if save_to_db and metrics:
                self.db.insert_social_metric(
                    date_val=today,
                    platform_name='Instagram',
                    hashtag=hashtag,
                    views=metrics.get('views'),
                    videos=metrics.get('videos'),
                    likes=metrics.get('likes')
                )
            
            # Rate limiting - Instagram is more aggressive
            time.sleep(config.RATE_LIMIT_DELAY * 2)
        
        return results


def main():
    """Run Instagram scraper as standalone script"""
    logger.info("Starting Instagram scraper")
    
    db = DatabaseManager()
    scraper = InstagramScraper(db)
    
    results = scraper.scrape_all_hashtags(save_to_db=True)
    
    # Summary
    successful = sum(1 for v in results.values() if v is not None)
    logger.info(f"Scraping complete: {successful}/{len(results)} hashtags successful")
    
    return results


if __name__ == "__main__":
    main()