"""
Daily batch job runner for Allegro Poland Demand Sensing System
Orchestrates all scrapers and handles errors gracefully
"""
import logging
from datetime import datetime, date
from typing import Dict, Any
import sys

import config
from db_manager import DatabaseManager
from tiktok_scraper import TikTokScraper
from instagram_scraper import InstagramScraper
from allegro_scraper import AllegroScraper

# Setup logging
log_file = config.LOGS_DIR / f"batch_{date.today().isoformat()}.log"
logging.basicConfig(
    level=config.LOG_LEVEL,
    format=config.LOG_FORMAT,
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class DailyBatchRunner:
    """Orchestrates daily data collection from all sources"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.tiktok_scraper = TikTokScraper(self.db)
        self.instagram_scraper = InstagramScraper(self.db)
        self.allegro_scraper = AllegroScraper(self.db)
        
        self.results = {
            'start_time': datetime.now(),
            'tiktok': None,
            'instagram': None,
            'allegro': None,
            'errors': []
        }
    
    def run_tiktok(self) -> bool:
        """Run TikTok scraper"""
        try:
            logger.info("=" * 60)
            logger.info("STARTING TIKTOK SCRAPER")
            logger.info("=" * 60)
            
            results = self.tiktok_scraper.scrape_all_hashtags(save_to_db=True)
            
            successful = sum(1 for v in results.values() if v is not None)
            total = len(results)
            
            self.results['tiktok'] = {
                'successful': successful,
                'total': total,
                'success_rate': successful / total if total > 0 else 0
            }
            
            logger.info(f"TikTok scraping complete: {successful}/{total} successful")
            return successful > 0
            
        except Exception as e:
            logger.error(f"TikTok scraper failed: {e}", exc_info=True)
            self.results['errors'].append(f"TikTok: {str(e)}")
            return False
    
    def run_instagram(self) -> bool:
        """Run Instagram scraper"""
        try:
            logger.info("=" * 60)
            logger.info("STARTING INSTAGRAM SCRAPER")
            logger.info("=" * 60)
            
            results = self.instagram_scraper.scrape_all_hashtags(save_to_db=True)
            
            successful = sum(1 for v in results.values() if v is not None)
            total = len(results)
            
            self.results['instagram'] = {
                'successful': successful,
                'total': total,
                'success_rate': successful / total if total > 0 else 0
            }
            
            logger.info(f"Instagram scraping complete: {successful}/{total} successful")
            return successful > 0
            
        except Exception as e:
            logger.error(f"Instagram scraper failed: {e}", exc_info=True)
            self.results['errors'].append(f"Instagram: {str(e)}")
            return False
    
    def run_allegro(self) -> bool:
        """Run Allegro scraper"""
        try:
            logger.info("=" * 60)
            logger.info("STARTING ALLEGRO SCRAPER")
            logger.info("=" * 60)
            
            results = self.allegro_scraper.scrape_all_keywords(save_to_db=True)
            
            successful = sum(1 for v in results.values() if v is not None)
            total = len(results)
            
            self.results['allegro'] = {
                'successful': successful,
                'total': total,
                'success_rate': successful / total if total > 0 else 0
            }
            
            logger.info(f"Allegro scraping complete: {successful}/{total} successful")
            return successful > 0
            
        except Exception as e:
            logger.error(f"Allegro scraper failed: {e}", exc_info=True)
            self.results['errors'].append(f"Allegro: {str(e)}")
            return False
    
    def run_all(self) -> Dict[str, Any]:
        """Run all scrapers in sequence"""
        logger.info("=" * 60)
        logger.info(f"DAILY BATCH STARTED: {self.results['start_time']}")
        logger.info("=" * 60)
        
        # Run all scrapers (continue even if one fails)
        tiktok_ok = self.run_tiktok()
        instagram_ok = self.run_instagram()
        allegro_ok = self.run_allegro()
        
        # Calculate summary
        self.results['end_time'] = datetime.now()
        self.results['duration'] = (
            self.results['end_time'] - self.results['start_time']
        ).total_seconds()
        
        self.results['overall_success'] = all([tiktok_ok, instagram_ok, allegro_ok])
        
        # Log summary
        logger.info("=" * 60)
        logger.info("BATCH SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Duration: {self.results['duration']:.2f} seconds")
        logger.info(f"TikTok: {self.results['tiktok']}")
        logger.info(f"Instagram: {self.results['instagram']}")
        logger.info(f"Allegro: {self.results['allegro']}")
        
        if self.results['errors']:
            logger.error(f"Errors encountered: {len(self.results['errors'])}")
            for error in self.results['errors']:
                logger.error(f"  - {error}")
        
        logger.info(f"Overall success: {self.results['overall_success']}")
        logger.info("=" * 60)
        
        return self.results


def main():
    """Main entry point for daily batch job"""
    runner = DailyBatchRunner()
    results = runner.run_all()
    
    # Exit with appropriate code for automation tools
    sys.exit(0 if results['overall_success'] else 1)


if __name__ == "__main__":
    main()