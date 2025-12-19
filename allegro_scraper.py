"""
Allegro Poland marketplace scraper
Extracts pricing, offer counts, and sales indicators for desk setup products
"""
import time
import logging
from datetime import date
from typing import Dict, Optional, List
import requests
from bs4 import BeautifulSoup
import re
from statistics import mean

import config
from db_manager import DatabaseManager

logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)


class AllegroScraper:
    """Scrapes Allegro Poland marketplace metrics"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': config.USER_AGENT,
            'Accept-Language': 'pl-PL,pl;q=0.9,en;q=0.8',
        })
    
    def _parse_price(self, price_str: str) -> Optional[float]:
        """Parse Allegro price format (e.g., '129,99 zł', '1 299,00 zł')"""
        try:
            # Remove currency and spaces
            price_str = price_str.replace('zł', '').replace('PLN', '').strip()
            # Replace comma with dot and remove spaces
            price_str = price_str.replace(',', '.').replace(' ', '')
            return float(price_str)
        except Exception as e:
            logger.warning(f"Could not parse price: {price_str} - {e}")
            return None
    
    def _extract_sales_proxy(self, listing_html: str) -> Optional[int]:
        """
        Extract sales proxy from listing
        Looks for 'kupiło X osób' (X people bought) indicator
        """
        try:
            # Pattern: "kupiło 123 osoby" or "kupiło ponad 100 osób"
            match = re.search(r'kupi[łl]o\s+(?:ponad\s+)?(\d+)\s+osób', listing_html, re.IGNORECASE)
            if match:
                return int(match.group(1))
            
            # Alternative pattern for single buyer
            if 'kupiło 1 osoba' in listing_html.lower():
                return 1
            
            return None
        except Exception as e:
            logger.warning(f"Could not extract sales proxy: {e}")
            return None
    
    def scrape_product_keyword(self, keyword: str) -> Optional[Dict]:
        """
        Scrape marketplace metrics for a product keyword
        
        Args:
            keyword: Product search keyword (e.g., 'monitor light bar')
            
        Returns:
            Dictionary with avg_price, offer_count, sales_proxy
        """
        # Build search URL for Allegro Poland
        search_url = f"{config.ALLEGRO_BASE_URL}/listing"
        params = {
            'string': keyword,
            'order': 'p',  # Sort by popularity
        }
        
        try:
            logger.info(f"Scraping Allegro for: {keyword}")
            
            response = self.session.get(
                search_url,
                params=params,
                timeout=config.REQUEST_TIMEOUT
            )
            
            if response.status_code != 200:
                logger.error(f"HTTP {response.status_code} for keyword: {keyword}")
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract listings (Allegro uses various article/div structures)
            listings = soup.find_all('article', limit=config.ALLEGRO_TOP_N_LISTINGS)
            
            if not listings:
                # Fallback: try different selectors
                listings = soup.find_all('div', {'data-box-name': 'offer'}, limit=config.ALLEGRO_TOP_N_LISTINGS)
            
            if not listings:
                logger.warning(f"No listings found for: {keyword}")
                return None
            
            prices = []
            sales_indicators = []
            
            for listing in listings:
                # Extract price
                price_elem = listing.find('span', {'class': re.compile(r'.*price.*', re.I)})
                if not price_elem:
                    price_elem = listing.find('span', string=re.compile(r'\d+[,\.]\d+\s*zł'))
                
                if price_elem:
                    price_text = price_elem.get_text(strip=True)
                    price = self._parse_price(price_text)
                    if price:
                        prices.append(price)
                
                # Extract sales proxy
                sales_proxy = self._extract_sales_proxy(str(listing))
                if sales_proxy:
                    sales_indicators.append(sales_proxy)
            
            # Calculate metrics
            avg_price = mean(prices) if prices else None
            offer_count = len(listings)
            total_sales = sum(sales_indicators) if sales_indicators else None
            
            result = {
                'avg_price': round(avg_price, 2) if avg_price else None,
                'offer_count': offer_count,
                'sales_proxy': total_sales
            }
            
            logger.info(
                f"{keyword}: {offer_count} offers, "
                f"avg price: {result['avg_price']} PLN, "
                f"sales proxy: {result['sales_proxy']}"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error scraping Allegro for '{keyword}': {e}")
            return None
    
    def scrape_all_keywords(self, save_to_db: bool = True) -> Dict[str, Optional[Dict]]:
        """
        Scrape all configured product keywords
        
        Args:
            save_to_db: Whether to save results to database
            
        Returns:
            Dictionary mapping keywords to their metrics
        """
        results = {}
        today = date.today()
        
        for keyword in config.ALLEGRO_KEYWORDS:
            metrics = self.scrape_product_keyword(keyword)
            results[keyword] = metrics
            
            if save_to_db and metrics:
                self.db.insert_marketplace_metric(
                    date_val=today,
                    keyword=keyword,
                    avg_price=metrics.get('avg_price'),
                    offer_count=metrics.get('offer_count'),
                    sales_proxy=metrics.get('sales_proxy')
                )
            
            # Rate limiting
            time.sleep(config.RATE_LIMIT_DELAY)
        
        return results


def main():
    """Run Allegro scraper as standalone script"""
    logger.info("Starting Allegro scraper")
    
    db = DatabaseManager()
    scraper = AllegroScraper(db)
    
    results = scraper.scrape_all_keywords(save_to_db=True)
    
    # Summary
    successful = sum(1 for v in results.values() if v is not None)
    logger.info(f"Scraping complete: {successful}/{len(results)} keywords successful")
    
    return results


if __name__ == "__main__":
    main()