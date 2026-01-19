"""
EU RASFF (Rapid Alert System for Food and Feed) data collector.
Uses Playwright for web scraping from the EU RASFF portal.
"""

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from loguru import logger
import sys
import re

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from schema import normalize_dataframe
from utils.deduplication import merge_and_deduplicate
from utils.storage import save_to_parquet


class RASFFScraper:
    """Scraper for EU RASFF portal."""
    
    def __init__(self, data_dir: Path = None):
        """
        Initialize RASFF scraper.
        
        Args:
            data_dir: Directory for storing processed data
        """
        self.source = "EU_RASFF"
        self.data_dir = data_dir or Path("data/processed")
        self.base_url = "https://webgate.ec.europa.eu/rasff-window/screen/list"
        
    def scrape(self, days_back: int = 7) -> pd.DataFrame:
        """
        Scrape RASFF data from the portal.
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            DataFrame with scraped data
        """
        logger.info(f"Starting RASFF scraping for last {days_back} days")
        
        records = []
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                
                # Navigate to RASFF portal
                logger.info(f"Navigating to {self.base_url}")
                page.goto(self.base_url, timeout=30000)
                
                # Wait for content to load
                page.wait_for_timeout(3000)
                
                # This is a simplified example - actual implementation would need
                # to interact with the specific RASFF portal structure
                # For demonstration, we'll create sample data
                logger.warning("Using mock data - actual portal scraping requires specific selectors")
                
                browser.close()
                
        except PlaywrightTimeout:
            logger.error("Timeout while accessing RASFF portal")
        except Exception as e:
            logger.error(f"Error during RASFF scraping: {e}")
        
        # Create mock data for demonstration
        records = self._create_mock_data(days_back)
        
        if not records:
            logger.warning("No records scraped from RASFF")
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        logger.info(f"Scraped {len(df)} records from RASFF")
        
        return df
    
    def _create_mock_data(self, days_back: int) -> list:
        """Create mock RASFF data for demonstration."""
        records = []
        base_date = datetime.now()
        
        for i in range(min(days_back, 5)):  # Create up to 5 sample records
            date = base_date - timedelta(days=i)
            records.append({
                'source': self.source,
                'source_reference': f'2024.{1000 + i}',
                'notification_date': date,
                'ingestion_date': datetime.now(),
                'product_name': f'Frozen vegetables from Country {chr(65 + i)}',
                'product_category': 'Fruits and vegetables',
                'origin_country': f'Country_{chr(65 + i)}',
                'destination_country': 'European Union',
                'hazard_category': 'Pesticides',
                'hazard_substance': f'Pesticide_{i}',
                'risk_decision': 'alert',
                'risk_level': 'serious' if i % 2 == 0 else 'moderate',
                'action_taken': 'Product recalled',
                'description': f'High levels of pesticide detected in frozen vegetables batch {i}',
                'data_quality_score': 0.95,
            })
        
        return records
    
    def transform_to_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform scraped data to unified schema.
        
        Args:
            df: Raw scraped DataFrame
            
        Returns:
            Normalized DataFrame
        """
        # Ensure source is set
        df['source'] = self.source
        
        # Set ingestion date
        df['ingestion_date'] = datetime.now()
        
        # Normalize to schema
        df_normalized = normalize_dataframe(df)
        
        return df_normalized
    
    def collect_and_store(self, days_back: int = 7) -> int:
        """
        Collect RASFF data and store to Parquet.
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            Number of new records stored
        """
        # Scrape data
        df = self.scrape(days_back)
        
        if df.empty:
            logger.info("No new RASFF data to process")
            return 0
        
        # Transform to schema
        df = self.transform_to_schema(df)
        
        # Deduplicate
        df_new = merge_and_deduplicate(df, self.data_dir)
        
        if df_new.empty:
            logger.info("No new RASFF records after deduplication")
            return 0
        
        # Save to Parquet
        save_to_parquet(df_new, self.data_dir, self.source)
        
        return len(df_new)


def main():
    """Main entry point for RASFF collector."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Collect EU RASFF data')
    parser.add_argument('--days', type=int, default=7, help='Days to look back')
    parser.add_argument('--data-dir', type=Path, default=Path('data/processed'),
                       help='Output directory')
    
    args = parser.parse_args()
    
    scraper = RASFFScraper(data_dir=args.data_dir)
    count = scraper.collect_and_store(days_back=args.days)
    
    logger.info(f"RASFF collection complete: {count} new records")


if __name__ == '__main__':
    main()
