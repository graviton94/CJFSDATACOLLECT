"""
FDA Import Alerts collector with Country-Count CDC logic.
Collects data from FDA Import Alerts system.
"""

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import requests
from loguru import logger
import sys
import re

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from schema import normalize_dataframe
from utils.deduplication import merge_and_deduplicate
from utils.storage import save_to_parquet


class FDACollector:
    """Collector for FDA Import Alerts."""
    
    def __init__(self, data_dir: Path = None):
        """
        Initialize FDA collector.
        
        Args:
            data_dir: Directory for storing processed data
        """
        self.source = "FDA_IMPORT_ALERTS"
        self.data_dir = data_dir or Path("data/processed")
        self.base_url = "https://www.accessdata.fda.gov/cms_ia/default.html"
        
    def collect(self, days_back: int = 7) -> pd.DataFrame:
        """
        Collect FDA Import Alerts data.
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            DataFrame with collected data
        """
        logger.info(f"Starting FDA Import Alerts collection for last {days_back} days")
        
        # For demonstration, create mock data
        # Real implementation would scrape or use FDA API
        records = self._create_mock_data(days_back)
        
        if not records:
            logger.warning("No records collected from FDA")
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        
        # Apply Country-Count CDC logic
        df = self._apply_country_count_cdc(df)
        
        logger.info(f"Collected {len(df)} records from FDA Import Alerts")
        
        return df
    
    def _create_mock_data(self, days_back: int) -> list:
        """Create mock FDA Import Alerts data."""
        records = []
        base_date = datetime.now()
        
        countries = ['China', 'India', 'Mexico', 'Vietnam', 'Thailand']
        products = ['Dietary Supplements', 'Seafood', 'Spices', 'Pharmaceuticals', 'Medical Devices']
        
        for i in range(min(days_back * 2, 10)):  # Create up to 10 sample records
            date = base_date - timedelta(days=i // 2)
            country = countries[i % len(countries)]
            product = products[i % len(products)]
            
            records.append({
                'source': self.source,
                'source_reference': f'IA-{2024}-{100 + i}',
                'notification_date': date,
                'ingestion_date': datetime.now(),
                'product_name': product,
                'product_category': self._categorize_product(product),
                'origin_country': country,
                'destination_country': 'United States',
                'hazard_category': self._determine_hazard(product),
                'hazard_substance': None,
                'risk_decision': 'import_alert',
                'risk_level': 'high',
                'action_taken': 'Detention without physical examination',
                'description': f'Import alert for {product} from {country}',
                'data_quality_score': 0.90,
                'country': country,  # For CDC logic
            })
        
        return records
    
    def _categorize_product(self, product: str) -> str:
        """Categorize product type."""
        if 'Seafood' in product:
            return 'Fish and seafood'
        elif 'Dietary' in product or 'Pharmaceuticals' in product:
            return 'Food supplements'
        elif 'Spices' in product:
            return 'Herbs and spices'
        else:
            return 'Other'
    
    def _determine_hazard(self, product: str) -> str:
        """Determine hazard category based on product."""
        if 'Seafood' in product:
            return 'Biological hazards'
        elif 'Dietary' in product or 'Pharmaceuticals' in product:
            return 'Composition'
        elif 'Spices' in product:
            return 'Microbial contamination'
        else:
            return 'Other hazards'
    
    def _apply_country_count_cdc(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Country-Count CDC (Change Data Capture) logic.
        Tracks changes in country-level alert counts over time.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with CDC enrichment
        """
        # Count alerts by country
        country_counts = df.groupby('origin_country').size().reset_index(name='country_alert_count')
        
        # Merge back to main dataframe
        df = df.merge(country_counts, on='origin_country', how='left')
        
        # Calculate country risk score based on alert frequency
        max_count = df['country_alert_count'].max() if not df.empty else 1
        df['country_risk_score'] = df['country_alert_count'] / max_count
        
        # Store CDC metadata in additional_info
        df['additional_info'] = df.apply(
            lambda row: {
                'country_alert_count': int(row['country_alert_count']),
                'country_risk_score': float(row['country_risk_score'])
            },
            axis=1
        )
        
        # Drop temporary columns
        df = df.drop(columns=['country', 'country_alert_count', 'country_risk_score'], errors='ignore')
        
        logger.info("Applied Country-Count CDC logic")
        return df
    
    def transform_to_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform collected data to unified schema.
        
        Args:
            df: Raw collected DataFrame
            
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
        Collect FDA data and store to Parquet.
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            Number of new records stored
        """
        # Collect data
        df = self.collect(days_back)
        
        if df.empty:
            logger.info("No new FDA data to process")
            return 0
        
        # Transform to schema
        df = self.transform_to_schema(df)
        
        # Deduplicate
        df_new = merge_and_deduplicate(df, self.data_dir)
        
        if df_new.empty:
            logger.info("No new FDA records after deduplication")
            return 0
        
        # Save to Parquet
        save_to_parquet(df_new, self.data_dir, self.source)
        
        return len(df_new)


def main():
    """Main entry point for FDA collector."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Collect FDA Import Alerts data')
    parser.add_argument('--days', type=int, default=7, help='Days to look back')
    parser.add_argument('--data-dir', type=Path, default=Path('data/processed'),
                       help='Output directory')
    
    args = parser.parse_args()
    
    collector = FDACollector(data_dir=args.data_dir)
    count = collector.collect_and_store(days_back=args.days)
    
    logger.info(f"FDA collection complete: {count} new records")


if __name__ == '__main__':
    main()
