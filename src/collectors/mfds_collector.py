"""
Korea MFDS (Ministry of Food and Drug Safety) data collector.
Uses Open API to collect food safety data from Korea.
"""

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import requests
from loguru import logger
import sys
import os
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from schema import normalize_dataframe
from utils.deduplication import merge_and_deduplicate
from utils.storage import save_to_parquet


class MFDSCollector:
    """Collector for Korea MFDS Open API."""
    
    def __init__(self, api_key: Optional[str] = None, data_dir: Path = None):
        """
        Initialize MFDS collector.
        
        Args:
            api_key: MFDS Open API key (can also be set via MFDS_API_KEY env var)
            data_dir: Directory for storing processed data
        """
        self.source = "KR_MFDS"
        self.data_dir = data_dir or Path("data/processed")
        self.api_key = api_key or os.getenv('MFDS_API_KEY')
        self.base_url = "https://openapi.foodsafetykorea.go.kr/api"
        
    def collect(self, days_back: int = 7) -> pd.DataFrame:
        """
        Collect MFDS data via Open API.
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            DataFrame with collected data
        """
        logger.info(f"Starting MFDS collection for last {days_back} days")
        
        if not self.api_key:
            logger.warning("No MFDS API key provided - using mock data")
            records = self._create_mock_data(days_back)
        else:
            # Call actual API
            records = self._call_api(days_back)
        
        if not records:
            logger.warning("No records collected from MFDS")
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        logger.info(f"Collected {len(df)} records from MFDS")
        
        return df
    
    def _call_api(self, days_back: int) -> list:
        """
        Call MFDS Open API to retrieve data.
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            List of records
        """
        records = []
        
        try:
            # Example endpoint - actual endpoint would need to be configured
            # Common MFDS endpoints include:
            # - I2570: Food recall information
            # - I0488: Import food violations
            endpoint = "I2570"  # Food recall example
            
            url = f"{self.base_url}/{self.api_key}/{endpoint}/json/1/100"
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Parse API response (format depends on specific endpoint)
            # This is a simplified example
            if endpoint in data:
                items = data.get(endpoint, {}).get('row', [])
                records = self._parse_api_response(items)
            
            logger.info(f"Retrieved {len(records)} records from MFDS API")
            
        except requests.RequestException as e:
            logger.error(f"Error calling MFDS API: {e}")
        except Exception as e:
            logger.error(f"Error parsing MFDS response: {e}")
        
        return records
    
    def _parse_api_response(self, items: list) -> list:
        """Parse API response items into standardized format."""
        records = []
        
        for item in items:
            try:
                # Map API fields to our schema
                # Field names would depend on actual MFDS API response
                record = {
                    'source': self.source,
                    'source_reference': item.get('RECALL_NO', 'UNKNOWN'),
                    'notification_date': self._parse_date(item.get('RECALL_DATE')),
                    'ingestion_date': datetime.now(),
                    'product_name': item.get('PRDLST_NM', 'Unknown Product'),
                    'product_category': item.get('PRDLST_TYPE', None),
                    'origin_country': 'South Korea',
                    'destination_country': 'South Korea',
                    'hazard_category': item.get('VIOL_CONT', 'Unknown Hazard'),
                    'hazard_substance': None,
                    'risk_decision': 'recall',
                    'risk_level': self._determine_risk_level(item),
                    'action_taken': item.get('TAKE_STEP', None),
                    'description': item.get('DETAIL', None),
                    'data_quality_score': 0.92,
                }
                records.append(record)
            except Exception as e:
                logger.warning(f"Error parsing item: {e}")
                continue
        
        return records
    
    def _parse_date(self, date_str: Optional[str]) -> datetime:
        """Parse date string from API."""
        if not date_str:
            return datetime.now()
        
        try:
            # Try common Korean date formats
            for fmt in ['%Y%m%d', '%Y-%m-%d', '%Y.%m.%d']:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            return datetime.now()
        except:
            return datetime.now()
    
    def _determine_risk_level(self, item: dict) -> str:
        """Determine risk level from API data."""
        # This would be based on actual API fields
        return 'moderate'
    
    def _create_mock_data(self, days_back: int) -> list:
        """Create mock MFDS data for demonstration."""
        records = []
        base_date = datetime.now()
        
        products = [
            'Kimchi', 'Instant Noodles', 'Soy Sauce', 'Ginseng Products', 'Seaweed Snacks'
        ]
        hazards = [
            'Microbial contamination', 'Heavy metals', 'Pesticide residues',
            'Food additives', 'Allergens'
        ]
        
        for i in range(min(days_back, 5)):  # Create up to 5 sample records
            date = base_date - timedelta(days=i)
            
            records.append({
                'source': self.source,
                'source_reference': f'KR-2024-{500 + i}',
                'notification_date': date,
                'ingestion_date': datetime.now(),
                'product_name': products[i % len(products)],
                'product_category': 'Processed foods',
                'origin_country': 'South Korea',
                'destination_country': 'South Korea',
                'hazard_category': hazards[i % len(hazards)],
                'hazard_substance': None,
                'risk_decision': 'recall',
                'risk_level': 'moderate' if i % 2 == 0 else 'low',
                'action_taken': 'Product recall and disposal',
                'description': f'Voluntary recall of {products[i % len(products)]} due to {hazards[i % len(hazards)]}',
                'data_quality_score': 0.92,
            })
        
        return records
    
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
        Collect MFDS data and store to Parquet.
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            Number of new records stored
        """
        # Collect data
        df = self.collect(days_back)
        
        if df.empty:
            logger.info("No new MFDS data to process")
            return 0
        
        # Transform to schema
        df = self.transform_to_schema(df)
        
        # Deduplicate
        df_new = merge_and_deduplicate(df, self.data_dir)
        
        if df_new.empty:
            logger.info("No new MFDS records after deduplication")
            return 0
        
        # Save to Parquet
        save_to_parquet(df_new, self.data_dir, self.source)
        
        return len(df_new)


def main():
    """Main entry point for MFDS collector."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Collect Korea MFDS data')
    parser.add_argument('--days', type=int, default=7, help='Days to look back')
    parser.add_argument('--data-dir', type=Path, default=Path('data/processed'),
                       help='Output directory')
    parser.add_argument('--api-key', type=str, help='MFDS API key')
    
    args = parser.parse_args()
    
    collector = MFDSCollector(api_key=args.api_key, data_dir=args.data_dir)
    count = collector.collect_and_store(days_back=args.days)
    
    logger.info(f"MFDS collection complete: {count} new records")


if __name__ == '__main__':
    main()
