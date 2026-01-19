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
        self.source = "MFDS"
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
        Implements actual API calls for I0030 (Risk Information) and I2710 (Overseas Blocked Food).
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            List of records from all endpoints
        """
        records = []
        
        # Endpoints to query:
        # I0030: Risk Information Service
        # I2710: Overseas Blocked Food Import Information
        endpoints = ['I0030', 'I2710']
        
        for endpoint in endpoints:
            try:
                # MFDS API supports pagination
                page_size = 100
                start_idx = 1
                
                while True:
                    end_idx = start_idx + page_size - 1
                    
                    # Build API URL
                    # Format: {base_url}/{api_key}/{endpoint}/{data_type}/{start_idx}/{end_idx}
                    url = f"{self.base_url}/{self.api_key}/{endpoint}/json/{start_idx}/{end_idx}"
                    
                    logger.info(f"Calling MFDS API: {endpoint} (rows {start_idx}-{end_idx})")
                    
                    response = requests.get(url, timeout=30)
                    response.raise_for_status()
                    
                    data = response.json()
                    
                    # Check for API errors
                    # MFDS returns error structure like: {endpoint: {RESULT: {CODE: "ERROR-xxx"}}}
                    if endpoint in data:
                        result = data[endpoint].get('RESULT', {})
                        if result.get('CODE') and 'ERROR' in result.get('CODE', ''):
                            logger.warning(f"MFDS API returned error for {endpoint}: {result.get('MSG', 'Unknown error')}")
                            break
                        
                        # Get data rows
                        items = data[endpoint].get('row', [])
                        
                        if not items:
                            logger.info(f"No more data from {endpoint}")
                            break
                        
                        # Parse response
                        parsed_records = self._parse_api_response(items, endpoint)
                        records.extend(parsed_records)
                        
                        logger.info(f"Retrieved {len(parsed_records)} records from {endpoint} (page {start_idx}-{end_idx})")
                        
                        # Check if we got less than page_size, indicating last page
                        if len(items) < page_size:
                            break
                        
                        # Move to next page
                        start_idx = end_idx + 1
                    else:
                        logger.warning(f"Unexpected response structure from {endpoint}")
                        break
                
            except requests.RequestException as e:
                logger.error(f"Error calling MFDS API endpoint {endpoint}: {e}")
            except Exception as e:
                logger.error(f"Error parsing MFDS response for {endpoint}: {e}")
        
        return records
    
    def _parse_api_response(self, items: list, endpoint: str) -> list:
        """
        Parse API response items into standardized format.
        
        Args:
            items: List of items from API response
            endpoint: API endpoint identifier (I0030 or I2710)
            
        Returns:
            List of parsed records
        """
        records = []
        
        for item in items:
            try:
                # Field mapping varies by endpoint
                if endpoint == 'I0030':
                    # I0030: Risk Information Service
                    # Maps fields like PRDUCT (product), PRDLST_NM (product name), etc.
                    record = {
                        'source': self.source,
                        'source_reference': item.get('PRDLST_REPORT_NO', item.get('PRDLST_CD', 'UNKNOWN')),
                        'notification_date': self._parse_date(item.get('PRDLST_DCNM_DT', item.get('REG_DT'))),
                        'ingestion_date': datetime.now(),
                        'product_name': item.get('PRDUCT', item.get('PRDLST_NM', 'Unknown Product')),
                        'product_category': item.get('PRDLST_CL_NM', item.get('INDUTY_NM', None)),
                        'origin_country': item.get('ORIGIN_NM', item.get('BSSH_NM', 'South Korea')),
                        'destination_country': 'South Korea',
                        'hazard_category': item.get('VIOL_CONT', item.get('TEST_ITEM_NM', 'Unknown Hazard')),
                        'hazard_substance': item.get('TEST_ITEM_NM', None),
                        'risk_decision': 'risk_information',
                        'risk_level': self._determine_risk_level(item),
                        'action_taken': item.get('취급방법', None),
                        'description': item.get('PRDLST_NTCE_MATR', None),
                        'data_quality_score': 0.90,
                    }
                elif endpoint == 'I2710':
                    # I2710: Overseas Blocked Food Import Information
                    # Maps fields for imported food that was blocked
                    record = {
                        'source': self.source,
                        'source_reference': item.get('SEQ', item.get('NTCE_NO', 'UNKNOWN')),
                        'notification_date': self._parse_date(item.get('DSPS_DT', item.get('REG_DT'))),
                        'ingestion_date': datetime.now(),
                        'product_name': item.get('PRDLST_NM', 'Unknown Product'),
                        'product_category': item.get('PRDLST_CL_NM', None),
                        'origin_country': item.get('MNFCT_CNTRY_NM', item.get('EXPT_CNTRY_NM', 'Unknown')),
                        'destination_country': 'South Korea',
                        'hazard_category': item.get('DSPS_RSN', item.get('VIOL_CONT', 'Unknown Hazard')),
                        'hazard_substance': item.get('UNSUITABLE_ITEM_NM', None),
                        'risk_decision': 'import_blocked',
                        'risk_level': 'high',  # Blocked imports are typically high risk
                        'action_taken': item.get('DSPS_MTHD_NM', 'Import blocked'),
                        'description': item.get('VIOL_CONT', None),
                        'data_quality_score': 0.92,
                    }
                else:
                    # Fallback for unknown endpoints
                    record = {
                        'source': self.source,
                        'source_reference': item.get('SEQ', 'UNKNOWN'),
                        'notification_date': self._parse_date(item.get('REG_DT')),
                        'ingestion_date': datetime.now(),
                        'product_name': item.get('PRDLST_NM', 'Unknown Product'),
                        'product_category': None,
                        'origin_country': 'South Korea',
                        'destination_country': 'South Korea',
                        'hazard_category': 'Unknown',
                        'hazard_substance': None,
                        'risk_decision': 'unknown',
                        'risk_level': 'moderate',
                        'action_taken': None,
                        'description': None,
                        'data_quality_score': 0.70,
                    }
                
                records.append(record)
            except Exception as e:
                logger.warning(f"Error parsing item from {endpoint}: {e}")
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
