"""
Data deduplication utilities for food safety records.
Uses unique keys based on source and reference number.
"""

import pandas as pd
import hashlib
from typing import Set, List
from pathlib import Path
from loguru import logger


def generate_unique_key(source: str, source_reference: str) -> str:
    """
    Generate a unique key for a food safety record.
    
    Args:
        source: Data source identifier
        source_reference: Original reference number
        
    Returns:
        Unique key string
    """
    key_string = f"{source}::{source_reference}"
    return hashlib.sha256(key_string.encode()).hexdigest()[:16]


def deduplicate_records(df: pd.DataFrame, existing_keys: Set[str] = None) -> pd.DataFrame:
    """
    Remove duplicate records based on unique keys.
    
    Args:
        df: DataFrame with food safety records
        existing_keys: Set of existing record IDs to check against
        
    Returns:
        Deduplicated DataFrame
    """
    if existing_keys is None:
        existing_keys = set()
    
    # Generate unique keys for all records
    df['_unique_key'] = df.apply(
        lambda row: generate_unique_key(row['source'], row['source_reference']),
        axis=1
    )
    
    # Filter out existing records
    initial_count = len(df)
    df_new = df[~df['_unique_key'].isin(existing_keys)].copy()
    
    # Remove duplicates within the new data
    df_new = df_new.drop_duplicates(subset=['_unique_key'], keep='first')
    
    # Set record_id to unique_key
    df_new['record_id'] = df_new['_unique_key']
    df_new = df_new.drop(columns=['_unique_key'])
    
    duplicates_removed = initial_count - len(df_new)
    if duplicates_removed > 0:
        logger.info(f"Removed {duplicates_removed} duplicate records")
    
    return df_new


def load_existing_keys(data_dir: Path) -> Set[str]:
    """
    Load existing record IDs from stored Parquet files.
    
    Args:
        data_dir: Directory containing Parquet files
        
    Returns:
        Set of existing record IDs
    """
    existing_keys = set()
    
    if not data_dir.exists():
        logger.warning(f"Data directory does not exist: {data_dir}")
        return existing_keys
    
    parquet_files = list(data_dir.glob("*.parquet"))
    
    for parquet_file in parquet_files:
        try:
            df = pd.read_parquet(parquet_file, columns=['record_id'])
            existing_keys.update(df['record_id'].tolist())
            logger.info(f"Loaded {len(df)} existing keys from {parquet_file.name}")
        except Exception as e:
            logger.error(f"Error loading {parquet_file}: {e}")
    
    logger.info(f"Total existing keys loaded: {len(existing_keys)}")
    return existing_keys


def merge_and_deduplicate(new_df: pd.DataFrame, data_dir: Path) -> pd.DataFrame:
    """
    Merge new data with existing data and remove duplicates.
    
    Args:
        new_df: New DataFrame to add
        data_dir: Directory containing existing Parquet files
        
    Returns:
        Deduplicated DataFrame containing only new records
    """
    existing_keys = load_existing_keys(data_dir)
    return deduplicate_records(new_df, existing_keys)
