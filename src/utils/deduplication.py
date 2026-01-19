"""
Data deduplication utilities for food safety records.
Uses unique keys based on source, reference number, and date.
"""

import pandas as pd
import hashlib
from typing import Set, List, Union
from pathlib import Path
from loguru import logger


def generate_unique_key(source: str, ref_no: str, date_registered: str = None) -> str:
    """
    Generate a unique key for a food safety record.
    
    Args:
        source: Data source identifier (FDA, RASFF, MFDS)
        ref_no: Original reference number
        date_registered: Optional registration date for additional uniqueness
        
    Returns:
        Unique key string
    """
    if date_registered:
        key_string = f"{source}::{ref_no}::{date_registered}"
    else:
        key_string = f"{source}::{ref_no}"
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
        lambda row: generate_unique_key(
            row['source'], 
            row['ref_no'],
            str(row['date_registered']) if pd.notna(row['date_registered']) else None
        ),
        axis=1
    )
    
    # Filter out existing records
    initial_count = len(df)
    df_new = df[~df['_unique_key'].isin(existing_keys)].copy()
    
    # Remove duplicates within the new data
    df_new = df_new.drop_duplicates(subset=['_unique_key'], keep='first')
    
    # Set id to unique_key
    df_new['id'] = df_new['_unique_key']
    df_new = df_new.drop(columns=['_unique_key'])
    
    duplicates_removed = initial_count - len(df_new)
    if duplicates_removed > 0:
        logger.info(f"Removed {duplicates_removed} duplicate records")
    
    return df_new


def load_existing_keys(path: Union[str, Path]) -> Set[str]:
    """
    Load existing record IDs from stored Parquet file.
    
    Args:
        path: Path to Parquet file
        
    Returns:
        Set of existing record IDs
    """
    existing_keys = set()
    path_obj = Path(path)
    
    if not path_obj.exists():
        logger.info(f"No existing data file found at: {path_obj}")
        return existing_keys
    
    try:
        df = pd.read_parquet(path_obj, columns=['id'], engine='pyarrow')
        existing_keys.update(df['id'].tolist())
        logger.info(f"Loaded {len(existing_keys)} existing keys from {path_obj}")
    except Exception as e:
        logger.error(f"Error loading existing keys from {path_obj}: {e}")
    
    return existing_keys


def filter_duplicates(df: pd.DataFrame, path: Union[str, Path]) -> pd.DataFrame:
    """
    Filter out duplicates before saving to storage.
    
    Args:
        df: New DataFrame to filter
        path: Path to existing Parquet file
        
    Returns:
        Deduplicated DataFrame containing only new records
    """
    existing_keys = load_existing_keys(path)
    return deduplicate_records(df, existing_keys)


def merge_and_deduplicate(df: pd.DataFrame, data_dir: Union[str, Path]) -> pd.DataFrame:
    """
    Merge new data with existing data and remove duplicates.
    Convenience wrapper for filter_duplicates.
    
    Args:
        df: New DataFrame to merge
        data_dir: Directory containing hub_data.parquet file
        
    Returns:
        Deduplicated DataFrame containing only new records
    """
    data_dir = Path(data_dir)
    parquet_path = data_dir / 'hub_data.parquet'
    return filter_duplicates(df, parquet_path)
