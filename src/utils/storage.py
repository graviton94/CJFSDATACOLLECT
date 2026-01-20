"""
Storage utilities for saving and loading food safety data.
Handles Parquet file operations with schema validation.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Union
from loguru import logger
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from schema import validate_schema, UNIFIED_SCHEMA


def save_to_parquet(df: pd.DataFrame, data_dir: Union[str, Path], source: str = None) -> int:
    """
    Save DataFrame to Parquet file with schema validation and append support.
    
    Args:
        df: DataFrame to save
        data_dir: Directory path or full file path to Parquet file
        source: Optional source name (for logging purposes only, not used for file naming)
        
    Returns:
        Number of new records added
    """
    # If data_dir is a file path (ends with .parquet), use it directly
    # Otherwise, assume it's a directory and use hub_data.parquet
    path_obj = Path(data_dir)
    if path_obj.suffix != '.parquet':
        path_obj = path_obj / 'hub_data.parquet'
    
    # Validate and normalize schema before saving
    df = validate_schema(df)
    
    # Strict filtering: Only keep columns defined in UNIFIED_SCHEMA
    df = df[UNIFIED_SCHEMA].copy()
    
    # Ensure parent directory exists
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    
    # Track number of new records
    new_count = len(df)
    
    # Check if file exists for appending
    if path_obj.exists():
        try:
            # Load existing data
            existing_df = pd.read_parquet(path_obj, engine='pyarrow')
            logger.info(f"Loaded {len(existing_df)} existing records from {path_obj}")
            
            # Filter existing data to only include UNIFIED_SCHEMA columns (drop obsolete columns)
            existing_cols = [col for col in UNIFIED_SCHEMA if col in existing_df.columns]
            existing_df = existing_df[existing_cols].copy()
            
            # Append new data
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            logger.info(f"Appending {new_count} new records to existing {len(existing_df)} records")
            
            # Save combined data
            combined_df.to_parquet(
                path_obj,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            logger.info(f"Saved {len(combined_df)} total records to {path_obj}")
        except Exception as e:
            logger.error(f"Error appending to existing file: {e}")
            raise
    else:
        # Save new file
        df.to_parquet(
            path_obj,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        logger.info(f"Saved {new_count} records to new file {path_obj}")
    
    return new_count


def load_parquet(path: Union[str, Path]) -> pd.DataFrame:
    """
    Load DataFrame from Parquet file.
    
    Args:
        path: Path to Parquet file
        
    Returns:
        DataFrame loaded from file
    """
    path_obj = Path(path)
    
    if not path_obj.exists():
        logger.warning(f"Parquet file does not exist: {path_obj}")
        return pd.DataFrame()
    
    try:
        df = pd.read_parquet(path_obj, engine='pyarrow')
        logger.info(f"Loaded {len(df)} records from {path_obj}")
        return df
    except Exception as e:
        logger.error(f"Error loading {path_obj}: {e}")
        raise


def load_all_data(data_dir: Union[str, Path]) -> pd.DataFrame:
    """
    Load all data from hub_data.parquet.
    
    Args:
        data_dir: Directory containing hub_data.parquet
        
    Returns:
        DataFrame with all data (filtered to UNIFIED_SCHEMA columns only)
    """
    data_dir = Path(data_dir)
    parquet_path = data_dir / 'hub_data.parquet'
    
    if not parquet_path.exists():
        logger.warning(f"No data file found at {parquet_path}")
        return pd.DataFrame()
    
    try:
        df = pd.read_parquet(parquet_path, engine='pyarrow')
        logger.info(f"Loaded {len(df)} total records from {parquet_path}")
        
        # Strict filtering: Only return columns defined in UNIFIED_SCHEMA
        existing_cols = [col for col in UNIFIED_SCHEMA if col in df.columns]
        df = df[existing_cols].copy()
        
        return df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return pd.DataFrame()


def load_recent_data(data_dir: Union[str, Path], days: int = 30) -> pd.DataFrame:
    """
    Load data from the last N days from hub_data.parquet.
    
    Args:
        data_dir: Directory containing hub_data.parquet
        days: Number of days to load (default: 30)
        
    Returns:
        DataFrame with recent data
    """
    df = load_all_data(data_dir)
    
    if df.empty:
        return df
    
    # Filter by date_registered
    if 'date_registered' not in df.columns:
        logger.warning("date_registered column not found, returning all data")
        return df
    
    try:
        # Calculate cutoff date
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Ensure date_registered is datetime
        df['date_registered'] = pd.to_datetime(df['date_registered'])
        
        # Filter recent data
        df_recent = df[df['date_registered'] >= cutoff_date].copy()
        
        logger.info(f"Filtered to {len(df_recent)} records from last {days} days (out of {len(df)} total)")
        return df_recent
        
    except Exception as e:
        logger.error(f"Error filtering recent data: {e}")
        return df


def save_to_hub(df: pd.DataFrame, data_dir: Union[str, Path] = None) -> int:
    """
    Save DataFrame to hub with deduplication based on composite key.
    
    This function combines loading, deduplication, and saving in a single operation:
    1. Loads existing data from data/hub/hub_data.parquet (if exists)
    2. Filters out duplicate rows using composite key (data_source + source_detail + registration_date)
    3. Appends only new unique rows to existing data
    4. Saves back to hub_data.parquet with snappy compression
    5. Returns count of newly added records
    
    Args:
        df: New DataFrame to save (must follow UNIFIED_SCHEMA)
        data_dir: Directory path to hub_data.parquet (default: data/hub)
        
    Returns:
        Count of newly added records
        
    Raises:
        ValueError: If DataFrame fails schema validation
    """
    # Validate and normalize schema before processing
    df = validate_schema(df)
    
    # Set default data_dir if not provided
    if data_dir is None:
        data_dir = Path("data/hub")
    else:
        data_dir = Path(data_dir)
    
    # Ensure directory exists
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Full path to hub_data.parquet
    hub_path = data_dir / 'hub_data.parquet'
    
    # Load existing data if file exists
    existing_df = pd.DataFrame()
    
    if hub_path.exists():
        try:
            existing_df = pd.read_parquet(hub_path, engine='pyarrow')
            logger.info(f"Loaded {len(existing_df)} existing records from {hub_path}")
            
            # Filter existing data to only include UNIFIED_SCHEMA columns (drop obsolete columns)
            existing_cols = [col for col in UNIFIED_SCHEMA if col in existing_df.columns]
            existing_df = existing_df[existing_cols].copy()
        except Exception as e:
            logger.error(f"Error loading existing hub data: {e}")
            # Continue with empty existing data
    
    # Create composite key for deduplication: data_source + source_detail + registration_date
    # This ensures we don't duplicate the same record from the same source
    if not existing_df.empty:
        existing_df['_dedup_key'] = (
            existing_df['data_source'].astype(str) + '::' +
            existing_df['source_detail'].astype(str) + '::' +
            existing_df['registration_date'].astype(str)
        )
        existing_keys = set(existing_df['_dedup_key'].tolist())
        existing_df = existing_df.drop(columns=['_dedup_key'])
    else:
        existing_keys = set()
    
    # Create dedup key for new data
    df['_dedup_key'] = (
        df['data_source'].astype(str) + '::' +
        df['source_detail'].astype(str) + '::' +
        df['registration_date'].astype(str)
    )
    
    # Filter out duplicates - keep only rows with keys not in existing data
    new_records = df[~df['_dedup_key'].isin(existing_keys)].copy()
    new_records = new_records.drop(columns=['_dedup_key'])
    new_count = len(new_records)
    
    if new_count == 0:
        logger.info("No new unique records to add")
        return 0
    
    # Combine existing and new data
    if not existing_df.empty:
        combined_df = pd.concat([existing_df, new_records], ignore_index=True)
        logger.info(f"Appending {new_count} new records to existing {len(existing_df)} records")
    else:
        combined_df = new_records
        logger.info(f"Creating new hub file with {new_count} records")
    
    # Save to Parquet with snappy compression
    try:
        combined_df.to_parquet(
            hub_path,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        logger.info(f"Saved {len(combined_df)} total records to {hub_path}")
    except Exception as e:
        logger.error(f"Error saving to hub: {e}")
        raise
    
    return new_count
