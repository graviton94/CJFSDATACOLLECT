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

from schema import validate_data, normalize_dataframe


def save_to_parquet(df: pd.DataFrame, data_dir: Union[str, Path], source: str = None) -> Path:
    """
    Save DataFrame to Parquet file with schema validation and append support.
    
    Args:
        df: DataFrame to save
        data_dir: Directory path or full file path to Parquet file
        source: Optional source name (for logging purposes only, not used for file naming)
        
    Returns:
        Path to saved file
    """
    # If data_dir is a file path (ends with .parquet), use it directly
    # Otherwise, assume it's a directory and use hub_data.parquet
    path_obj = Path(data_dir)
    if path_obj.suffix != '.parquet':
        path_obj = path_obj / 'hub_data.parquet'
    
    # Validate schema before saving
    is_valid, errors = validate_data(df)
    if not is_valid:
        logger.error(f"Schema validation failed: {errors}")
        raise ValueError(f"Schema validation failed: {errors}")
    
    # Ensure parent directory exists
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    
    # Check if file exists for appending
    if path_obj.exists():
        try:
            # Load existing data
            existing_df = pd.read_parquet(path_obj, engine='pyarrow')
            logger.info(f"Loaded {len(existing_df)} existing records from {path_obj}")
            
            # Append new data
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            logger.info(f"Appending {len(df)} new records to existing {len(existing_df)} records")
            
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
        logger.info(f"Saved {len(df)} records to new file {path_obj}")
    
    return path_obj


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
        DataFrame with all data
    """
    data_dir = Path(data_dir)
    parquet_path = data_dir / 'hub_data.parquet'
    
    if not parquet_path.exists():
        logger.warning(f"No data file found at {parquet_path}")
        return pd.DataFrame()
    
    try:
        df = pd.read_parquet(parquet_path, engine='pyarrow')
        logger.info(f"Loaded {len(df)} total records from {parquet_path}")
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
