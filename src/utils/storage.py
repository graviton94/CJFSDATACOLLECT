"""
Storage utilities for saving and loading food safety data.
Handles Parquet file operations with schema validation.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from loguru import logger
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from schema import validate_data, normalize_dataframe


def save_to_parquet(df: pd.DataFrame, path: str) -> Path:
    """
    Save DataFrame to Parquet file with schema validation and append support.
    
    Args:
        df: DataFrame to save
        path: Path to Parquet file (supports appending to existing file)
        
    Returns:
        Path to saved file
    """
    # Validate schema before saving
    is_valid, errors = validate_data(df)
    if not is_valid:
        logger.error(f"Schema validation failed: {errors}")
        raise ValueError(f"Schema validation failed: {errors}")
    
    path_obj = Path(path)
    
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


def load_parquet(path: str) -> pd.DataFrame:
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
