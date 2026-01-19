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

from schema import validate_schema, normalize_dataframe


def save_to_parquet(df: pd.DataFrame, output_dir: Path, source: str = None) -> Path:
    """
    Save DataFrame to Parquet file with schema validation.
    
    Args:
        df: DataFrame to save
        output_dir: Output directory
        source: Optional source identifier for filename
        
    Returns:
        Path to saved file
    """
    # Validate schema before saving
    is_valid, errors = validate_schema(df)
    if not is_valid:
        logger.error(f"Schema validation failed: {errors}")
        raise ValueError(f"Schema validation failed: {errors}")
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    source_prefix = f"{source}_" if source else ""
    filename = f"{source_prefix}{timestamp}.parquet"
    output_path = output_dir / filename
    
    # Save to Parquet
    df.to_parquet(
        output_path,
        engine='pyarrow',
        compression='snappy',
        index=False
    )
    
    logger.info(f"Saved {len(df)} records to {output_path}")
    return output_path


def load_all_data(data_dir: Path) -> pd.DataFrame:
    """
    Load all Parquet files from directory.
    
    Args:
        data_dir: Directory containing Parquet files
        
    Returns:
        Combined DataFrame
    """
    if not data_dir.exists():
        logger.warning(f"Data directory does not exist: {data_dir}")
        return pd.DataFrame()
    
    parquet_files = list(data_dir.glob("*.parquet"))
    
    if not parquet_files:
        logger.warning(f"No Parquet files found in {data_dir}")
        return pd.DataFrame()
    
    dfs = []
    for parquet_file in parquet_files:
        try:
            df = pd.read_parquet(parquet_file)
            dfs.append(df)
            logger.info(f"Loaded {len(df)} records from {parquet_file.name}")
        except Exception as e:
            logger.error(f"Error loading {parquet_file}: {e}")
    
    if not dfs:
        return pd.DataFrame()
    
    combined_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Total records loaded: {len(combined_df)}")
    
    return combined_df


def load_recent_data(data_dir: Path, days: int = 30) -> pd.DataFrame:
    """
    Load recent data from Parquet files.
    
    Args:
        data_dir: Directory containing Parquet files
        days: Number of days to look back
        
    Returns:
        DataFrame with recent records
    """
    df = load_all_data(data_dir)
    
    if df.empty:
        return df
    
    # Filter by ingestion date
    cutoff_date = datetime.now() - pd.Timedelta(days=days)
    df_recent = df[df['ingestion_date'] >= cutoff_date]
    
    logger.info(f"Filtered to {len(df_recent)} records from last {days} days")
    return df_recent
