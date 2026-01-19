"""
Unified schema for food safety risk data from multiple sources.
Normalizes data from EU RASFF, FDA Import Alerts, and Korea MFDS.
"""

from dataclasses import dataclass, field
from typing import Optional, List
from datetime import datetime
import pandas as pd


@dataclass
class FoodSafetyRecord:
    """Unified data model for food safety incidents."""
    
    # Unique identifier
    record_id: str
    
    # Source information
    source: str  # 'EU_RASFF', 'FDA_IMPORT_ALERTS', 'KR_MFDS'
    source_reference: str  # Original reference number from source
    
    # Temporal data
    notification_date: datetime
    ingestion_date: datetime
    
    # Product information
    product_name: str
    
    # Geographic data
    origin_country: str
    
    # Risk information
    hazard_category: str
    risk_decision: str  # e.g., 'alert', 'rejection', 'recall'
    
    # Optional fields
    product_category: Optional[str] = None
    destination_country: Optional[str] = None
    hazard_substance: Optional[str] = None
    risk_level: Optional[str] = None  # e.g., 'serious', 'moderate', 'low'
    action_taken: Optional[str] = None
    description: Optional[str] = None
    
    # Metadata
    data_quality_score: float = 1.0  # 0-1 scale
    additional_info: dict = field(default_factory=dict)


# Unified Parquet schema definition
UNIFIED_SCHEMA = {
    'record_id': 'string',
    'source': 'string',
    'source_reference': 'string',
    'notification_date': 'datetime64[ns]',
    'ingestion_date': 'datetime64[ns]',
    'product_name': 'string',
    'product_category': 'string',
    'origin_country': 'string',
    'destination_country': 'string',
    'hazard_category': 'string',
    'hazard_substance': 'string',
    'risk_decision': 'string',
    'risk_level': 'string',
    'action_taken': 'string',
    'description': 'string',
    'data_quality_score': 'float64',
}


def validate_schema(df: pd.DataFrame) -> tuple[bool, List[str]]:
    """
    Validate DataFrame against unified schema.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []
    
    # Check required columns
    required_cols = [
        'record_id', 'source', 'source_reference', 'notification_date',
        'ingestion_date', 'product_name', 'origin_country', 'hazard_category',
        'risk_decision'
    ]
    
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        errors.append(f"Missing required columns: {missing_cols}")
    
    # Validate data types for present columns
    for col, expected_type in UNIFIED_SCHEMA.items():
        if col in df.columns:
            if expected_type == 'string':
                if not pd.api.types.is_string_dtype(df[col]) and not pd.api.types.is_object_dtype(df[col]):
                    errors.append(f"Column '{col}' should be string type")
            elif expected_type.startswith('datetime'):
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    errors.append(f"Column '{col}' should be datetime type")
            elif expected_type == 'float64':
                if not pd.api.types.is_float_dtype(df[col]):
                    errors.append(f"Column '{col}' should be float type")
    
    # Validate source values
    if 'source' in df.columns:
        valid_sources = {'EU_RASFF', 'FDA_IMPORT_ALERTS', 'KR_MFDS'}
        invalid_sources = set(df['source'].unique()) - valid_sources
        if invalid_sources:
            errors.append(f"Invalid source values: {invalid_sources}")
    
    # Validate data quality score range
    if 'data_quality_score' in df.columns:
        out_of_range = df[(df['data_quality_score'] < 0) | (df['data_quality_score'] > 1)]
        if not out_of_range.empty:
            errors.append(f"data_quality_score must be between 0 and 1, found {len(out_of_range)} invalid values")
    
    return len(errors) == 0, errors


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize DataFrame to match unified schema.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Normalized DataFrame
    """
    # Ensure all schema columns exist
    for col in UNIFIED_SCHEMA.keys():
        if col not in df.columns:
            df[col] = None
    
    # Convert data types
    for col, dtype in UNIFIED_SCHEMA.items():
        if dtype == 'string':
            df[col] = df[col].astype('string')
        elif dtype.startswith('datetime'):
            df[col] = pd.to_datetime(df[col], errors='coerce')
        elif dtype == 'float64':
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Reorder columns to match schema
    return df[list(UNIFIED_SCHEMA.keys())]
