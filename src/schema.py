"""
Unified schema for food safety risk data from multiple sources.
Normalizes data from EU RASFF, FDA Import Alerts, and Korea MFDS.
Follows the 16 Standard Headers defined in SCHEMA_DOCS.md.
"""

from typing import List, Tuple, Union
from datetime import datetime
import pandas as pd
from pathlib import Path


# Unified Parquet schema definition - 16 Standard Headers
UNIFIED_SCHEMA = {
    'id': 'string',                          # Internal ID (UUID-v4)
    'ref_no': 'string',                      # Source reference number
    'source': 'string',                      # Data source (FDA, RASFF, MFDS)
    'date_registered': 'datetime64[ns]',    # Registration date
    'product_type_raw': 'string',            # Product type (original)
    'product_type': 'string',                # Product type (standardized)
    'category': 'string',                    # Category
    'product_name': 'string',                # Product name
    'origin_raw': 'string',                  # Origin country (original)
    'origin': 'string',                      # Origin country (standardized)
    'notifying_country_raw': 'string',       # Notifying country (original)
    'notifying_country': 'string',           # Notifying country (standardized)
    'hazard_reason': 'string',               # Hazard reason/test item
    'analyzable': 'bool',                    # Whether analyzable
    'hazard_category': 'string',             # Hazard category
    'tags': 'object',                        # Tags list
}


def validate_data(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate DataFrame against unified schema (16 Standard Headers).
    This is the primary validation function required by the issue specification.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []
    
    # Check required columns (all 16 are required)
    required_cols = [
        'id', 'ref_no', 'source', 'date_registered', 'product_type_raw',
        'product_type', 'category', 'product_name', 'origin_raw', 'origin',
        'notifying_country_raw', 'notifying_country', 'hazard_reason',
        'analyzable', 'hazard_category', 'tags'
    ]
    
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        errors.append(f"Missing required columns: {missing_cols}")
        return False, errors
    
    # Validate data types for present columns
    for col, expected_type in UNIFIED_SCHEMA.items():
        if col in df.columns:
            if expected_type == 'string':
                if not pd.api.types.is_string_dtype(df[col]) and not pd.api.types.is_object_dtype(df[col]):
                    errors.append(f"Column '{col}' should be string type")
            elif expected_type.startswith('datetime'):
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    errors.append(f"Column '{col}' should be datetime type")
            elif expected_type == 'bool':
                if not pd.api.types.is_bool_dtype(df[col]):
                    errors.append(f"Column '{col}' should be bool type")
            elif expected_type == 'object':
                # Tags can be object type (list)
                pass
    
    # Validate source values
    if 'source' in df.columns:
        valid_sources = {'FDA', 'RASFF', 'MFDS'}
        invalid_sources = set(df['source'].dropna().unique()) - valid_sources
        if invalid_sources:
            errors.append(f"Invalid source values: {invalid_sources}. Must be one of {valid_sources}")
    
    return len(errors) == 0, errors


def validate_schema(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Backward compatibility wrapper for validate_data.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        Tuple of (is_valid, list of error messages)
    """
    return validate_data(df)


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize DataFrame to match unified schema.
    Maps collector field names to target schema field names.
    
    Args:
        df: Input DataFrame with collector schema
        
    Returns:
        Normalized DataFrame with target schema
    """
    # Create a copy to avoid modifying the original
    df = df.copy()
    
    # Field mapping from collector schema to target schema
    field_mapping = {
        'source_reference': 'ref_no',
        'notification_date': 'date_registered',
        'product_category': 'product_type_raw',
        'origin_country': 'origin_raw',
        'destination_country': 'notifying_country_raw',
    }
    
    # Apply field mapping
    for old_name, new_name in field_mapping.items():
        if old_name in df.columns:
            df[new_name] = df[old_name]
    
    # Map hazard_category to hazard_reason if not already set
    if 'hazard_reason' not in df.columns and 'hazard_category' in df.columns:
        df['hazard_reason'] = df['hazard_category']
    
    # Categorize hazard into standard categories (미생물, 화학물질, etc.)
    # This would normally use more sophisticated logic or a lookup table
    if 'hazard_category' in df.columns:
        # Keep the original hazard_category but standardize format
        df['hazard_category'] = df['hazard_category'].apply(_standardize_hazard_category)
    
    # Set default values for missing required fields
    if 'id' not in df.columns:
        df['id'] = None  # Will be set during deduplication
    
    if 'product_type' not in df.columns:
        # Standardized product type - would normally use translation dict
        df['product_type'] = df.get('product_type_raw', None)
    
    if 'category' not in df.columns:
        df['category'] = '식품'  # Default to 'food' in Korean
    
    if 'origin' not in df.columns:
        # Standardized origin - would normally use translation dict
        df['origin'] = df.get('origin_raw', None)
    
    if 'notifying_country' not in df.columns:
        # Standardized notifying country - would normally use translation dict
        df['notifying_country'] = df.get('notifying_country_raw', None)
    
    if 'analyzable' not in df.columns:
        df['analyzable'] = True  # Default to True
    
    if 'tags' not in df.columns:
        df['tags'] = [[] for _ in range(len(df))]  # Empty list for tags
    
    # Ensure all schema columns exist with defaults
    for col in UNIFIED_SCHEMA.keys():
        if col not in df.columns:
            if col == 'tags':
                df[col] = [[] for _ in range(len(df))]
            elif col == 'analyzable':
                df[col] = True
            elif col == 'hazard_category':
                df[col] = '미상'  # Default to 'unknown' in Korean
            else:
                df[col] = None
    
    # Convert data types
    for col, dtype in UNIFIED_SCHEMA.items():
        if col not in df.columns:
            continue  # Skip columns not in DataFrame
            
        if dtype == 'string':
            df[col] = df[col].astype('string')
        elif dtype.startswith('datetime'):
            df[col] = pd.to_datetime(df[col], errors='coerce')
        elif dtype == 'bool':
            df[col] = df[col].astype('bool')
        elif dtype == 'object':
            # Keep as object (for lists, etc.)
            pass
    
    # Reorder columns to match schema and drop extra columns
    return df[list(UNIFIED_SCHEMA.keys())]


def _standardize_hazard_category(hazard: str) -> str:
    """
    Standardize hazard category to Korean categories.
    
    Args:
        hazard: Raw hazard description
        
    Returns:
        Standardized hazard category
    """
    if not hazard or pd.isna(hazard):
        return '미상'  # Unknown
    
    hazard_lower = str(hazard).lower()
    
    # Map to Korean categories
    if any(term in hazard_lower for term in ['microb', 'bacteria', 'salmonella', 'listeria', 'e. coli', 'e.coli']):
        return '미생물'  # Microbiological
    elif any(term in hazard_lower for term in ['chemical', 'pesticide', 'heavy metal', 'toxin']):
        return '화학물질'  # Chemical
    elif any(term in hazard_lower for term in ['allergen', 'allergy']):
        return '알레르기'  # Allergen
    elif any(term in hazard_lower for term in ['foreign', 'object', 'matter']):
        return '이물질'  # Foreign matter
    else:
        return '기타'  # Other
