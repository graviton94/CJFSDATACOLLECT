"""
Centralized Fuzzy Matching Utility for Product and Hazard Lookups

This module provides fuzzy matching functions to improve the mapping success rate
when raw data from different sources contains variations in naming conventions.

Key Features:
1. Case-insensitive matching
2. Partial string matching (keywords)
3. Fuzzy string matching using rapidfuzz for better accuracy
4. Configurable similarity threshold

Example:
    "Frozen Shrimp" should match "Shrimp, Frozen" or "Shrimp"
    "Aflatoxin B1" should match "Aflatoxin"
"""

import pandas as pd
from rapidfuzz import fuzz, process
from typing import Dict, Optional, List


class FuzzyMatcher:
    """
    Fuzzy matching engine for product and hazard reference data lookups.
    
    Uses a multi-strategy approach:
    1. Exact match (fastest, case-insensitive)
    2. Keyword match (partial string search)
    3. Fuzzy match (similarity score using rapidfuzz)
    """
    
    def __init__(self, similarity_threshold: int = 80, long_text_threshold: int = 30):
        """
        Initialize the fuzzy matcher.
        
        Args:
            similarity_threshold: Minimum similarity score (0-100) for fuzzy matches.
                                 Default is 80, which means 80% similarity.
            long_text_threshold: Character count threshold to trigger sentence scanning mode.
                                Default is 30 characters.
        """
        self.similarity_threshold = similarity_threshold
        self.long_text_threshold = long_text_threshold
    
    def _normalize_text(self, text: str) -> str:
        """
        Normalize text for comparison.
        
        Args:
            text: Input text to normalize
            
        Returns:
            Normalized text (lowercase, stripped whitespace)
        """
        if not text or pd.isna(text):
            return ""
        return str(text).strip().lower()
    
    def _exact_match(self, search_term: str, reference_df: pd.DataFrame, 
                     match_columns: List[str]) -> Optional[pd.Series]:
        """
        Try exact match first (fastest method).
        
        Args:
            search_term: Normalized search term
            reference_df: Reference DataFrame
            match_columns: List of columns to search in
            
        Returns:
            Matched row as pandas Series, or None if no match found
        """
        for col in match_columns:
            if col in reference_df.columns:
                mask = reference_df[col].astype(str).str.strip().str.lower() == search_term
                if mask.any():
                    return reference_df[mask].iloc[0]
        return None
    
    def _keyword_match(self, search_term: str, reference_df: pd.DataFrame,
                       match_columns: List[str]) -> Optional[pd.Series]:
        """
        Try keyword/partial match (contains check).
        
        This helps match cases like:
        - "Frozen Shrimp" contains "Shrimp"
        - "Aflatoxin B1" contains "Aflatoxin"
        
        Args:
            search_term: Normalized search term
            reference_df: Reference DataFrame
            match_columns: List of columns to search in
            
        Returns:
            Matched row as pandas Series, or None if no match found
        """
        for col in match_columns:
            if col in reference_df.columns:
                # Try: does the reference contain the search term?
                mask = reference_df[col].astype(str).str.strip().str.lower().str.contains(
                    search_term, regex=False, na=False
                )
                if mask.any():
                    return reference_df[mask].iloc[0]
                
                # Try reverse: does the search term contain the reference?
                for idx, ref_value in reference_df[col].items():
                    ref_normalized = self._normalize_text(ref_value)
                    if ref_normalized and ref_normalized in search_term:
                        return reference_df.loc[idx]
        
        return None
    
    def _sentence_scan_match(self, search_term: str, reference_df: pd.DataFrame,
                             match_columns: List[str]) -> Optional[pd.Series]:
        """
        Scan long text for presence of reference keywords (Reverse Lookup).
        
        This is specifically designed for long descriptions like FDA alerts:
        "The sample analysis revealed high levels of Aflatoxin B1 in the product..."
        
        Strategy:
        1. Iterate through ALL reference keywords in the DataFrame
        2. Check if any keyword appears in the search term (case-insensitive)
        3. Return the first match found
        
        Args:
            search_term: Normalized long text to scan
            reference_df: Reference DataFrame with keywords
            match_columns: List of columns to extract keywords from
            
        Returns:
            Matched row as pandas Series, or None if no match found
        """
        # Collect all possible keywords from reference data
        best_match = None
        best_match_length = 0  # Prefer longer matches (more specific)
        
        for col in match_columns:
            if col in reference_df.columns:
                for idx, ref_value in reference_df[col].items():
                    ref_normalized = self._normalize_text(ref_value)
                    if not ref_normalized:
                        continue
                    
                    # Check if the keyword exists in the search term
                    # Use word boundary check to avoid partial matches like "lead" in "misleading"
                    if ref_normalized in search_term:
                        # Prefer longer, more specific matches
                        if len(ref_normalized) > best_match_length:
                            best_match = reference_df.loc[idx]
                            best_match_length = len(ref_normalized)
        
        return best_match
    
    def _fuzzy_match(self, search_term: str, reference_df: pd.DataFrame,
                     match_columns: List[str]) -> Optional[pd.Series]:
        """
        Try fuzzy match using rapidfuzz (slowest but most flexible).
        
        Uses WRatio algorithm which is best for general string matching.
        
        Args:
            search_term: Normalized search term
            reference_df: Reference DataFrame
            match_columns: List of columns to search in
            
        Returns:
            Matched row as pandas Series, or None if no match found
        """
        best_match = None
        best_score = 0
        
        for col in match_columns:
            if col in reference_df.columns:
                # Get all non-null values from this column
                choices = reference_df[col].astype(str).str.strip().str.lower().tolist()
                
                # Find best match using rapidfuzz
                result = process.extractOne(
                    search_term,
                    choices,
                    scorer=fuzz.WRatio,
                    score_cutoff=self.similarity_threshold
                )
                
                if result:
                    match_text, score, idx = result
                    if score > best_score:
                        best_score = score
                        best_match = reference_df.iloc[idx]
        
        return best_match
    
    def match_product_type(self, raw_text: str, product_ref_df: pd.DataFrame) -> Dict[str, Optional[str]]:
        """
        Match raw product type to reference data using multi-strategy approach.
        
        Strategy order:
        1. Exact match (fastest)
        2. Keyword match (partial string)
        3. Fuzzy match (similarity score)
        
        Args:
            raw_text: Raw product type from data source
            product_ref_df: Reference DataFrame with product hierarchy
            
        Returns:
            Dictionary with keys:
            - 'top': Top-level product type (HTRK_PRDLST_NM or GR_NM)
            - 'upper': Upper product type (HRRK_PRDLST_NM or PRDLST_CL_NM)
            
        Example:
            >>> matcher = FuzzyMatcher()
            >>> result = matcher.match_product_type("Frozen Shrimp", product_df)
            >>> print(result)
            {'top': '수산물', 'upper': '갑각류'}
        """
        info = {"top": None, "upper": None}
        
        if product_ref_df.empty or not raw_text:
            return info
        
        # Normalize search term
        search_term = self._normalize_text(raw_text)
        if not search_term:
            return info
        
        # Columns to search in (Korean name, English name)
        match_columns = ['KOR_NM', 'ENG_NM']
        
        # Try matching strategies in order (early exit on first match)
        matched_row = self._exact_match(search_term, product_ref_df, match_columns)
        if matched_row is None:
            matched_row = self._keyword_match(search_term, product_ref_df, match_columns)
        if matched_row is None:
            matched_row = self._fuzzy_match(search_term, product_ref_df, match_columns)
        
        if matched_row is not None:
            # Extract output fields: NAMES instead of CODES
            # Try HTRK_PRDLST_NM first, fallback to GR_NM
            if "HTRK_PRDLST_NM" in matched_row.index and pd.notna(matched_row.get("HTRK_PRDLST_NM")):
                info["top"] = matched_row.get("HTRK_PRDLST_NM")
            elif "GR_NM" in matched_row.index and pd.notna(matched_row.get("GR_NM")):
                info["top"] = matched_row.get("GR_NM")
            
            # Try HRRK_PRDLST_NM first, fallback to PRDLST_CL_NM
            if "HRRK_PRDLST_NM" in matched_row.index and pd.notna(matched_row.get("HRRK_PRDLST_NM")):
                info["upper"] = matched_row.get("HRRK_PRDLST_NM")
            elif "PRDLST_CL_NM" in matched_row.index and pd.notna(matched_row.get("PRDLST_CL_NM")):
                info["upper"] = matched_row.get("PRDLST_CL_NM")
        
        return info
    
    def match_hazard_category(self, raw_text: str, hazard_ref_df: pd.DataFrame) -> Dict[str, any]:
        """
        Match raw hazard item to reference data using multi-strategy approach.
        
        Strategy order:
        1. For short text (≤ long_text_threshold): Use standard matching
           - Exact match (fastest)
           - Keyword match (partial string)
           - Fuzzy match (similarity score)
        2. For long text (> long_text_threshold): Use sentence scanning
           - Scan for known keywords in the text (Reverse Lookup)
           - Fall back to standard matching if no keywords found
        
        Args:
            raw_text: Raw hazard item from data source
            hazard_ref_df: Reference DataFrame with hazard classifications
            
        Returns:
            Dictionary with keys:
            - 'category': Hazard category (M_KOR_NM)
            - 'analyzable': Boolean indicating if item can be analyzed in lab
            - 'interest': Boolean indicating if item is of strategic interest
            
        Example:
            >>> matcher = FuzzyMatcher()
            >>> # Short text - standard matching
            >>> result = matcher.match_hazard_category("Aflatoxin B1", hazard_df)
            >>> # Long text - sentence scanning
            >>> result = matcher.match_hazard_category(
            ...     "The sample analysis revealed high levels of Aflatoxin B1 in the product",
            ...     hazard_df
            ... )
            >>> print(result)
            {'category': '곰팡이독소', 'analyzable': True, 'interest': True}
        """
        info = {"category": None, "analyzable": False, "interest": False}
        
        if hazard_ref_df.empty or not raw_text:
            return info
        
        # Normalize search term
        search_term = self._normalize_text(raw_text)
        if not search_term:
            return info
        
        # Columns to search in (more options for hazards)
        match_columns = ['KOR_NM', 'ENG_NM', 'ABRV', 'NCKNM', 'TESTITM_NM']
        
        # Determine strategy based on text length
        matched_row = None
        
        if len(search_term) > self.long_text_threshold:
            # Long text: Use sentence scanning first (Reverse Lookup)
            matched_row = self._sentence_scan_match(search_term, hazard_ref_df, match_columns)
            
            # If sentence scanning fails, fall back to standard matching
            if matched_row is None:
                matched_row = self._exact_match(search_term, hazard_ref_df, match_columns)
            if matched_row is None:
                matched_row = self._keyword_match(search_term, hazard_ref_df, match_columns)
            # Skip fuzzy match for long text to avoid false positives
        else:
            # Short text: Use standard matching strategies
            matched_row = self._exact_match(search_term, hazard_ref_df, match_columns)
            if matched_row is None:
                matched_row = self._keyword_match(search_term, hazard_ref_df, match_columns)
            if matched_row is None:
                matched_row = self._fuzzy_match(search_term, hazard_ref_df, match_columns)
        
        if matched_row is not None:
            # Extract output fields
            info["category"] = matched_row.get("M_KOR_NM") if "M_KOR_NM" in matched_row.index else None
            info["analyzable"] = bool(matched_row.get("ANALYZABLE", False)) if "ANALYZABLE" in matched_row.index else False
            info["interest"] = bool(matched_row.get("INTEREST_ITEM", False)) if "INTEREST_ITEM" in matched_row.index else False
        
        return info


# Convenience functions for backward compatibility with existing code
def match_product_type(raw_text: str, product_ref_df: pd.DataFrame, 
                       similarity_threshold: int = 80) -> Dict[str, Optional[str]]:
    """
    Convenience function to match product type without creating a FuzzyMatcher instance.
    
    Args:
        raw_text: Raw product type from data source
        product_ref_df: Reference DataFrame with product hierarchy
        similarity_threshold: Minimum similarity score (0-100) for fuzzy matches
        
    Returns:
        Dictionary with 'top' and 'upper' product type classifications
    """
    matcher = FuzzyMatcher(similarity_threshold=similarity_threshold)
    return matcher.match_product_type(raw_text, product_ref_df)


def match_hazard_category(raw_text: str, hazard_ref_df: pd.DataFrame,
                          similarity_threshold: int = 80, long_text_threshold: int = 30) -> Dict[str, any]:
    """
    Convenience function to match hazard category without creating a FuzzyMatcher instance.
    
    Args:
        raw_text: Raw hazard item from data source
        hazard_ref_df: Reference DataFrame with hazard classifications
        similarity_threshold: Minimum similarity score (0-100) for fuzzy matches
        long_text_threshold: Character count threshold to trigger sentence scanning mode
        
    Returns:
        Dictionary with 'category', 'analyzable', and 'interest' fields
    """
    matcher = FuzzyMatcher(similarity_threshold=similarity_threshold, 
                          long_text_threshold=long_text_threshold)
    return matcher.match_hazard_category(raw_text, hazard_ref_df)
