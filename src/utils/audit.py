"""
Data Quality Audit Script for hub_data.parquet

This script performs comprehensive health checks on the unified food safety database:
1. Schema validation against UNIFIED_SCHEMA
2. Missing value analysis for all 13 columns
3. Mapping failure detection (raw data exists but derived data missing)
4. Formatted reporting using tabulate

Usage:
    python src/utils/audit.py
"""

import pandas as pd
from pathlib import Path
from tabulate import tabulate
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from schema import UNIFIED_SCHEMA


class DataQualityAuditor:
    """Audit tool for validating data quality in hub_data.parquet"""
    
    def __init__(self, parquet_path: Path = None):
        """
        Initialize the auditor.
        
        Args:
            parquet_path: Path to hub_data.parquet (defaults to data/hub_data.parquet)
        """
        if parquet_path is None:
            # Default to data/hub_data.parquet from project root
            project_root = Path(__file__).parent.parent.parent
            parquet_path = project_root / "data" / "hub_data.parquet"
        
        self.parquet_path = Path(parquet_path)
        self.df = None
        self.total_rows = 0
        
    def load_data(self):
        """Load the parquet file and verify it exists"""
        if not self.parquet_path.exists():
            raise FileNotFoundError(
                f"❌ Parquet file not found: {self.parquet_path}\n"
                f"Please run data collectors first to generate the file."
            )
        
        print(f"📂 Loading data from: {self.parquet_path}")
        self.df = pd.read_parquet(self.parquet_path)
        self.total_rows = len(self.df)
        print(f"✅ Loaded {self.total_rows:,} rows\n")
        
    def verify_schema(self):
        """Verify that DataFrame columns match UNIFIED_SCHEMA exactly"""
        print("🔍 Schema Validation")
        print("=" * 60)
        
        df_columns = set(self.df.columns)
        expected_columns = set(UNIFIED_SCHEMA)
        
        # Check for missing columns
        missing_cols = expected_columns - df_columns
        if missing_cols:
            print(f"❌ Missing columns: {missing_cols}")
            return False
        
        # Check for extra columns
        extra_cols = df_columns - expected_columns
        if extra_cols:
            print(f"⚠️  Extra columns (not in UNIFIED_SCHEMA): {extra_cols}")
        
        # Check column order
        if list(self.df.columns) != UNIFIED_SCHEMA:
            print(f"⚠️  Column order differs from UNIFIED_SCHEMA")
        
        if not missing_cols and not extra_cols:
            print(f"✅ Schema validation passed: All {len(UNIFIED_SCHEMA)} columns present")
        
        print()
        return True
        
    def analyze_missing_values(self):
        """Calculate and display missing value statistics for each column"""
        print("📊 Missing Value Analysis")
        print("=" * 60)
        
        results = []
        
        for col in UNIFIED_SCHEMA:
            if col not in self.df.columns:
                results.append({
                    'Column': col,
                    'Missing Count': 'N/A',
                    'Missing %': 'N/A',
                    'Status': '❌ Missing'
                })
                continue
            
            # Count missing values (None, NaN, empty string)
            missing_mask = (
                self.df[col].isna() | 
                (self.df[col] == "") | 
                (self.df[col].astype(str) == "None")
            )
            
            # Special handling for boolean columns
            if col in ['analyzable', 'interest_item']:
                missing_mask = self.df[col].isna()
            
            missing_count = missing_mask.sum()
            missing_pct = (missing_count / self.total_rows * 100) if self.total_rows > 0 else 0
            
            # Determine status
            if missing_pct == 0:
                status = "✅ Perfect"
            elif missing_pct < 10:
                status = "⚠️  Minor"
            elif missing_pct < 50:
                status = "⚠️  Moderate"
            else:
                status = "❌ Critical"
            
            results.append({
                'Column': col,
                'Missing Count': f"{missing_count:,}",
                'Missing %': f"{missing_pct:.2f}%",
                'Status': status
            })
        
        # Print table
        print(tabulate(results, headers='keys', tablefmt='grid'))
        print()
        
        return results
    
    def detect_mapping_failures(self):
        """
        Detect rows where raw data exists but derived data is missing.
        
        Mapping pairs:
        - product_type (raw) -> top_level_product_type (derived)
        - product_type (raw) -> upper_product_type (derived)
        - hazard_item (raw) -> hazard_category (derived)
        - hazard_item (raw) -> analyzable (derived)
        - hazard_item (raw) -> interest_item (derived)
        """
        print("🔎 Mapping Failure Detection")
        print("=" * 60)
        
        results = []
        
        # Helper function to check if value is empty
        def is_empty(series):
            return (
                series.isna() | 
                (series == "") | 
                (series.astype(str) == "None")
            )
        
        # 1. product_type -> top_level_product_type
        if 'product_type' in self.df.columns and 'top_level_product_type' in self.df.columns:
            has_raw = ~is_empty(self.df['product_type'])
            missing_derived = is_empty(self.df['top_level_product_type'])
            failure_count = (has_raw & missing_derived).sum()
            failure_pct = (failure_count / self.total_rows * 100) if self.total_rows > 0 else 0
            
            results.append({
                'Mapping': 'product_type → top_level_product_type',
                'Failures': f"{failure_count:,}",
                'Failure %': f"{failure_pct:.2f}%",
                'Status': '❌ Critical' if failure_count > 0 else '✅ OK'
            })
        
        # 2. product_type -> upper_product_type
        if 'product_type' in self.df.columns and 'upper_product_type' in self.df.columns:
            has_raw = ~is_empty(self.df['product_type'])
            missing_derived = is_empty(self.df['upper_product_type'])
            failure_count = (has_raw & missing_derived).sum()
            failure_pct = (failure_count / self.total_rows * 100) if self.total_rows > 0 else 0
            
            results.append({
                'Mapping': 'product_type → upper_product_type',
                'Failures': f"{failure_count:,}",
                'Failure %': f"{failure_pct:.2f}%",
                'Status': '❌ Critical' if failure_count > 0 else '✅ OK'
            })
        
        # 3. hazard_item -> hazard_category
        if 'hazard_item' in self.df.columns and 'hazard_category' in self.df.columns:
            has_raw = ~is_empty(self.df['hazard_item'])
            missing_derived = is_empty(self.df['hazard_category'])
            failure_count = (has_raw & missing_derived).sum()
            failure_pct = (failure_count / self.total_rows * 100) if self.total_rows > 0 else 0
            
            results.append({
                'Mapping': 'hazard_item → hazard_category',
                'Failures': f"{failure_count:,}",
                'Failure %': f"{failure_pct:.2f}%",
                'Status': '❌ Critical' if failure_count > 0 else '✅ OK'
            })
        
        # Print table
        print(tabulate(results, headers='keys', tablefmt='grid'))
        print()
        
        return results
    
    def generate_summary(self):
        """Generate executive summary of the audit"""
        print("📋 Executive Summary")
        print("=" * 60)
        
        summary = [
            ['Total Records', f"{self.total_rows:,}"],
            ['Schema Columns', f"{len(UNIFIED_SCHEMA)} (Expected)"],
            ['Actual Columns', f"{len(self.df.columns)}"],
            ['Data Sources', ', '.join(self.df['data_source'].unique()) if 'data_source' in self.df.columns else 'N/A'],
            ['File Path', str(self.parquet_path)],
        ]
        
        print(tabulate(summary, tablefmt='grid'))
        print()
    
    def run_full_audit(self):
        """Execute complete audit workflow"""
        print("\n" + "=" * 60)
        print("🔬 CJFSDATACOLLECT - Data Quality Audit Report")
        print("=" * 60)
        print()
        
        # Step 1: Load data
        self.load_data()
        
        # Step 2: Executive Summary
        self.generate_summary()
        
        # Step 3: Schema validation
        schema_valid = self.verify_schema()
        
        # Step 4: Missing value analysis
        self.analyze_missing_values()
        
        # Step 5: Mapping failure detection
        self.detect_mapping_failures()
        
        # Final verdict
        print("=" * 60)
        if schema_valid and self.total_rows > 0:
            print("✅ Audit completed successfully")
        else:
            print("⚠️  Audit completed with warnings - please review above")
        print("=" * 60)
        print()


def main():
    """Main entry point for the audit script"""
    auditor = DataQualityAuditor()
    
    try:
        auditor.run_full_audit()
    except FileNotFoundError as e:
        print(str(e))
        sys.exit(1)
    except Exception as e:
        print(f"❌ Audit failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
