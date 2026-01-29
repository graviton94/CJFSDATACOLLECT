import pandas as pd
from pathlib import Path
from loguru import logger

class ReferenceEnricher:
    """
    Module to post-process and enrich reference data after API collection.
    Ensures hierarchical names and cross-references are persisted in Parquet files.
    """
    
    REF_DIR = Path("data/reference")
    
    def __init__(self, ref_dir: Path = None):
        if ref_dir:
            self.REF_DIR = ref_dir
            
    def enrich_all(self):
        """Main entry point to enrich all supported master data files."""
        logger.info("🚀 Starting Reference Data Enrichment process...")
        
        # 1. Product Type Master (Hierarchy Resolution)
        self.enrich_product_master()
        
        # 2. Add more master enrichments here as needed (Spec, Hazard, etc.)
        self.enrich_spec_master("individual_spec_master.parquet")
        self.enrich_spec_master("common_spec_master.parquet")
        
        logger.info("✨ Enrichment process completed.")

    def enrich_product_master(self):
        """
        Enrich 'product_code_master' with human-readable hierarchy names.
        Maps HTRK_PRDLST_CD -> 최상위품목명, HRNK_PRDLST_CD -> 상위품목명.
        """
        file_path = self.REF_DIR / "product_code_master.parquet"
        if not file_path.exists():
            logger.warning(f"Skipping Product Master: {file_path} not found.")
            return
            
        try:
            df = pd.read_parquet(file_path)
            if 'PRDLST_CD' not in df.columns or 'KOR_NM' not in df.columns:
                logger.error("Product Master missing required columns for mapping.")
                return
            
            # Create a lookup map (Code -> Name)
            lookup_map = df.set_index('PRDLST_CD')['KOR_NM'].to_dict()
            
            # Enrich
            df['최상위품목명'] = df['HTRK_PRDLST_CD'].map(lookup_map).fillna(df['HTRK_PRDLST_CD'])
            df['상위품목명'] = df['HRNK_PRDLST_CD'].map(lookup_map).fillna(df['HRNK_PRDLST_CD'])
            
            # Standardize flags
            if 'IS_MANUAL_FIXED' not in df.columns:
                df['IS_MANUAL_FIXED'] = False
            if 'USE_YN' not in df.columns:
                df['USE_YN'] = 'Y'
            
            # Save enriched version
            df.to_parquet(file_path, engine='pyarrow', index=False)
            logger.info(f"✅ Enriched Product Master: {len(df)} rows.")
            
        except Exception as e:
            logger.error(f"Failed to enrich Product Master: {e}")

    def enrich_spec_master(self, filename: str):
        """
        Standardize and enrich Specification masters.
        Ensures consistency for PRDLST_CD_NM, TESTITM_NM and metadata.
        Lookups names from Product and Hazard masters.
        """
        file_path = self.REF_DIR / filename
        if not file_path.exists():
            return
            
        try:
            df = pd.read_parquet(file_path)
            
            # Ensure IS_MANUAL_FIXED exists
            if 'IS_MANUAL_FIXED' not in df.columns:
                df['IS_MANUAL_FIXED'] = False
                
            # 1. Product Master Lookup (for PRDLST_CD_NM)
            prod_path = self.REF_DIR / "product_code_master.parquet"
            if prod_path.exists():
                prod_df = pd.read_parquet(prod_path)
                p_map = prod_df.set_index('PRDLST_CD')['KOR_NM'].to_dict()
                if 'PRDLST_CD' in df.columns:
                    df['PRDLST_CD_NM'] = df['PRDLST_CD'].map(p_map).fillna(df.get('PRDLST_CD_NM', df['PRDLST_CD']))

            # 2. Hazard Master Lookup (for TESTITM_NM)
            haz_path = self.REF_DIR / "hazard_code_master.parquet"
            if haz_path.exists():
                haz_df = pd.read_parquet(haz_path)
                h_map = haz_df.set_index('TESTITM_CD')['KOR_NM'].to_dict()
                if 'TESTITM_CD' in df.columns:
                    df['TESTITM_NM'] = df['TESTITM_CD'].map(h_map).fillna(df.get('TESTITM_NM', df['TESTITM_CD']))
            
            df.to_parquet(file_path, engine='pyarrow', index=False)
            logger.info(f"✅ Enriched Spec Master ({filename}): {len(df)} rows.")
            
        except Exception as e:
            logger.error(f"Failed to enrich {filename}: {e}")

if __name__ == "__main__":
    enricher = ReferenceEnricher()
    enricher.enrich_all()
