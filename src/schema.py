"""
Unified schema for food safety risk data from multiple sources.
Normalizes data from EU RASFF, FDA Import Alerts, and Korea MFDS.
Follows the 13-column standard defined below.
"""

import pandas as pd
import hashlib
from datetime import datetime
import numpy as np

# ---------------------------------------------------------
# 1. Unified Schema Definition (The 13 Commandments)
# 모든 수집 데이터는 최종적으로 이 컬럼 구성을 따라야 합니다.
# ---------------------------------------------------------
UNIFIED_SCHEMA = [
    "registration_date",        # 등록일자 (YYYY-MM-DD)
    "data_source",              # 데이터소스 (FDA, RASFF, MFDS)
    "source_detail",            # 상세출처 (API ID, Ref No 등)
    "product_type",             # 품목유형 (원본)
    "top_level_product_type",   # 최상위품목유형 (Lookup)
    "upper_product_type",       # 상위품목유형 (Lookup)
    "product_name",             # 제품명
    "origin_country",           # 원산지
    "notifying_country",        # 통보국
    "hazard_category",          # 분류(카테고리) (Lookup)
    "hazard_item",              # 시험항목 (위해정보 원본)
    "analyzable",               # 분석가능여부 (Boolean Lookup)
    "interest_item"             # 관심항목 (Boolean Lookup)
]

# ---------------------------------------------------------
# 2. Display Headers Mapping (For UI/Excel Export)
# 한글 헤더 매핑을 여기서 중앙 관리하여 인코딩 이슈를 방지합니다.
# ---------------------------------------------------------
DISPLAY_HEADERS = {
    "registration_date": "등록일자",
    "data_source": "데이터소스",
    "source_detail": "상세출처",
    "product_type": "품목유형",
    "top_level_product_type": "최상위품목유형",
    "upper_product_type": "상위품목유형",
    "product_name": "제품명",
    "origin_country": "원산지",
    "notifying_country": "통보국",
    "hazard_category": "분류(카테고리)",
    "hazard_item": "시험항목",
    "analyzable": "분석가능여부",
    "interest_item": "관심항목"
}

def generate_record_id(source: str, ref_no: str) -> str:
    """
    Source와 Unique Reference를 조합하여 고유 ID(SHA256) 생성
    """
    if not ref_no:
        ref_no = datetime.now().isoformat()
    raw_key = f"{source}::{ref_no}"
    return hashlib.sha256(raw_key.encode('utf-8')).hexdigest()

def get_empty_dataframe() -> pd.DataFrame:
    """스키마 구조를 가진 빈 DataFrame 반환"""
    return pd.DataFrame(columns=UNIFIED_SCHEMA)

def validate_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    DataFrame이 13개 컬럼 규칙을 준수하도록 강제 변환합니다.
    """
    # 명시적 복사로 SettingWithCopyWarning 방지
    df = df.copy()
    
    # 1. 누락된 컬럼은 None으로 생성
    for col in UNIFIED_SCHEMA:
        if col not in df.columns:
            df[col] = None
            
    # 2. 컬럼 순서 강제 및 불필요 컬럼 제거
    df = df[UNIFIED_SCHEMA].copy()
    
    # 3. 데이터 타입 강제 변환
    # 날짜: Datetime -> Date String (YYYY-MM-DD)
    if 'registration_date' in df.columns:
        df.loc[:, 'registration_date'] = pd.to_datetime(
            df['registration_date'], errors='coerce'
        ).dt.strftime('%Y-%m-%d')
        
    # Boolean 필드 처리 (None -> False)
    for bool_col in ['analyzable', 'interest_item']:
        df.loc[:, bool_col] = df[bool_col].fillna(False).astype(bool)

    # 문자열 필드 처리 (None -> "")
    str_cols = [c for c in UNIFIED_SCHEMA if c not in ['analyzable', 'interest_item']]
    df.loc[:, str_cols] = df[str_cols].fillna("")
    
    return df
