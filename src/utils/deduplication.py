import pandas as pd
from pathlib import Path
from src.schema import generate_record_id, UNIFIED_SCHEMA

def merge_and_deduplicate(new_df: pd.DataFrame, data_dir: Path = Path("data/hub")) -> pd.DataFrame:
    """
    기존 Parquet 파일과 병합하며 중복을 제거합니다.
    Unique Key 기준: data_source + source_detail
    """
    file_path = data_dir / "hub_data.parquet"
    
    if new_df.empty:
        return new_df

    # 신규 데이터에 임시 ID 생성
    new_df['temp_id'] = new_df.apply(
        lambda x: generate_record_id(x['data_source'], x['source_detail']), axis=1
    )
    
    if not file_path.exists():
        # 기존 파일 없으면 전체 저장 (temp_id 제거 후)
        return new_df.drop(columns=['temp_id'])
    
    try:
        # 기존 데이터 로드
        existing_df = pd.read_parquet(file_path)
        
        # 기존 데이터에도 임시 ID 생성 (만약 없다면)
        existing_df['temp_id'] = existing_df.apply(
            lambda x: generate_record_id(x['data_source'], x['source_detail']), axis=1
        )
        
        # 중복 제거 로직: 기존 ID에 없는 것만 신규로 간주
        existing_ids = set(existing_df['temp_id'])
        non_duplicate_df = new_df[~new_df['temp_id'].isin(existing_ids)]
        
        print(f"   🔍 Deduplication: {len(new_df)} incoming -> {len(non_duplicate_df)} new unique records.")
        
        # 병합
        combined_df = pd.concat([existing_df, non_duplicate_df], ignore_index=True)
        
        # 임시 ID 제거 및 반환
        return combined_df.drop(columns=['temp_id'])
        
    except Exception as e:
        print(f"⚠️ Deduplication Error: {e}. Appending without check.")
        return new_df.drop(columns=['temp_id'])