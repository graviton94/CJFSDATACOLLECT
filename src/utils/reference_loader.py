import os
import requests
import pandas as pd
from pathlib import Path
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from loguru import logger

class ReferenceLoader:
    """
    식약처 기준정보(백서)를 수집하여 Parquet 파일로 저장하는 로더 (Zombie Mode 적용)
    
    [수집 대상]
    1. I2510: 식품공전 품목유형
    2. I2530: 식품공전 시험항목
    3. I2580: 식품공전 개별기준규격
    4. I2600: 식품공전 공통기준규격
    """
    
    API_KEY = "4e740c4337844667821c" 
    BASE_URL = "http://openapi.foodsafetykorea.go.kr/api"
    OUTPUT_DIR = Path("data/reference")

    def __init__(self):
        self.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        self.targets = {
            "I2510": {"name": "product_code_master", "desc": "식품공전 품목유형"},
            "I2530": {"name": "hazard_code_master", "desc": "식품공전 시험항목"},
            "I2580": {"name": "individual_spec_master", "desc": "식품공전 개별기준규격"},
            "I2600": {"name": "common_spec_master", "desc": "식품공전 공통기준규격"}
        }
        
        # 세션 설정 (Connection Pool 및 Retry 자동화)
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    def fetch_data(self, service_id, target_config):
        """특정 서비스 ID에 대해 끝까지 Pagination을 수행 (Retry 강화)"""
        all_rows = []
        start = 1
        step = 1000
        
        print(f"📥 [{target_config['desc']}] 수집 시작 ({service_id})...")
        
        while True:
            end = start + step - 1
            url = f"{self.BASE_URL}/{self.API_KEY}/{service_id}/json/{start}/{end}"
            
            try:
                # Timeout을 30초로 넉넉하게 설정
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                try:
                    data = response.json()
                except ValueError:
                    print(f"   ⚠️ JSON Decoding Failed at {start}-{end}. Skipping...")
                    break

                # 1. API 응답 구조 검증
                if service_id not in data:
                    if 'RESULT' in data and 'MSG' in data['RESULT']:
                         msg = data['RESULT']['MSG']
                         # 데이터 없음 메시지면 정상 종료
                         if "해당하는 데이터가 없습니다" in msg:
                             print(f"   🎉 수집 완료 (End of Data message at {start})")
                             break
                         print(f"   ⚠️ API Message: {msg}")
                    break

                # 2. 데이터 존재 여부 확인
                if 'row' not in data[service_id]:
                    print(f"   🎉 수집 완료 (No 'row' key at {start})")
                    break
                
                rows = data[service_id]['row']
                if not rows:
                    print(f"   🎉 수집 완료 (Empty rows at {start})")
                    break
                    
                all_rows.extend(rows)
                print(f"   ✅ Fetched {start}~{end} (누적: {len(all_rows)}건)")
                
                start += step
                time.sleep(0.2) # 딜레이 약간 증가
                
            except requests.exceptions.ReadTimeout:
                print(f"   ⏳ Timeout at {start}-{end}. Retrying in 30 seconds...")
                time.sleep(30)
                continue # 재시도
            except Exception as e:
                print(f"   ❌ Critical Error at {start}-{end}: {e}")
                # 치명적 오류 시, 지금까지 모은 거라도 저장하기 위해 루프 종료
                break
        
        # Convert to DataFrame
        df = pd.DataFrame(all_rows)
        
        # Add ANALYZABLE and INTEREST_ITEM columns for hazard_code_master (I2530)
        if service_id == "I2530" and not df.empty:
            if 'ANALYZABLE' not in df.columns:
                df['ANALYZABLE'] = False
            if 'INTEREST_ITEM' not in df.columns:
                df['INTEREST_ITEM'] = False
                
        return df

    def run(self):
        """전체 타겟 실행 (수동고정 데이터 보존 로직 포함)"""
        # Primary Key Mapping based on constants
        pk_map = {
            "I2510": "PRDLST_CD",
            "I2530": "TESTITM_CD",
            "I2580": "INDV_SPEC_SEQ",
            "I2600": "CMMN_SPEC_SEQ"
        }

        for service_id, config in self.targets.items():
            try:
                new_df = self.fetch_data(service_id, config)
                if new_df.empty:
                    print(f"⚠️ {config['desc']} 수집 실패: 데이터 없음\n")
                    continue
                
                file_path = self.OUTPUT_DIR / f"{config['name']}.parquet"
                pk = pk_map.get(service_id)

                if file_path.exists() and pk:
                    try:
                        old_df = pd.read_parquet(file_path)
                        
                        if 'IS_MANUAL_FIXED' in old_df.columns:
                            # 1. 수동 고정된 데이터 추출
                            manual_df = old_df[old_df['IS_MANUAL_FIXED'] == True].copy()
                            
                            if not manual_df.empty:
                                print(f"   💡 {len(manual_df)}건의 수동 고정 데이터를 발견했습니다. 보존 처리합니다.")
                                
                                # 2. 새로운 데이터에서 수동 고정된 PK를 제외
                                if pk in new_df.columns:
                                    manual_pks = manual_df[pk].unique()
                                    new_df = new_df[~new_df[pk].isin(manual_pks)]
                                
                                # 3. 병합
                                final_df = pd.concat([manual_df, new_df], ignore_index=True)
                            else:
                                final_df = new_df
                        else:
                            final_df = new_df
                    except Exception as merge_err:
                        print(f"   ⚠️ 병합 중 오류 발생, 신규 데이터로 대체합니다: {merge_err}")
                        final_df = new_df
                else:
                    final_df = new_df

                # Ensure IS_MANUAL_FIXED exists in final output
                if 'IS_MANUAL_FIXED' not in final_df.columns:
                    final_df['IS_MANUAL_FIXED'] = False

                final_df.to_parquet(file_path, engine='pyarrow', compression='snappy', index=False)
                print(f"💾 저장 완료: {file_path} (Total {len(final_df)} rows)\n")

            except Exception as e:
                print(f"🚫 {config['desc']} 처리 중단: {e}\n")
        
        # 2. Enrich and Standardize Data
        try:
            from src.utils.reference_enricher import ReferenceEnricher
            enricher = ReferenceEnricher()
            enricher.enrich_all()
        except Exception as e:
            print(f"⚠️ Enrichment failed: {e}")

        # 3. 국가명 마스터 데이터 처리
        self._process_country_master()
    
    def _process_country_master(self):
        """국가명 마스터 데이터 TSV -> Parquet 변환"""
        print("📥 국가명 마스터 데이터 처리 중...")
        
        tsv_path = self.OUTPUT_DIR / "country_master.tsv"
        parquet_path = self.OUTPUT_DIR / "country_master.parquet"
        
        if not tsv_path.exists():
            print(f"   ⚠️ {tsv_path} 파일이 없습니다.")
            return
        
        try:
            # TSV 파일 읽기 (한글 인코딩 지원)
            df = pd.read_csv(tsv_path, sep='\t', encoding='utf-8')
            
            # 컬럼명 정규화 (띄어쓰기 제거, 영문으로 변환)
            df.columns = [
                'country_name_eng',
                'country_name_kor',
                'iso_2',
                'iso_3',
                'iso_numeric'
            ]
            
            # NULL 값 처리
            df = df.fillna('')
            
            # Parquet 저장
            df.to_parquet(parquet_path, engine='pyarrow', compression='snappy', index=False)
            print(f"   💾 국가명 마스터 저장 완료: {parquet_path} (Total {len(df)} rows)")
            
        except Exception as e:
            print(f"   ❌ 국가명 마스터 처리 실패: {e}")

if __name__ == "__main__":
    loader = ReferenceLoader()
    loader.run()