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
                
        return pd.DataFrame(all_rows)

    def run(self):
        """전체 타겟 실행"""
        for service_id, config in self.targets.items():
            try:
                df = self.fetch_data(service_id, config)
                
                if not df.empty:
                    file_path = self.OUTPUT_DIR / f"{config['name']}.parquet"
                    df.to_parquet(file_path, engine='pyarrow', compression='snappy', index=False)
                    print(f"💾 저장 완료: {file_path} (Total {len(df)} rows)\n")
                else:
                    print(f"⚠️ {config['desc']} 수집 실패: 데이터 없음\n")
            except Exception as e:
                print(f"🚫 {config['desc']} 처리 중단: {e}\n")
        
        # 국가명 마스터 데이터 처리
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