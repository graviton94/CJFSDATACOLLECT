import os
import json
import re
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# 통합 스키마 및 유틸리티 가져오기
from src.schema import UNIFIED_SCHEMA, validate_schema, generate_record_id, get_empty_dataframe

load_dotenv()

class MFDSCollector:
    """
    대한민국 식약처(MFDS) 위해정보 수집기
    현재 구현된 서비스:
    - I2620: 국내식품 부적합 정보 (Domestic Food Inspection Failure)
    """
    
    BASE_URL = "http://openapi.foodsafetykorea.go.kr/api"
    REF_DIR = Path("data/reference")
    
    def __init__(self):
        self.api_key = os.getenv("KOREA_FOOD_API_KEY")
        if not self.api_key:
            raise ValueError("❌ Error: KOREA_FOOD_API_KEY가 .env 파일에 없습니다.")
            
        # ---------------------------------------------------------
        # [Smart Lookup] 기준정보 로드 (메모리 캐싱)
        # ---------------------------------------------------------
        print("📥 기준정보(Reference Data) 로드 중...")
        self.product_ref_df = self._load_reference_df("product_code_master.parquet")
        self.hazard_ref_df = self._load_reference_df("hazard_code_master.parquet")
        self.country_ref = self._load_country_reference()
        print("✅ 기준정보 로드 완료.")

    def _load_reference_df(self, filename):
        """
        Parquet 파일을 DataFrame으로 로드 (Multi-column 검색 지원)
        """
        file_path = self.REF_DIR / filename
        if not file_path.exists():
            print(f"   ⚠️ Warning: {filename} 파일이 없습니다. Lookup 기능이 제한됩니다.")
            return pd.DataFrame()
        
        try:
            df = pd.read_parquet(file_path)
            print(f"   📚 {filename} 로드 완료 (총 {len(df)}건, 컬럼: {df.columns.tolist()})")
            return df
        except Exception as e:
            print(f"   ❌ {filename} 로드 실패: {e}")
            return pd.DataFrame()

    def fetch_service(self, service_id, start_idx, end_idx):
        """API 호출 및 JSON 응답 반환"""
        url = f"{self.BASE_URL}/{self.api_key}/{service_id}/json/{start_idx}/{end_idx}"
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # 데이터 검증
            if service_id in data and 'row' in data[service_id]:
                return data[service_id]['row']
            # 에러 메시지 확인
            if 'RESULT' in data and 'MSG' in data['RESULT']:
                if "해당하는 데이터가 없습니다" in data['RESULT']['MSG']:
                    return []
            return []
        except Exception as e:
            print(f"   ⚠️ API 호출 에러 ({start_idx}-{end_idx}): {e}")
            return []

    def normalize_date(self, date_str):
        """날짜 변환: 2025.03.12 -> 2025-03-12"""
        if not date_str: return None
        return date_str.replace('.', '-')

    def _lookup_product_info(self, product_type):
        """
        품목유형 이름으로 상위/최상위 유형 조회
        
        Logic 1: Product Hierarchy Lookup
        - Input: product_type (from API)
        - Reference: product_code_master.parquet
        - Matching Rule: Find row where product_type matches KOR_NM OR ENG_NM
        - Output Mapping:
          - top_level_product_type ← HTRK_PRDLST_CD (from reference)
          - upper_product_type ← HRRK_PRDLST_CD (from reference)
        """
        info = {"top": None, "upper": None}
        
        if self.product_ref_df.empty or not product_type:
            return info
        
        # 매칭할 컬럼들 (KOR_NM, ENG_NM)
        match_columns = ['KOR_NM', 'ENG_NM']
        
        # 각 컬럼에서 매칭 시도
        matched_row = None
        for col in match_columns:
            if col in self.product_ref_df.columns:
                # 정확히 일치하는 행 찾기 (대소문자 구분 없이)
                mask = self.product_ref_df[col].astype(str).str.strip().str.lower() == str(product_type).strip().lower()
                if mask.any():
                    matched_row = self.product_ref_df[mask].iloc[0]
                    break
        
        if matched_row is not None:
            # 출력 필드 추출: HTRK_PRDLST_CD, HRRK_PRDLST_CD
            info["top"] = matched_row.get("HTRK_PRDLST_CD") if "HTRK_PRDLST_CD" in matched_row.index else None
            info["upper"] = matched_row.get("HRRK_PRDLST_CD") if "HRRK_PRDLST_CD" in matched_row.index else None
        
        return info

    def _lookup_hazard_info(self, hazard_item):
        """
        시험항목 이름으로 분류(카테고리) 조회
        
        Logic 2: Hazard Classification Lookup
        - Input: hazard_item (from API)
        - Reference: hazard_code_master.parquet
        - Matching Rule: Find row where hazard_item matches ANY of:
          ['KOR_NM', 'ENG_NM', 'ABRV', 'NCKNM', 'TESTITM_NM']
        - Output Mapping:
          - hazard_category ← M_KOR_NM (from reference)
          - analyzable ← ANALYZABLE (from reference)
          - interest_item ← INTEREST_ITEM (from reference)
        """
        info = {"category": None, "analyzable": False, "interest": False}
        
        if self.hazard_ref_df.empty or not hazard_item:
            return info
        
        # 매칭할 컬럼들
        match_columns = ['KOR_NM', 'ENG_NM', 'ABRV', 'NCKNM', 'TESTITM_NM']
        
        # 각 컬럼에서 매칭 시도
        matched_row = None
        for col in match_columns:
            if col in self.hazard_ref_df.columns:
                # 정확히 일치하는 행 찾기 (대소문자 구분 없이)
                mask = self.hazard_ref_df[col].astype(str).str.strip().str.lower() == str(hazard_item).strip().lower()
                if mask.any():
                    matched_row = self.hazard_ref_df[mask].iloc[0]
                    break
        
        if matched_row is not None:
            # 출력 필드 추출: M_KOR_NM, ANALYZABLE, INTEREST_ITEM
            info["category"] = matched_row.get("M_KOR_NM") if "M_KOR_NM" in matched_row.index else None
            info["analyzable"] = bool(matched_row.get("ANALYZABLE", False)) if "ANALYZABLE" in matched_row.index else False
            info["interest"] = bool(matched_row.get("INTEREST_ITEM", False)) if "INTEREST_ITEM" in matched_row.index else False
        
        return info

    def collect_i2620(self):
        """
        [I2620] 국내식품 검사부적합 수집 로직
        """
        service_id = "I2620"
        print(f"🚀 [I2620] 국내식품 부적합 정보 수집 시작...")
        
        all_records = []
        start, step = 1, 1000
        
        while True:
            end = start + step - 1
            rows = self.fetch_service(service_id, start, end)
            
            if not rows:
                print(f"   🎉 수집 완료 (Total pages processed)")
                break
                
            print(f"   - Processing {start} ~ {end} (Got {len(rows)} items)")
            
            for row in rows:
                try:
                    # 1. 필드 추출 (API 명세 기준)
                    raw_date = row.get("CRET_DTM", "") # 등록일 (YYYY.MM.DD)
                    product_name = row.get("PRDTNM", "") # 제품명
                    product_type = row.get("PRDLST_CD_NM", "") # 식품유형 (ex: 냉이)
                    hazard_item = row.get("TEST_ITMNM", "") # 부적합항목 (ex: 펜디메탈린)
                    unique_seq = row.get("RTRVLDSUSE_SEQ", "") # 회수폐기일련번호
                    
                    # 2. 데이터 정제 & Lookup
                    reg_date = self.normalize_date(raw_date)
                    prod_info = self._lookup_product_info(product_type)
                    hazard_info = self._lookup_hazard_info(hazard_item)
                    
                    # 3. 상세 출처 생성
                    source_detail = f"{service_id}-{unique_seq}" if unique_seq else f"{service_id}-UNKNOWN"
                    
                    # 4. 통합 스키마 매핑 (13 Columns Strict)
                    record = {
                        "registration_date": reg_date,
                        "data_source": "MFDS",
                        "source_detail": source_detail,
                        "product_type": product_type,
                        "top_level_product_type": prod_info["top"],
                        "upper_product_type": prod_info["upper"],
                        "product_name": product_name,
                        "origin_country": "South Korea", # 국내식품
                        "notifying_country": "South Korea",
                        "hazard_category": hazard_info["category"],
                        "hazard_item": hazard_item,
                        "analyzable": hazard_info["analyzable"],
                        "interest_item": hazard_info["interest"]
                    }
                    all_records.append(record)
                    
                except Exception as e:
                    print(f"   ⚠️ Skipping row due to error: {e}")
                    continue

            # 테스트용: 너무 많으면 오래 걸리므로 일단 2000건에서 break (실제 운영 시 제거)
            # if end >= 2000: break 
            
            start += step

        if not all_records:
            return get_empty_dataframe()
            
        return pd.DataFrame(all_records)

    def collect_i0490(self):
        """
        [I0490] 회수판매중지 정보 수집 로직
        """
        service_id = "I0490"
        print(f"🚀 [I0490] 회수판매중지 정보 수집 시작...")
        
        all_records = []
        start, step = 1, 1000
        
        while True:
            end = start + step - 1
            rows = self.fetch_service(service_id, start, end)
            
            if not rows:
                print(f"   🎉 수집 완료 (Total pages processed)")
                break
                
            print(f"   - Processing {start} ~ {end} (Got {len(rows)} items)")
            
            for row in rows:
                try:
                    # 1. 필드 추출 (API 명세 기준)
                    raw_date = row.get("CRET_DTM", "")  # 등록일 (YYYY-MM-DD HH:MM:SS)
                    product_name = row.get("PRDTNM", "")  # 제품명
                    product_type = row.get("PRDLST_CD_NM", "")  # 식품유형
                    recall_reason = row.get("RTRVLPRVNS", "")  # 회수사유 (e.g., 이물 혼입)
                    unique_seq = row.get("RTRVLDSUSE_SEQ", "")  # 회수폐기일련번호
                    
                    # 2. 날짜 정규화: YYYY-MM-DD HH:MM:SS -> YYYY-MM-DD (첫 10글자만)
                    reg_date = raw_date[:10] if raw_date else None
                    
                    # 3. 데이터 정제 & Lookup
                    prod_info = self._lookup_product_info(product_type)
                    hazard_info = self._lookup_hazard_info(recall_reason)
                    
                    # 4. 상세 출처 생성
                    source_detail = f"{service_id}-{unique_seq}" if unique_seq else f"{service_id}-UNKNOWN"
                    
                    # 5. 통합 스키마 매핑 (13 Columns Strict)
                    record = {
                        "registration_date": reg_date,
                        "data_source": "MFDS",
                        "source_detail": source_detail,
                        "product_type": product_type,
                        "top_level_product_type": prod_info["top"],
                        "upper_product_type": prod_info["upper"],
                        "product_name": product_name,
                        "origin_country": "South Korea",  # 국내식품 회수
                        "notifying_country": "South Korea",
                        "hazard_category": hazard_info["category"],
                        "hazard_item": recall_reason,
                        "analyzable": hazard_info["analyzable"],
                        "interest_item": hazard_info["interest"]
                    }
                    all_records.append(record)
                    
                except Exception as e:
                    print(f"   ⚠️ Skipping row due to error: {e}")
                    continue

            start += step

        if not all_records:
            return get_empty_dataframe()
            
        return pd.DataFrame(all_records)

    def _load_country_reference(self):
        """국가명 마스터 TSV -> 딕셔너리 변환"""
        file_path = self.REF_DIR / "country_master.tsv"
        if not file_path.exists():
            print(f"   ⚠️ Warning: country_master.tsv 파일이 없습니다.")
            return {}
        
        try:
            # TSV 파일을 읽기 (탭 구분자)
            df = pd.read_csv(file_path, sep='\t', encoding='utf-8')
            
            # 국가명(한글)을 키로, 영문명+ISO를 값으로 하는 딕셔너리 생성
            country_dict = {}
            for _, row in df.iterrows():
                kor_name = row.get('국가명(국문)', '')
                if kor_name and pd.notna(kor_name):
                    country_dict[kor_name] = {
                        'eng': row.get('국가명(영문)', ''),
                        'iso_2': row.get('ISO(2자리)', ''),
                        'iso_3': row.get('ISO(3자리)', '')
                    }
            return country_dict
        except Exception as e:
            print(f"   ❌ country_master.tsv 로드 실패: {e}")
            return {}

    def _normalize_country_name(self, raw_country):
        """원본 국가명(BDT에서 추출)을 정규화된 국가명으로 변환"""
        if not raw_country:
            return "Overseas"
        
        # 좌우 공백 제거
        raw_country = raw_country.strip()
        
        # 1차: 정확한 매치
        if raw_country in self.country_ref:
            return self.country_ref[raw_country]['eng']
        
        # 2차: 부분 매치 (첫 문자 일치)
        for kor_name, data in self.country_ref.items():
            if kor_name.startswith(raw_country[:2]):  # 첫 2글자 일치
                return data['eng']
        
        # 3차: 반환 (매칭 실패)
        return raw_country if raw_country else "Overseas"

    def _extract_origin_from_bdt(self, bdt_text):
        """BDT 필드에서 지역(원산지) 정보 정규식 추출"""
        if not bdt_text:
            return "Overseas"
        
        # 패턴 1: "-지역: 국가명" 또는 "지역: 국가명"
        match = re.search(r"[-]?지역:\s*([가-힣a-zA-Z\s]+?)(?:\s*[-]|$)", bdt_text)
        if match:
            origin = match.group(1).strip()
            return origin if origin else "Overseas"
        
        # 패턴 2: "지역" 키워드 뒤의 텍스트
        match = re.search(r"지역\s*[:\-]\s*([가-힣a-zA-Z]+)", bdt_text)
        if match:
            return match.group(1).strip()
        
        # 추출 실패 시 기본값
        return "Overseas"

    def collect_i2810(self):
        """
        [I2810] 해외 위해식품 회수정보 수집 로직
        데이터가 BDT 필드의 비정형 텍스트에 포함되어 있어 정규식 파싱이 필요함
        """
        service_id = "I2810"
        print(f"🚀 [I2810] 해외 위해식품 회수정보 수집 시작...")
        
        all_records = []
        start, step = 1, 1000
        
        while True:
            end = start + step - 1
            rows = self.fetch_service(service_id, start, end)
            
            if not rows:
                print(f"   🎉 수집 완료 (Total pages processed)")
                break
                
            print(f"   - Processing {start} ~ {end} (Got {len(rows)} items)")
            
            for row in rows:
                try:
                    # 1. 필드 추출
                    raw_date = row.get("CRET_DTM", "")  # 등록일 (YYYYMMDD)
                    product_name = row.get("TITL", "")  # 제품명
                    hazard_item = row.get("DETECT_TITL", "")  # 위해물질
                    notify_no = row.get("NTCTXT_NO", "")  # 통지번호
                    bdt_text = row.get("BDT", "")  # 비정형 텍스트 (지역 추출 대상)
                    
                    # 2. 날짜 정규화: YYYYMMDD -> YYYY-MM-DD
                    if raw_date and len(raw_date) == 8:
                        reg_date = f"{raw_date[0:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
                    else:
                        reg_date = None
                    
                    # 3. BDT에서 원산지 추출 (정규식)
                    origin_country = self._extract_origin_from_bdt(bdt_text)
                    
                    # 4. Lookup을 통한 분류 정보 조회
                    # 제품유형은 고정값이므로 lookup 스킵
                    hazard_info = self._lookup_hazard_info(hazard_item)
                    
                    # 5. 상세 출처 생성
                    source_detail = f"{service_id}-{notify_no}" if notify_no else f"{service_id}-UNKNOWN"
                    
                    # 6. 통합 스키마 매핑 (13 Columns Strict)
                    record = {
                        "registration_date": reg_date,
                        "data_source": "MFDS",
                        "source_detail": source_detail,
                        "product_type": "수입식품(해외회수)",  # 고정값
                        "top_level_product_type": "수입식품",  # 고정값
                        "upper_product_type": "위해회수",  # 고정값
                        "product_name": product_name,
                        "origin_country": origin_country,  # BDT에서 추출
                        "notifying_country": "South Korea",  # 고정값 (MFDS)
                        "hazard_category": hazard_info["category"],
                        "hazard_item": hazard_item,
                        "analyzable": hazard_info["analyzable"],
                        "interest_item": hazard_info["interest"]
                    }
                    all_records.append(record)
                    
                except Exception as e:
                    print(f"   ⚠️ Skipping row due to error: {e}")
                    continue

            start += step

        if not all_records:
            return get_empty_dataframe()
            
        return pd.DataFrame(all_records)

    def collect(self):
        """메인 실행 함수: 모든 MFDS 서비스 통합 수집"""
        # 1. 각 서비스별 수집
        df_i2620 = self.collect_i2620()
        df_i0490 = self.collect_i0490()
        df_i2810 = self.collect_i2810()
        
        # 2. 결과 병합
        dfs_to_combine = [df for df in [df_i2620, df_i0490, df_i2810] if not df.empty]
        
        if not dfs_to_combine:
            return get_empty_dataframe()
        
        combined_df = pd.concat(dfs_to_combine, ignore_index=True)
        
        # 3. 최종 스키마 검증 및 반환
        final_df = validate_schema(combined_df)
        print(f"✅ [Total] 총 {len(final_df)} 건 수집 및 정규화 완료 (I2620 + I0490 + I2810).")
        return final_df

if __name__ == "__main__":
    collector = MFDSCollector()
    df = collector.collect()
    print(df.head(5))
    
    # 결과 확인용 저장
    # df.to_csv("mfds_i2620_result.csv", index=False, encoding='utf-8-sig')