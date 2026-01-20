import re
import pandas as pd
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright
from src.schema import validate_schema, get_empty_dataframe, UNIFIED_SCHEMA

class ImpFoodScraper:
    """
    수입식품정보마루 (Imported Food Safety Portal) 스크래퍼
    - 대상: 수입식품 부적합 정보
    - 방식: Playwright를 이용한 DOM 속성 추출
    """
    
    # 베이스 URL (검색 조건: 전체 조회, 100개씩 보기)
    BASE_URL = "https://impfood.mfds.go.kr/CFCEE01F01/getList?limit=100&searchCondition=pdNm"
    REF_DIR = Path("data/reference")
    
    def __init__(self):
        # Load reference data as DataFrame (consistent with MFDS approach)
        print("📥 기준정보(Reference Data) 로드 중...")
        self.product_ref_df = self._load_reference_df("product_code_master.parquet")
        self.hazard_ref_df = self._load_reference_df("hazard_code_master.parquet")
        print("✅ 기준정보 로드 완료.")

    def _load_reference_df(self, filename):
        """
        Parquet 파일을 DataFrame으로 로드 (Multi-column 검색 지원)
        - Consistent with MFDSCollector approach
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

    def _lookup_product_info(self, product_type):
        """
        품목유형 이름으로 상위/최상위 유형 조회
        - Same logic as MFDSCollector._lookup_product_info
        - Returns NAMES (NM) instead of CODES (CD)
        """
        info = {"top": None, "upper": None}
        
        if self.product_ref_df.empty or not product_type:
            return info
        
        # 매칭할 컬럼들 (KOR_NM, ENG_NM)
        match_columns = ['KOR_NM', 'ENG_NM']
        
        # Normalize search term (strip whitespace to fix matching issues)
        search_term = str(product_type).strip().lower()
        
        # 각 컬럼에서 매칭 시도 (early exit on first match)
        matched_row = None
        for col in match_columns:
            if col in self.product_ref_df.columns:
                # 정확히 일치하는 행 찾기 (대소문자 구분 없이, 공백 제거)
                mask = self.product_ref_df[col].astype(str).str.strip().str.lower() == search_term
                if mask.any():
                    matched_row = self.product_ref_df[mask].iloc[0]
                    break  # Early exit on first match
        
        if matched_row is not None:
            # 출력 필드 추출: NAMES instead of CODES
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

    def _lookup_hazard_info(self, hazard_item):
        """
        시험항목 이름으로 분류(카테고리) 조회
        - Same logic as MFDSCollector._lookup_hazard_info
        """
        info = {"category": None, "analyzable": False, "interest": False}
        
        if self.hazard_ref_df.empty or not hazard_item:
            return info
        
        # 매칭할 컬럼들
        match_columns = ['KOR_NM', 'ENG_NM', 'ABRV', 'NCKNM', 'TESTITM_NM']
        
        # Normalize search term
        search_term = str(hazard_item).strip().lower()
        
        # 각 컬럼에서 매칭 시도 (early exit on first match)
        matched_row = None
        for col in match_columns:
            if col in self.hazard_ref_df.columns:
                # 정확히 일치하는 행 찾기 (대소문자 구분 없이)
                mask = self.hazard_ref_df[col].astype(str).str.strip().str.lower() == search_term
                if mask.any():
                    matched_row = self.hazard_ref_df[mask].iloc[0]
                    break  # Early exit on first match
        
        if matched_row is not None:
            # 출력 필드 추출: M_KOR_NM, ANALYZABLE, INTEREST_ITEM
            info["category"] = matched_row.get("M_KOR_NM") if "M_KOR_NM" in matched_row.index else None
            info["analyzable"] = bool(matched_row.get("ANALYZABLE", False)) if "ANALYZABLE" in matched_row.index else False
            info["interest"] = bool(matched_row.get("INTEREST_ITEM", False)) if "INTEREST_ITEM" in matched_row.index else False
        
        return info

    def scrape(self, max_pages=3):
        """
        데이터 수집 실행
        :param max_pages: 수집할 최대 페이지 수 (기본 3페이지, 약 300건)
        """
        print(f"🚀 [ImpFood] 수입식품정보마루 수집 시작 (Max {max_pages} pages)...")
        records = []
        
        try:
            with sync_playwright() as p:
                # 브라우저 실행 (Headless)
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                
                # Performance optimization: Block images and fonts
                def block_resources(route):
                    """Block images, fonts, and other unnecessary resources"""
                    if route.request.resource_type in ["image", "font", "stylesheet"]:
                        route.abort()
                    else:
                        route.continue_()
                
                page.route("**/*", block_resources)
                
                for current_page in range(1, max_pages + 1):
                    target_url = f"{self.BASE_URL}&page={current_page}"
                    print(f"   Reading Page {current_page}...")
                    
                    page.goto(target_url, timeout=60000)
                    
                    # 리스트 로딩 대기 (첫 번째 카드의 제품명이 뜰 때까지)
                    try:
                        page.wait_for_selector("div.gallery.type2 ul li h4", timeout=10000)
                    except:
                        print(f"   ⚠️ Page {current_page}: 데이터 로딩 실패 또는 끝.")
                        break
                    
                    # 카드 리스트 확보
                    cards = page.locator("div.gallery.type2 ul li").all()
                    print(f"   - Found {len(cards)} items.")
                    
                    if not cards:
                        break
                        
                    for card in cards:
                        try:
                            # 1. ID 추출 (자바스크립트 링크의 ID 속성)
                            # 예: <a href="javascript:fnShowVioltCtnt..." id="202600014999">
                            # Locator를 사용하여 해당 a 태그를 찾음
                            id_locator = card.locator("a[href^='javascript:fnShowVioltCtnt']")
                            if id_locator.count() == 0:
                                continue
                            unique_id = id_locator.first.get_attribute("id")
                            
                            # 2. 날짜 추출
                            date_str = card.locator("span[title='부적합일자']").inner_text().strip()
                            
                            # 3. 제품명 추출 (한글 + 영문)
                            name_kr = card.locator("strong[title='제품한글명']").inner_text().strip()
                            name_en = card.locator("span[title='제품영문명']").inner_text().strip()
                            product_name = f"{name_kr} ({name_en})" if name_en else name_kr
                            
                            # 4. 품목 및 원산지
                            product_type = card.locator("span[title='품목명']").inner_text().strip()
                            origin = card.locator("span[title='제조국가']").inner_text().strip()
                            
                            # 5. 위반내역 (중요: title 속성에서 가져옴)
                            # a 태그 안에 있는 span의 title 속성
                            hazard_item = id_locator.locator("span").get_attribute("title")
                            
                            # Lookup (using updated logic that returns Names)
                            prod_info = self._lookup_product_info(product_type)
                            hazard_info = self._lookup_hazard_info(hazard_item)

                            # 6. 스키마 매핑 (13 Columns)
                            record = {
                                "registration_date": date_str, # 포맷이 이미 YYYY-MM-DD 형태임
                                "data_source": "ImpFood",
                                "source_detail": f"ImpFood-{unique_id}",
                                "product_type": product_type,
                                "top_level_product_type": prod_info["top"],
                                "upper_product_type": prod_info["upper"],
                                "product_name": product_name,
                                "origin_country": origin,
                                "notifying_country": "South Korea",
                                "hazard_category": hazard_info["category"],
                                "hazard_item": hazard_item,
                                "analyzable": hazard_info["analyzable"],
                                "interest_item": hazard_info["interest"]
                            }
                            records.append(record)
                            
                        except Exception as e:
                            # 개별 카드 파싱 에러 시 스킵하고 계속 진행
                            # print(f"Card Error: {e}")
                            continue
                            
                browser.close()
                
        except Exception as e:
            print(f"❌ [ImpFood] Critical Error: {e}")
            return get_empty_dataframe()

        if not records:
            print("⚠️ 수집된 데이터가 없습니다.")
            return get_empty_dataframe()
            
        df = pd.DataFrame(records)
        print(f"✅ [ImpFood] 총 {len(df)} 건 수집 및 정규화 완료.")
        return validate_schema(df)

if __name__ == "__main__":
    # 테스트 실행
    scraper = ImpFoodScraper()
    df = scraper.scrape(max_pages=1)
    print(df.head())
    print(f"Total collected: {len(df)}")