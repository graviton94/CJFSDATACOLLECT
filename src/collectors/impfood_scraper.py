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
        # 기준정보 로드 (Lookup용)
        self.product_ref = self._load_reference("product_code_master.parquet", "PRDLST_NM")
        self.hazard_ref = self._load_reference("hazard_code_master.parquet", "TEST_ITM_NM")

    def _load_reference(self, filename, index_col):
        """Parquet 파일을 읽어 검색용 Dict로 변환"""
        file_path = self.REF_DIR / filename
        if not file_path.exists():
            return {}
        try:
            df = pd.read_parquet(file_path)
            if index_col not in df.columns: return {}
            return df.drop_duplicates(subset=[index_col]).set_index(index_col).to_dict('index')
        except: return {}

    def _lookup_info(self, product_type, hazard_item):
        """기준정보 매핑 로직"""
        p_info = {"top": None, "upper": None}
        if product_type in self.product_ref:
            row = self.product_ref[product_type]
            p_info["top"] = row.get("GR_NM") or row.get("HRNK_PRDLST_NM")
            p_info["upper"] = row.get("PRDLST_CL_NM")
            
        h_info = {"cat": "Uncategorized", "analyzable": False, "interest": False}
        # 시험항목은 정확히 일치하지 않을 수 있어 포함 여부로 체크 (간이 로직)
        # 실무에선 Fuzzy Matching 필요하지만, 여기선 Exact Match 우선
        if hazard_item in self.hazard_ref:
            row = self.hazard_ref[hazard_item]
            h_info["cat"] = row.get("LCLS_NM", "Uncategorized")
            # h_info["analyzable"] = ... (백서 컬럼에 따라)
            
        return p_info, h_info

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
                            
                            # Lookup
                            p_info, h_info = self._lookup_info(product_type, hazard_item)

                            # 6. 스키마 매핑 (13 Columns)
                            record = {
                                "registration_date": date_str, # 포맷이 이미 YYYY-MM-DD 형태임
                                "data_source": "ImpFood",
                                "source_detail": f"ImpFood-{unique_id}",
                                "product_type": product_type,
                                "top_level_product_type": p_info["top"],
                                "upper_product_type": p_info["upper"],
                                "product_name": product_name,
                                "origin_country": origin,
                                "notifying_country": "South Korea",
                                "hazard_category": h_info["cat"],
                                "hazard_item": hazard_item,
                                "analyzable": h_info["analyzable"],
                                "interest_item": h_info["interest"]
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
        return validate_schema(df)

if __name__ == "__main__":
    # 테스트 실행
    scraper = ImpFoodScraper()
    df = scraper.scrape(max_pages=1)
    print(df.head())
    print(f"Total collected: {len(df)}")
