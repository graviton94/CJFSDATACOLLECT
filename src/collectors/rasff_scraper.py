import pandas as pd
from datetime import datetime
from playwright.sync_api import sync_playwright
from src.schema import UNIFIED_SCHEMA, validate_schema, get_empty_dataframe

class RASFFCollector:
    """
    EU RASFF Scraper (Playwright 기반, Schema v2 적용)
    """
    URL = "https://webgate.ec.europa.eu/rasff-window/screen/search"
    
    def scrape(self, days_back=7):
        print(f"🚀 [RASFF] 수집 시작 (Last {days_back} days)...")
        records = []
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(self.URL, timeout=60000)
                
                # 테이블 로딩 대기
                try:
                    page.wait_for_selector("table tbody tr", timeout=30000)
                except:
                    print("⚠️ RASFF 테이블 로딩 실패 (Timeout)")
                    return get_empty_dataframe()

                # 데이터 추출
                rows = page.query_selector_all("table tbody tr")
                print(f"   Found {len(rows)} rows.")
                
                for row in rows:
                    cols = row.query_selector_all("td")
                    if not cols: continue
                    
                    # RASFF 컬럼 순서 (가정): Date | Reference | Country | Subject | Category ...
                    # 실제 사이트 변경 시 인덱스 수정 필요
                    try:
                        raw_date = cols[0].inner_text().strip()
                        ref_no = cols[1].inner_text().strip()
                        origin = cols[2].inner_text().strip()
                        subject = cols[3].inner_text().strip()
                        category = cols[4].inner_text().strip()
                        
                        # 날짜 변환 (DD/MM/YYYY -> YYYY-MM-DD)
                        try:
                            dt = datetime.strptime(raw_date, "%d/%m/%Y")
                            reg_date = dt.strftime("%Y-%m-%d")
                        except:
                            reg_date = datetime.now().strftime("%Y-%m-%d")
                            
                        # 14개 컬럼 매핑
                        records.append({
                            "registration_date": reg_date,
                            "data_source": "RASFF",
                            "source_detail": ref_no,
                            "product_type": category,
                            "top_level_product_type": None,
                            "upper_product_type": None,
                            "product_name": subject, # RASFF는 Subject에 제품명이 포함됨
                            "origin_country": origin,
                            "notifying_country": "EU Member States",
                            "hazard_category": "Uncategorized",
                            "hazard_item": subject, # 상세 내용을 위해 Subject 중복 사용
                            "full_text": None,  # RASFF does not provide full text context in current implementation
                            "analyzable": False,
                            "interest_item": False
                        })
                    except Exception as e:
                        continue
                        
                browser.close()
                
        except Exception as e:
            print(f"❌ Playwright Error: {e}")
            return get_empty_dataframe()

        if not records:
            return get_empty_dataframe()
            
        df = pd.DataFrame(records)
        return validate_schema(df)

if __name__ == "__main__":
    c = RASFFCollector()
    print(c.scrape().head())