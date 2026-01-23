import os
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from src.schema import UNIFIED_SCHEMA, validate_schema, get_empty_dataframe

class FDACollector:
    """
    US FDA Import Alert 수집기 (Schema v2 적용)
    """
    
    BASE_URL = "https://www.accessdata.fda.gov/cms_ia"
    LIST_URL = f"{BASE_URL}/countrylist.html"
    STATE_FILE = "data/state/fda_counts.json"
    
    def __init__(self):
        os.makedirs(os.path.dirname(self.STATE_FILE), exist_ok=True)

    # ... [get_current_counts, load_previous_counts, save_current_counts는 기존 로직 유지] ...
    # 지면 관계상 핵심 파싱 로직인 parse_detail_page를 중점적으로 수정합니다.

    def get_current_counts(self):
        # (기존과 동일하므로 생략 가능하지만, 실행을 위해 간략 버전 포함)
        try:
            response = requests.get(self.LIST_URL, timeout=15)
            soup = BeautifulSoup(response.text, 'html.parser')
            country_map = {}
            for table in soup.find_all('table'):
                for row in table.find_all('tr'):
                    cols = row.find_all('td')
                    if len(cols) < 2: continue
                    link = cols[0].find('a')
                    if not link: continue
                    
                    try: count = int(cols[1].text.strip())
                    except: count = 0
                    
                    href = link.get('href')
                    code = href.split('_')[-1].replace('.html', '')
                    country_map[code] = {"name": link.text.strip(), "url": f"{self.BASE_URL}/{href}", "count": count}
            return country_map
        except: return {}

    def load_previous_counts(self):
        if os.path.exists(self.STATE_FILE):
             try:
                with open(self.STATE_FILE, 'r') as f:
                    return json.load(f)
             except json.JSONDecodeError:
                return {}
             except Exception:
                return {}
        return {}

    def save_current_counts(self, data):
        with open(self.STATE_FILE, 'w') as f:
            json.dump({k: v['count'] for k, v in data.items()}, f)

    def parse_detail_page(self, country_info):
        """상세 페이지 파싱 및 신규 스키마 매핑"""
        url = country_info['url']
        country_name = country_info['name']
        print(f"   Make request to -> {country_name}")
        
        results = []
        try:
            response = requests.get(url, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # FDA 테이블 구조 파싱 (일반적인 구조 가정)
            for table in soup.find_all('table'):
                rows = table.find_all('tr')
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) < 3: continue
                    
                    try:
                        # 예시 매핑 (실제 HTML 구조에 따라 인덱스 조정 필요)
                        # 보통: Alert # | Date | Product | Manufacturer | Charge(Reason)
                        alert_num = cols[0].text.strip()
                        pub_date_raw = cols[1].text.strip()
                        product_desc = cols[2].text.strip()
                        reason_desc = cols[-1].text.strip()
                        
                        # 날짜 변환 (MM/DD/YYYY -> YYYY-MM-DD)
                        try:
                            dt = datetime.strptime(pub_date_raw, "%m/%d/%Y")
                            reg_date = dt.strftime("%Y-%m-%d")
                        except:
                            reg_date = datetime.now().strftime("%Y-%m-%d")

                        # 13개 컬럼 매핑
                        record = {
                            "registration_date": reg_date,
                            "data_source": "FDA",
                            "source_detail": f"Import Alert {alert_num}",
                            "product_type": "Imported Food", # FDA는 유형이 비정형이라 고정 혹은 파싱 필요
                            "top_level_product_type": None, # Lookup 추후 적용
                            "upper_product_type": None,
                            "product_name": product_desc,
                            "origin_country": country_name,
                            "notifying_country": "United States",
                            "hazard_category": "Uncategorized", # Reason 분석 필요
                            "hazard_item": reason_desc,
                            "analyzable": False,
                            "interest_item": False
                        }
                        results.append(record)
                    except: continue
        except Exception as e:
            print(f"   ⚠️ Error parsing {country_name}: {e}")
        
        return results

    def collect(self):
        print("🚀 [FDA] 수집 시작 (CDC Mode)...")
        curr = self.get_current_counts()
        prev = self.load_previous_counts()
        
        target_countries = []
        for code, info in curr.items():
            if info['count'] > prev.get(code, 0):
                target_countries.append(info)
        
        # 최초 실행 시 테스트로 1개 강제 추가
        if not prev and curr and not target_countries:
            target_countries.append(list(curr.values())[0])
            
        all_records = []
        for country in target_countries:
            all_records.extend(self.parse_detail_page(country))
            
        self.save_current_counts(curr)
        
        if not all_records:
            print("✅ 변경 사항 없음.")
            return get_empty_dataframe()
            
        df = pd.DataFrame(all_records)
        return validate_schema(df)

if __name__ == "__main__":
    c = FDACollector()
    print(c.collect().head())