# 📋 Technical Specification: FDA Import Alert Precision Parser (v3.2)

## 1. Project Context & Goal

**목적**: FDA Import Alert 상세 페이지의 비구조화된(Unstructured) HTML에서 **국가 - 제품 - 날짜 - 상세내용**을 하나의 데이터 블록으로 정밀하게 매핑하여 수집.
**핵심 도전 과제**: 데이터가 Container(div 등)로 묶여 있지 않고 Sibling(대등한 위치의 형제 노드)으로 나열되어 있어, 단순한 위/아래 탐색으로는 단락이 섞임.

## 2. Technology Stack

**Language**: Python 3.10+
**Library**:
* `BeautifulSoup4`: DOM Tree 내비게이션 및 형제 노드 탐색.
* `re` (Regex): 제품 코드(`\d{2} [A-Z] - - \d{2}`) 및 날짜(`MM/DD/YYYY`) 패턴 매칭.
* `Pandas` & `Pyarrow`: 최종 데이터를 15개 컬럼 표준 스키마로 변환 및 Parquet 저장.



## 3. Parsing Logic: State-Machine Accumulation (핵심 원리)

에이전트가 구현해야 할 알고리즘의 핵심은 **'상태 유지 누적 파싱'**입니다.

1. **Segmenting**: `div.center > h4`를 만나면 새로운 국가(Country) 섹션 시작.
2. **State Logic**: 국가 섹션 내의 모든 Sibling 노드를 루프(Loop)로 순회하며 다음 '상태'를 감지:
* **Condition A (Product Code)**: 정규식 `(\d{2} [A-Z] - - \d{2})`가 포함된 노드를 발견하면 **새로운 레코드 객체**를 생성하고 이전 레코드는 리스트에 저장.
* **Condition B (Date)**: `Date Published:` 텍스트 발견 시 해당 블록의 `registration_date`로 확정.
* **Condition C (Content)**: 위 조건이 아닌 모든 텍스트/태그 노드는 현재 레코드의 `full_text` 버퍼에 순차적으로 누적(Append).


3. **Filtering**: 수집된 레코드 중 `registration_date`가 사용자가 요청한 `target_date`와 일치하는 것만 최종 결과에 포함.

## 4. Final Agent Instructions (프롬프트 복사용)

> **[Copy & Paste this to Copilot]**
> "FDA Import Alert 상세 페이지(예: IA 64-01)를 파싱하는 `fda_collector.py`를 수정하라.
> **구현 지침:**
> 1. **DOM 탐색**: `soup.find_all('div', class_='center')`로 국가 헤더를 먼저 찾고, 각 헤더 이후의 모든 형제 노드(`next_siblings`)를 순회하며 파싱하라.
> 2. **상태 기반 추출**:
> * `re.search(r'(\d{2} [A-Z] - - \d{2})', node_text)`를 만나면 이를 'Product Code'로 인식하고 새로운 데이터 블록 작성을 시작하라.
> * `Date Published:`가 포함된 노드를 만나면 해당 블록의 날짜로 할당하라.
> * 다음 Product Code나 다음 국가 헤더를 만나기 전까지의 모든 텍스트 노드 내용을 `full_text`에 누적하라.
> 
> 
> 3. **데이터 정제**:
> * `product_name`은 제품 코드 뒷부분의 텍스트만 추출하고, 불필요한 특수문자를 제거하라.
> * `registration_date`는 `YYYY-MM-DD` 형식으로 정규화하라.
> 
> 
> 4. **필터링**: 추출된 블록 중 날짜가 `target_date_str`과 일치하는 레코드만 반환하라.
> 5. **스키마 준수**: 결과는 반드시 프로젝트 표준인 15개 컬럼 UNIFIED_SCHEMA와 일치해야 하며, `validate_schema` 함수를 통과해야 한다.
> 
> 
> **구조적 특징 참고**: FDA 페이지는 데이터가 계층형 컨테이너에 담겨 있지 않으므로, 반드시 순차적 루프와 상태 플래그를 사용하여 블록의 시작과 끝을 판별해야 한다."
