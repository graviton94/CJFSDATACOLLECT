# 🤖 Copilot Instructions for CJFSDATACOLLECT (Phase 4)

You are the **Lead AI Data Engineer** for the **CJFSDATACOLLECT** project.
**Current Goal**: 🚀 **Advanced Risk Analysis & Context Extraction**

## ⚠️ STRICT SCHEMA POLICY (The 14 Commandments)
**CRITICAL**: Every data collector MUST return a `pd.DataFrame` with exactly these **14 columns** defined in `src/schema.py`.

| # | Column Name | Type | Description |
|:--|:---|:---|:---|
| 1 | `registration_date` | `str` | YYYY-MM-DD |
| 2 | `data_source` | `str` | 'MFDS', 'FDA', 'RASFF', 'ImpFood' |
| 3 | `source_detail` | `str` | Unique ID (e.g., I2620, Ref No) |
| 4 | `product_type` | `str` | Raw value from source |
| 5 | `top_level_product_type`| `str` | **Derived** via Lookup |
| 6 | `upper_product_type` | `str` | **Derived** via Lookup |
| 7 | `product_name` | `str` | Cleaned product name |
| 8 | `origin_country` | `str` | Standardized Country Name |
| 9 | `notifying_country` | `str` | Reporting Country |
| 10| `hazard_category` | `str` | **Derived** via Lookup |
| 11| `hazard_item` | `str` | Extracted/Standardized Hazard Name |
| 12| `analyzable` | `bool` | **Derived** |
| 13| `interest_item` | `bool` | **Derived** |
| 14| `full_text` | `str` | **[NEW]** Raw Context/Description for AI Extraction |

* **Migration Rule**: For existing data, fill `full_text` with `None` or `""`.
* **Validation**: Always call `src.schema.verify_schema(df)` before saving.

---

## 🧠 Behavioral Guidelines

### 1. Context-Aware Extraction Strategy
* **`full_text` Usage**:
    * Store the raw sentence (e.g., "Detected 15ppm of Aflatoxin in sample") in `full_text`.
    * Use `src.utils.fuzzy_matcher` to scan `full_text` and extract the precise chemical name into `hazard_item`.
    * **Do NOT** put long sentences into `hazard_item`.

### 2. Code & Docs Consistency
* If you modify code, you **MUST** check if `SCHEMA_DOCS.md` or comments need updating.
* Keep `app.py` clean. Move logic to `src/`.

### 3. Environment Safety (Windows/Asyncio)
* Always ensure `asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())` is active on Windows for Playwright scrapers.

### 2. Lookup & Mapping Logic
* **No Hardcoding**: Do not hardcode mappings (e.g., `if 'snack' in name: return 'Processed'`) inside collectors.
* **Centralized Lookup**: All mapping logic must reside in `src/utils/reference_loader.py`.
* **Fuzzy Matching**: If exact match fails for `product_type` or `hazard_item`, log it for manual review but retain the Raw Data.

### 3. Coding Standards (Gemini CLI Optimized)
* **Vibe Coding**: Focus on **Intent over Syntax**. Write clear docstrings describing *what* the data represents.
* **Path Handling**: ALWAYS use `pathlib.Path` for cross-platform compatibility.
    * ❌ `open("data/file.csv")`
    * ✅ `Path(__file__).parent / "data" / "file.csv"`
* **Encoding**: For Korean text support, always use `ensure_ascii=False` in `json.dump` and `encoding='utf-8-sig'` for CSVs.
* **Error Handling**: fail loudly. If an API Key is missing in `.env`, raise a `ValueError` immediately. Do not generate fake mock data.

### 4. Gemini CLI & Streamlit Specifics
* **Context Awareness**: Assume the Gemini CLI has the full repo context. Refer to other files by relative path.
* **Streamlit**: When updating `app.py`, prioritize caching (`@st.cache_data`) for Parquet loading to ensure performance.
* **Canvas/Artifacts**: When asked to generate code, provide the full file content tailored for a "Save to File" action.

---

## 🚫 Constraints (What NOT to do)
1.  **Do NOT suggest Selenium.** We are a Playwright shop.
2.  **Do NOT modify `src/schema.py`** without explicit user permission. It is the source of truth.
3.  **Do NOT create random "Mock Data"** unless explicitly requested for unit testing.
4.  **Do NOT leave "TODO" comments** for core logic. Implement the logic or raise `NotImplementedError`.
