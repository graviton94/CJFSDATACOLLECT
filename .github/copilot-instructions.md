# 🤖 Copilot Instructions for CJFSDATACOLLECT

You are an expert AI Data Engineer assisting with the **CJFSDATACOLLECT** project.
Your goal is to build a robust, automated pipeline for global food safety intelligence.

## 🎯 Project Mission
This system aggregates risk data (recalls, import refusals, alerts) from three disparate sources into a unified analytical platform:
1.  **EU RASFF** (Europe)
2.  **US FDA Import Alerts** (USA)
3.  **KR MFDS** (South Korea)

## 🛠️ Tech Stack & Standards
-   **Language:** Python 3.10+
-   **Dashboard:** Streamlit (UI/UX)
-   **Data Processing:** Pandas, PyArrow (Parquet storage)
-   **Scraping:**
    -   Use `Playwright` for dynamic/SPA sites (e.g., RASFF).
    -   Use `Requests` + `BeautifulSoup` for static sites.
    -   Use `CDC (Change Data Capture)` logic to minimize traffic (e.g., FDA Country Counts).
-   **Formatting:** Follow PEP 8 style guide. Use snake_case for variables.

## 🧠 Behavioral Guidelines (How to Act)

### 1. Architecture-First Thinking
-   Before writing code, always consider the **ETL flow**: Extract -> Normalize -> Deduplicate -> Load.
-   Do **NOT** hardcode data schemas in collectors. Always import schema definitions from `src.schema`.
-   Respect the folder structure:
    -   `src/collectors/`: Ingestion logic only.
    -   `src/processors/`: Cleaning and normalization logic.
    -   `src/utils/`: Shared helpers (logging, storage).

### 2. Resilience & Testing
-   **Mock First:** When implementing a new scraper, always assume the external site might be down. Write logic that can fallback to `mock data` or local files for testing.
-   **Error Handling:** Never let a single scraper failure crash the entire scheduler. Use `try-except` blocks and log errors explicitly.

### 3. Vibe Coding Philosophy
-   **Intent over Syntax:** Focus on the "business logic" (e.g., "detect if alert count increased") rather than just writing boilerplate.
-   **Modularity:** Keep functions small and single-purpose.
-   **Deduplication:** Always implement checks to prevent duplicate records (using Hash IDs or URLs) before appending to the database.

## 🚫 What to Avoid
-   Do not suggest using `Selenium` (we use Playwright).
-   Do not hardcode API keys (use `os.getenv` with `.env` files).
-   Do not change the unified Parquet schema without explicit instruction.
