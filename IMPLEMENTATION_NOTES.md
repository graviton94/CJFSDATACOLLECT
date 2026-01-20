# 📝 Implementation Notes & Decisions

## 2026-01-20: Schema & ImpFood Integration

### Decision 1: Unified Schema Enforcement
- **Context:** Old schema columns (`id`, `ref_no`) were causing confusion in the Dashboard.
- **Decision:** Updated `storage.py` to aggressively drop any column not in `UNIFIED_SCHEMA`.
- **Result:** Dashboard now displays clean, consistent data.

### Decision 2: Playwright for ImpFood
- **Context:** `impfood.mfds.go.kr` stores key data (ID, Violation Detail) in DOM attributes, not in API responses.
- **Decision:** Used Playwright to extract `title` attributes and `id` attributes from the DOM.
- **Result:** Successfully scraping detailed data without complex API reverse-engineering.

### Decision 3: Name-Based Lookup
- **Context:** The original lookup logic tried to fetch "Product Codes" (e.g., A01), but the Dashboard users prefer "Product Names" (e.g., Agricultural Products).
- **Decision:** Refactored `_lookup_product_info` to search and return names (Values ending in `_NM`) from the Reference Parquet.

### Technical Debt / TODO
- Refactor `ImpFood.py` filename to `impfood_scraper.py` to match naming convention (Snake Case).
- Enhance `reference_loader.py` to handle missing columns gracefully during initialization.
