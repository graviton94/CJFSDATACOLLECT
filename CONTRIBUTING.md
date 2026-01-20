# 🤝 Contributing to CJFSDATACOLLECT

## 1. Development Workflow

1.  **Fork & Clone** the repository.
2.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    playwright install
    ```
3.  **Create a Branch:** `feature/your-feature-name` or `fix/issue-number`.

## 2. Adding a New Collector

If you want to add a new data source (e.g., China CFDA):

1.  Create `src/collectors/china_collector.py`.
2.  Implement the `collect()` or `scrape()` method.
3.  Ensure the output DataFrame strictly follows `src.schema.UNIFIED_SCHEMA`.
4.  Register the collector in `src/scheduler.py`.

## 3. Schema Rules

- **DO NOT** add new columns to the final output without team discussion.
- All collectors **MUST** map their raw data to the 13 unified columns.
- Use `src.schema.validate_schema(df)` before returning data.

## 4. Testing

Run tests before pushing:
```bash
# Run all tests
pytest tests/
```
