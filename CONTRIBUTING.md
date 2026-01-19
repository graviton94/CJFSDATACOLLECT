# 🤝 Contributing to CJFSDATACOLLECT

This project embraces **"Vibe Coding"**. We use AI agents (Gemini CLI, Copilot) to generate code but enforce strict verification standards.

## 🤖 AI Vibe Coding Guidelines
1. **Context is King:** Always load `ARCHITECTURE.md` into your AI context before asking for major changes.
2. **Review First:** Do not blindly commit AI-generated code. Verify imports and logic errors.
3. **Modular Prompts:** Ask the AI to build small, testable functions (e.g., "Write a parser for this specific HTML table") rather than the whole system at once.

## 📝 Coding Standards
- **Language:** Python 3.10+
- **Style:** PEP 8 (mostly). Snake_case for variables/functions.
- **Docstrings:** Required for all public functions (Google Style).
- **Type Hinting:** Strongly encouraged (e.g., `def scrape(url: str) -> pd.DataFrame:`).

## 🔄 Workflow
1. Create a feature branch: `git checkout -b feature/rasff-scraper`
2. Implement logic (using Gemini CLI).
3. Run local tests: `pytest` (or manual run).
4. Commit with descriptive messages: `feat: add playwright support for rasff`

## 🚫 Anti-Patterns
- Hardcoding API keys (Use `.env`).
- Committing heavy raw data files (Add `*.csv`, `*.json` to `.gitignore`, keep only `parquet` or sample data).
