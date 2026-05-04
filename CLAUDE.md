# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DRE Interativo is a Flask web dashboard for visualizing **DRE (Demonstração do Resultado do Exercício)** — Income Statements — for Grupo Nordestão, a Brazilian supermarket chain. It reads `.xlsx` Excel files and renders interactive financial reports.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the development server (http://localhost:5000)
python app.py

# Override port
PORT=8080 python app.py
```

There are no tests in this project.

## Architecture

**Single-file backend:** All logic lives in [app.py](app.py). The app reads Excel workbooks from `consolidado/` (finalized monthly) or `parcial/` (in-progress weekly) directories and renders them through a single Jinja2 template.

**Data flow:**
1. Excel files (`DRE_LOJAS_MÊSANO.xlsx`, e.g. `DRE_LOJAS_FEV26.xlsx`) are read via `ler_excel_cached()` (LRU-cached, maxsize=16)
2. `listar_lojas()` extracts store names from row 3 of sheet `'DRE'`
3. `_extrair_dados_loja()` reads each store's columns: `[Real prev_year, %, Budget, %, Real curr_year, %, Var Orc, Var YoY]`
4. `carregar_dre()` / `carregar_dre_grupo()` normalizes percentages, classifies account types, and returns enriched data
5. Flask renders `templates/index.html` with the full dataset (no JS-driven AJAX for the main view)

**Store groups** (defined in `app.py`):
- `ORDEM_VAREJO`: 9 Nordestão retail stores
- `ORDEM_ATACADO`: 4 Superfácil wholesale stores
- `LOJAS_MOSSORO`: Mossoró-region stores (subset of Varejo)
- Classification logic: store name contains `"SUPERFÁCIL"` → Atacado, else → Varejo

**Account hierarchy:** `CONTAS_PRINCIPAIS` (69 entries) defines main account rows. `ESTRUTURA_DRE` maps each account to `{nivel, tipo, expansivel}`. Subitems are collapsed by default; clicking a row in the UI expands children.

**Group logos** are served from `static/` (`logo.png` = Nordestão, `facil.png` = Superfácil, `mosso.png` = Mossoró) and selected dynamically based on the active group.

## Key Conventions

- **All variable and function names are in Portuguese** (`conta`, `loja`, `periodo`, `carregar_dre`, etc.)
- Month abbreviations: `JAN, FEV, MAR, ABR, MAI, JUN, JUL, AGO, SET, OUT, NOV, DEZ`
- Period format: `"JAN/25"`, `"FEV/26"` — the year is the last 2 digits
- `tipo` query param: `"consolidado"` or `"parcial"`
- `grupo` query param: `"varejo"`, `"atacado"`, or `"mossoro"` (takes precedence over `loja`)

## Flask Routes

| Route | Purpose |
|---|---|
| `GET /` | Renders full HTML page; accepts `periodo`, `tipo`, `loja`, `grupo` query params |
| `GET /api/periodos` | Returns available periods as JSON (LRU-cached) |
| `GET /api/dre` | Returns DRE data as JSON for a given period/loja/grupo |

## Adding New Periods

Drop a new `.xlsx` file in `consolidado/` following the naming pattern `DRE_LOJAS_MÊSANO.xlsx`. The file must have a sheet named `'DRE'` with store names in row 3. The app auto-discovers files on startup (cache cleared on restart).
