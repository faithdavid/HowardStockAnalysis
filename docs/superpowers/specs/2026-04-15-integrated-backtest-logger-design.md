# Design Spec: Integrated Backtest Trade Logger

## 1. Goal
Provide a detailed "paper trail" for historical backtests by uploading individual trade results (ticker, entry, exit, outcome, reason) into the existing "Historical/Backtest" Airtable view.

## 2. Architecture
The system will now include a formatting step between computation and upload.
- **Data Source:** `backtester.py` generates a `results` list of dictionary objects.
- **Linker:** `airtable_push.py` will be updated to accept this raw list, format it into a Markdown table, and push it to a new field.

## 3. Schema Changes (Airtable)
We will add one new field to the **Historical/Backtest** table:
- **Field Name:** `Simulation Results`
- **Type:** `Long Text` (Enable Rich Text/Markdown)

## 4. Component Changes

### 4.1 backend/airtable_push.py
- Update `push_backtest_result(metrics)` to handle the `simulation_results` key.
- Add logic to map `simulation_results` to the new Airtable column.

### 4.2 backend/backtester.py
- Add a helper `format_results_table(results)` that converts the list of dicts into a Markdown string.
- Update `run_backtest` to call this helper and include the string in the `metrics` dict.

## 5. Verification Plan
1. Run a 5-day backtest for any module.
2. Confirm the record is created in Airtable.
3. Open the "Simulation Results" field in Airtable and verify the Markdown table is readable.
