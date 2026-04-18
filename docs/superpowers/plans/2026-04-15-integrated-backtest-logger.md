# Integrated Backtest Trade Logger Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upload every individual trade result from a backtest into a single Markdown table field in Airtable.

**Architecture:** Format the `results` list into a Markdown string and push it to the `Simulation Results` column in the `Historical/Backtest` table.

**Tech Stack:** Python, Airtable API.

---

### Task 1: Update airtable_push.py

**Files:**
- Modify: `backend/airtable_push.py:312-387`

- [ ] **Step 1: Update push_backtest_result to handle simulation_results**

```python
def push_backtest_result(metrics: dict) -> str:
    # ... (existing run_date logic)
    fields = {
        "Test Name":    metrics["test_name"],
        "Run Date":     run_date,
        "Module":       metrics["module"],
        "Date Range Start": metrics["date_range_start"],
        "Date Range End":   metrics["date_range_end"],
        "Total Trades": int(metrics["total_trades"]),
        "Win Rate":     float(metrics["win_rate"]),
        "Average Return": float(metrics["average_return"]),
        "Total Return": float(metrics["total_return"]),
    }
    
    # Add Simulation Results if present
    if "simulation_results" in metrics:
        fields["Simulation Results"] = metrics["simulation_results"]
    
    # ... (rest of optional_floats and optional_strings)
```

- [ ] **Step 2: Verify logic and Commit**
Check that `optional_strings` loop handles it or specifically add it as a key.

```bash
git add backend/airtable_push.py
git commit -m "feat(airtable): add simulation results field to backtest push"
```

### Task 2: Implement Markdown Formatting in backtester.py

**Files:**
- Modify: `backend/backtester.py:160-235` (Add utility)
- Modify: `backend/backtester.py:473-543` (Update orchestrator)

- [ ] **Step 1: Add format_results_table utility**

```python
def format_results_table(results: list[dict]) -> str:
    """Format individual trade results into a Markdown table string."""
    header = "| Ticker | Outcome | Return % | Exit Date | Reason | Score |\n"
    separator = "|--------|---------|----------|-----------|--------|-------|\n"
    rows = []
    for r in results:
        rows.append(
            f"| {r['ticker']:8} | {r['outcome']:7} | {r['return_pct']:+8.2f}% | "
            f"{r['exit_date']} | {r['exit_reason']:12} | {r['score']:5.1f} |"
        )
    return header + separator + "\n".join(rows)
```

- [ ] **Step 2: Update run_backtest to use the utility**

```python
def run_backtest(module: str, start_str: str, end_str: str):
    # ... (Step 1 & 2 logic)
    metrics = compute_metrics(results, module, start_str, end_str)

    # NEW: Add individual results log
    metrics["simulation_results"] = format_results_table(results)
    
    # ... (Step 3 & 4 logic)
```

- [ ] **Step 3: Commit**

```bash
git add backend/backtester.py
git commit -m "feat(backtest): format and include individual trade logs in Airtable push"
```

### Task 3: Manual Verification

- [ ] **Step 1: Run a backtest**

Run: `python backend/backtester.py --module Technical_Under_5 --start 2025-01-01 --end 2025-01-05`
Expected: Successful run, log shows "✓ Pushed to Airtable".

- [ ] **Step 2: Verify in Airtable**
Open the Airtable link provided in the logs.
Confirm: The new record has the "Simulation Results" column populated with a readable table.
