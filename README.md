## Wealth Intelligence Dashboard

This project renders a realtime wealth dashboard that fuses Airtable ?Operations? data with Finnhub market pricing. It is a static frontend (HTML/CSS/JS) that you can serve from any HTTP server as long as the Airtable and Finnhub credentials are configured in `config.local.js` (see `config.sample.js`).

Keep this README up to date whenever you change the Airtable schema, add operation types, or adjust how analytics are computed.

---

## Quick Setup

- Copy `config.sample.js` ? `config.local.js` and populate the values:
  - `FINNHUB_KEY`
  - `AIRTABLE_API_KEY`
  - `AIRTABLE_BASE_ID`
  - `AIRTABLE_TABLE_NAME` (default: `Operations`)
- Host the `/workspace` directory (e.g., `npx http-server .` or any static hosting provider).
- The app throttles UI redraws to every 5?s; you will see log output in the browser console for WebSocket events.

---

## Airtable Maintenance Guide

The dashboard assumes a single table of **operations** that records every cashflow, trade, and valuation event. Each row must provide the fields below (case-sensitive, matching existing schema):

| Field | Required | Purpose |
|-------|----------|---------|
| `Asset` | ? | Display name of the holding. Used as lookup key; keep stable over time. |
| `Category` | ? | Normalised to detect type (`Stock`, `Crypto`, `Real Estate`, `Cash`, etc.). Real-estate analytics rely on `Real Estate`. |
| `Operation type` | ? | Drives the core accounting flow (see table below). |
| `Amount` | ? (for trades) | Quantity of units positive for buys/deposits, negative for sells/withdrawals. |
| `Spent on operation` | ? | Actual cash impact. Positive values mean cash paid out; negative values mean cash received (rent, dividends, sales proceeds). |
| `Asset price on invest date` / `Price` | Optional | Used to derive cost basis when `Spent on operation` is missing. |
| `Date` | ? | ISO date string; operations are processed chronologically. |
| `Tags` | Optional | Array of keywords that refine behaviour (see sections below). |
| `Notes` | Optional | Ignored by the dashboard but useful for human tracking. |

### Operation Type Expectations

Keep the following semantics so calculations remain correct:

| Operation type | Expected sign conventions | Effect |
|----------------|---------------------------|--------|
| `PurchaseSell` | `Amount` > 0 for buys, < 0 for sells. `Spent on operation` should reflect the cash paid (positive) or received (negative). | Updates quantity, cost basis, realized P&L. |
| `ProfitLoss` | `Spent on operation` positive for expenses, negative for income. | Adds to realized P&L and cashflow. Rent is classified here (see below). |
| `DepositWithdrawal` | Use for cash movements unrelated to holdings; positive `Amount` / `Spent` increases cash, negative reduces. | Adjusts cash reserves. |
| Other values | Treated as neutral cashflow unless the tags override classification. | Keeps totals balanced without touching quantities. |

### Tagging Conventions

Tags are inspected in lowercase. Keep the vocabulary consistent:

- **Rent income**: `Operation type = ProfitLoss` **and** include one of the tags: `Rent`, `Rental`, `Lease`, `Tenant`, `Tenancy`, `Airbnb`, `Booking`.
  - Cash inflow can be recorded as negative `Spent on operation` (preferred) or by entering a positive `Amount` with a reference price; the dashboard normalises either form into rent collected.
- **Real-estate expenses**: apply any of `Expense`, `Maintenance`, `Repair`, `Repairs`, `Tax`, `Taxes`, `Property Tax`, `Insurance`, `Mortgage`, `MortgagePayment`, `HOA`, `HOA Fees`, `Utility`, `Utilities`, `Water`, `Electric`, `Electricity`, `Gas`, `Cleaning`, `Management`, `Interest`, `Service`, `Fee`, `Fees`.
- You can add new keywords by extending the arrays `RENT_TAGS` or `EXPENSE_TAGS` in `app.js`.

### Real Estate Analytics (Rental Portfolio)

Assets where `Category` normalises to `Real Estate` get an additional card summarising rental performance:

- **Final Asset Price** = absolute cash paid for purchases + absolute cash paid on expenses.
- **Outstanding** = `max(0, Final Asset Price - Rent Collected)`.
- **Rent YTD** = aggregate rent where `Date` falls in the current calendar year.
- **Rent / Mo** = average of the last 12 months with rent activity (or fewer if insufficient history).
- **Utilization** = rent collected ? final asset price (clamped to 100%).
- **Payoff ETA** = months required to clear `Outstanding` using the trailing average monthly rent (shows ?Paid off? when ? 0).

If you do not see values, confirm:

- The asset rows carry `Category = Real Estate`.
- Rent inflows follow the tag/operation pattern above.
- `Spent on operation` contains the true cash amount (negative for income).

### Data Hygiene Tips

- **Chronological integrity**: ensure `Date` is set; missing dates default to record insertion order which can break cost basis.
- **Consistent asset naming**: rename carefully; the dashboard merges rows by `Asset` string.
- **Populate `Spent on operation`** even if `Amount`/`Price` exist?this is the trusted cashflow.
- **Use tags liberally** to distinguish expenses vs. rent vs. other Profit/Loss items.
- **Prevailing currency**: all monetary values are treated in a single currency (usually USD). Mixed currencies will distort totals.

---

## Frontend Reference

- **HTML**: `index.html` defines layout.
- **Styles**: `styles.css` contains theme variables, animations, and card layouts.
- **Logic**: `app.js` handles Airtable fetch, Finnhub integration, chart updates, KPI flashes, rental analytics, and real-time throttling.
- **Config**: Keep `config.local.js` out of version control (already git-ignored).

### Charts

- Sparkline: recent total P&L values.
- Bar/Pie charts: P&L by asset, composition, daily change, allocation.
- ?Asset Growth Over Years?: cumulative net contributions by calendar year per asset.

Charts update in place without recreation to avoid UI jitter; extending metrics should re-use the `createOrUpdateChart` helper.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Net worth or P&L looks off | Missing/incorrect `Spent on operation` values | Populate cashflow column accurately. |
| Rental card empty | Tags/operation mismatch or category not `Real Estate` | Align with conventions in this README or extend tag arrays. |
| Rent / Mo shows ? | Less than 1 month of rent data | Add historical rent entries with correct tags. |
| Charts flicker | Serving page from local file system may block Finnhub WebSocket | Host over HTTP/HTTPS. |

---

## Change Log

 - `2025-11-02`: Documented rental analytics, tag conventions, required Airtable fields, and clarified final asset price/utilization definitions.
