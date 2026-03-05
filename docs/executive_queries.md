# Executive Queries (FAF Freight Data Warehouse)

This page contains a small set of “exec-ready” queries: each one answers a leadership question and includes a “how to read” guide.

Run `sql/analytics/00_params.sql` first to set:
- `@YEAR` (snapshot year)
- `@Y1`, `@Y2` (year-over-year comparison)

---

## Exec Query 1 — Top corridors by value (Where is the business concentrated?)

**SQL file:** `sql/analytics/12_top_origin_dest_pairs_named.sql`

### How to read
- Each row is an origin/destination pair (named zones) with total shipment value.
- Look for concentration: are the top 5–10 pairs disproportionately large?
- If the top rows are “origin == destination”, interpret as metro-internal freight intensity.

### What an exec can decide
- Which corridors deserve priority monitoring and forecasting
- Where to focus capacity and reliability investments
- Where to investigate unusual spikes or drops

---

## Exec Query 2 — Mode mix (% share of total) (How are we shipping overall?)

**SQL file:** `sql/analytics/03_exec_mode_mix.sql`

### How to read
- Shows total value by mode and share of total (%).
- If one mode dominates, you have operational risk concentration.
- Compare with YoY mode delta: `sql/analytics/06_mode_yoy_delta.sql`

### What an exec can decide
- Resilience strategy (multi-mode planning)
- Where cost increases or disruptions will hit hardest
- Which mode trends require explanation

---

## Exec Query 3 — Distance band distribution (How far are shipments going?)

**SQL files:**
- Snapshot: `sql/analytics/09_distance_band_snapshot.sql`
- YoY shift: `sql/analytics/10_distance_band_yoy.sql`

### How to read
- Snapshot shows the distribution across distance bands:
  - tons (weight)
  - tmiles (distance-weighted movement)
- YoY shows whether the network is shifting toward shorter or longer shipments.

### What an exec can decide
- Whether the network is becoming more local or more long-haul
- Which logistics strategy is implied (warehouse placement, carrier mix, cost structure)
- Whether policy/cost signals are reshaping freight movement patterns