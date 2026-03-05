# Business Narratives (FAF Freight Data Warehouse)

This document turns the analytics SQL queries into decision-ready narratives.
All results are reproducible using the SQL scripts under `sql/analytics/`.

---

## How to run (recommended workflow)

1) Open MySQL Workbench and connect to your local instance.
2) Run this first:
- `sql/analytics/00_params.sql` (sets `@YEAR`, `@Y1`, `@Y2`)

3) If Query #9 was slow before, run the support index script once:
- `sql/ddl/06_analytics_support_indexes_pr7.sql`

4) Then run queries in order:
- `sql/analytics/01_...` through `sql/analytics/12_...`

> Notes:
> - Many narratives use `@YEAR` as a “latest-year snapshot”. You can change it anytime in `00_params.sql`.
> - A few queries are YoY (year-over-year) and use `@Y1` and `@Y2`.

---

## Narrative 1 — “Where is the money moving?” (Top lanes + named pairs + winners/losers YoY)

### Queries
- `01_exec_top_lanes_value.sql` (top lanes by value)
- `12_top_origin_dest_pairs_named.sql` (named origin/destination lanes)
- `05_lane_yoy_delta.sql` (which lanes grew or shrank YoY)

### What to look for
- Concentration: are the top 10–20 lanes dominating total value?
- “Self-lanes” (origin == destination) appearing at the top can indicate:
  - local distribution intensity
  - metro-area internal shipments
  - potential aggregation grain effects (still valid, but interpret correctly)
- YoY delta: identify lanes with the biggest absolute increase/decrease.

### Decision use
- If a small set of lanes dominates value:
  - prioritize forecasting/monitoring for those corridors
  - treat them as “core network”
- If YoY shows major lane shifts:
  - review capacity planning, carrier contracts, and infrastructure needs
- If top lanes are heavily local/self-lanes:
  - validate whether business stakeholders expect metro-internal dominance
  - optionally add a lane-type split (internal vs cross-zone) in a later PR

---

## Narrative 2 — “What are we shipping, and how?” (Commodity + mode concentration risk)

### Queries
- `02_exec_top_commodities_value.sql` (top commodities by value)
- `03_exec_mode_mix.sql` (mode share, % of total)
- `08_commodity_mix_top.sql` (commodity mix for top group)
- `06_mode_yoy_delta.sql` (mode winners/losers YoY)

### What to look for
- Commodity concentration:
  - top commodities that dominate value can create macro sensitivity
- Mode concentration:
  - if one mode dominates, the network becomes vulnerable to disruption
- Mode YoY shifts:
  - growth/decline by mode can reflect policy, costs, capacity, or disruption

### Decision use
- Concentrated commodities:
  - highlight as “risk exposure” categories
  - watch price/volume drivers
- Strong dependency on one mode:
  - explore resilience strategies (multi-mode alternatives)
- Mode YoY shifts:
  - investigate root causes (fuel costs, labor, rail capacity, port constraints)

---

## Narrative 3 — “How far are shipments going, and what changed?” (Distance bands + trade type + balance)

### Queries
- `09_distance_band_snapshot.sql` (distance distribution snapshot)
- `10_distance_band_yoy.sql` (distance shift YoY)
- `07_trade_type_mix.sql` (trade type mix)
- `11_zone_in_out_balance.sql` (net balance by zone)

### What to look for
- Distance distribution:
  - short-haul vs long-haul dominance (value, tons, tmiles)
- YoY distance shift:
  - movement toward shorter/longer shipments changes network economics
- Trade type mix:
  - domestic vs international exposure (depends on your dim definitions)
- Zone balance:
  - zones with strong net outflow or inflow show structural role:
    - producers vs consumers vs transshipment hubs

### Decision use
- If long-haul tmiles dominate:
  - optimize for efficiency and capacity planning across long corridors
- If YoY indicates shift to shorter haul:
  - distribution/localization effects → rethink warehouse placement
- If certain zones show extreme net balance:
  - focus on infrastructure, congestion, and throughput constraints

---

## Quality + performance notes (scope)

- All queries are designed to run on a laptop-grade MySQL setup.
- PR#6 added performance indexes for core year-filtered aggregations.
- PR#7 adds a targeted support index (only if needed) to make the heaviest distance-band query reliable.

If a query still runs unusually slow:
1) verify indexes exist (`SHOW INDEX FROM fact_faf;`)
2) run `EXPLAIN ANALYZE` on the query
3) confirm `@YEAR` is set to a single year (not a wide range)