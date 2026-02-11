# FAF 5.7.1 — Source Data Notes

## Data Source

Freight Analysis Framework (FAF) 5.7.1  
U.S. Department of Transportation — Bureau of Transportation Statistics (BTS)

Raw CSV file:
FAF5.7.1_2018-2024.csv  
(~600+ MB, excluded from version control)

---

## Structural Overview

The dataset is stored in a **wide format**.

Each row represents a freight flow defined by:

- Origin
- Destination
- Mode
- Commodity
- Trade type
- Distance band

Annual measures are encoded as:

- `tons_YYYY`
- `value_YYYY`
- `current_value_YYYY`
- `tmiles_YYYY`

Years detected: **2018–2024 (7 years)**  
Measure families detected: **4**

This structure requires reshaping (wide → long) before loading into a normalized fact table.

---

## Zone Systems

Two distinct zone systems exist:

### Domestic Zones
- `dms_orig`
- `dms_dest`

Represent domestic FAF regions or states.


### Foreign Regions
- `fr_orig`
- `fr_dest`

Represent international regions.

Metadata confirms these systems are complementary (not redundant).  
They will be unified into a single `dim_zone` with a `zone_type` attribute.


Note: In domestic-only records, `fr_*` fields are null by design.
Conversely, foreign flows populate `fr_*` while domestic fields may be empty.
This structural pattern was confirmed during profiling.
---

## Mode Fields

- `dms_mode` — domestic primary mode
- `fr_inmode`, `fr_outmode` — international inbound/outbound legs

Final dimensional mapping strategy is defined in Phase 2.

---

## Other Dimensional Candidates

- `sctg2` — Commodity classification
- `trade_type` — Domestic / Import / Export
- `dist_band` — Distance band category

---

## Measures

For each year (2018–2024):

- `tons` — Total weight shipped
- `value` — Total value (2017 constant dollars)
- `current_value` — Nominal value in that year
- `tmiles` — Ton-miles

---

## Modeling Implication

Because measures are encoded per year as column suffixes, the dataset must be:

1. Loaded into staging.
2. Reshaped into long format.
3. Stored as one row per (flow + year).

This drives the star schema defined in PR#1.
