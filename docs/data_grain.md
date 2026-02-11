# Target Warehouse Design — Fact Grain Definition

## Final Fact Table Grain

One row represents:

(origin_zone,
 destination_zone,
 mode,
 commodity,
 trade_type,
 distance_band,
 year)

Formally:

(origin_zone_id,
 destination_zone_id,
 mode_id,
 commodity_id,
 trade_type_id,
 dist_band_id,
 year)

This ensures:

- One unique freight movement per year.
- No duplicate dimensional combinations.
- Additive measures across dimensions.

---

## Measures Stored on Fact

- tons
- value (2017 constant dollars)
- current_value (nominal dollars)
- tmiles

All measures are year-specific.

---

## Dimensional Strategy

### dim_zone

Unifies:

- Domestic FAF zones (`dms_*`)
- Foreign regions (`fr_*`)

Includes:
- zone_id (surrogate primary key)
- zone_code (natural code from source)
- zone_type (DMS / FR)

---

### dim_mode

Single consolidated mode dimension.

Primary reference: `dms_mode`.

Foreign in/out modes will be mapped consistently in Phase 2.

---

### dim_commodity

Based on `sctg2`.

---

### dim_trade_type

Domestic / Import / Export classifications.

---

### dim_distance_band

Distance grouping categories.

---

### dim_year

Explicit year dimension (2018–2024).

---

## Key Strategy Note

Surrogate keys will be used for all dimensions to:

- Improve join performance
- Isolate source code changes
- Maintain referential integrity

Natural codes will be preserved as attributes.

---

## Why This Grain

This grain supports:

- Lane ranking
- Mode share analysis
- Commodity mix
- Year-over-year comparisons

It aligns with standard star-schema modeling practices and supports efficient indexing.
