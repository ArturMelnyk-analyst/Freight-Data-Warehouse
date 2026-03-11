# Data Dictionary — FAF Freight Data Warehouse

The warehouse follows a star schema centered on `fact_faf`.

---

## fact_faf

Main analytical table storing freight flows.

### Grain

One row per:

- `origin_zone_id`
- `destination_zone_id`
- `mode_id`
- `commodity_id`
- `trade_type_id`
- `dist_band_id`
- `year`

### Key Columns

| Column | Description |
|---|---|
| `fact_id` | Surrogate primary key |
| `origin_zone_id` | FK → `dim_zone` |
| `destination_zone_id` | FK → `dim_zone` |
| `mode_id` | FK → `dim_mode` |
| `commodity_id` | FK → `dim_commodity` |
| `trade_type_id` | FK → `dim_trade_type` |
| `dist_band_id` | FK → `dim_distance_band` |
| `year` | Observation year |

### Measure Columns

| Column | Description |
|---|---|
| `tons` | Total shipment tonnage |
| `value` | Shipment value |
| `current_value` | Adjusted / current-dollar shipment value |
| `tmiles` | Ton-miles |

---

## dim_zone

Represents a freight zone used as origin or destination.

| Column | Description |
|---|---|
| `zone_id` | Surrogate key |
| `zone_code` | Source/business code |
| `zone_name` | Human-readable name |
| `zone_type` | Zone classification |

---

## dim_mode

Transportation mode dimension.

| Column | Description |
|---|---|
| `mode_id` | Surrogate key |
| `mode_code` | Source/business code |
| `mode_name` | Transportation mode name |

---

## dim_commodity

Commodity dimension.

| Column | Description |
|---|---|
| `commodity_id` | Surrogate key |
| `sctg2` | Commodity code |
| `commodity_name` | Commodity description |

---

## dim_trade_type

Trade classification dimension.

| Column | Description |
|---|---|
| `trade_type_id` | Surrogate key |
| `trade_type_code` | Source/business code |
| `trade_type_name` | Trade type description |

---

## dim_distance_band

Distance grouping dimension.

| Column | Description |
|---|---|
| `dist_band_id` | Surrogate key |
| `dist_band_code` | Source/business code |
| `dist_band_name` | Human-readable distance band |

---

## stg_faf

Raw/staging layer used prior to transformation into the fact table.

Purpose:

- preserve source imports
- support ETL mapping
- support debugging
- support validation
- enable chunked fact builds

---

## Indexing Note

Warehouse indexes support:

- year-filtered analytics
- lane aggregations
- mode / commodity aggregations
- trade type aggregations
- distance-band summaries

See `docs/performance_notes.md` for performance details.

