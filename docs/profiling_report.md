# Phase 1 Profiling Report — FAF 5.7.1

## Objective

Phase 1 establishes:

- Structural understanding of the dataset
- Dimensional modeling candidates
- Data quality signals
- Target warehouse grain definition
- Metadata-backed design decisions

All profiling was performed memory-safely using chunked reads.

---

## Dataset Characteristics

- File size: ~600+ MB
- Years detected: **2018–2024 (7 years)**
- Measure families detected: **4**
- Structure: Wide (annual measures encoded as column suffixes)

Each logical freight flow spans multiple year-specific measure columns.

---

## Data Quality Checks

The following checks were performed:

- Total row count (chunk-based scan)
- Key column null analysis
- Duplicate row snapshot
- Negative measure detection
- Representativeness check:
  - First chunk
  - Middle chunk
  - Last chunk

### Observations

- No structural corruption detected.
- Measure families consistently exist across all 7 years.
- Key columns are largely populated.
- DMS vs FR zones show complementary usage patterns with minimal overlap in sampled chunks.

No critical structural anomalies detected in sampled chunks.

### Structural Missingness Pattern

Initial chunk analysis shows `fr_*` columns at 100% null.

This is expected for purely domestic freight movements, where only `dms_*`
zone fields are populated. Multi-chunk sampling confirms that foreign flows
exist in other parts of the dataset and that `dms_*` and `fr_*` behave as
complementary systems.

Additionally, categorical code fields were evaluated treating both `NaN`
and `0` as missing values to avoid misinterpreting encoding artifacts as valid data.


---

## Metadata-Backed Zone Decision

From the official Data Dictionary:

- `dms_*` fields represent domestic FAF zones.
- `fr_*` fields represent foreign regions.

Empirical overlap checks confirm these systems are complementary.

### Decision

Both zone systems will be unified into:

`dim_zone`

with:

- `zone_id` (surrogate PK)
- `zone_code`
- `zone_type` (DMS / FR)

---

## Measure Structure

Measures follow pattern:

`<measure_family>_<year>`

Families:
- tons
- value
- current_value
- tmiles

Years:
2018–2024

This confirms that a wide → long transformation is required in Phase 4.

---

## Conclusion

Phase 1 confirms:

- Dataset structure is consistent.
- Dimensional modeling is feasible.
- Grain can be cleanly defined.
- Multi-year wide encoding requires transformation.

Phase 2 will implement dimension tables using metadata.
