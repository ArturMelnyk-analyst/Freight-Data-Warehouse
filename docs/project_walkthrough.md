# Project Walkthrough — FAF Freight Data Warehouse

This document explains the full project flow from raw dataset to final analytical output.

---

## 1. Problem Framing

Raw freight data is difficult to analyze directly because it is wide, not optimized for repeated business questions, and expensive to query without structure.

This project converts raw FAF data into a reproducible analytical warehouse.

---

## 2. Profiling

The project begins with profiling to determine:

- analytical grain
- key entities
- key measures
- dimension candidates
- validation risks

This phase defines the warehouse design.

---

## 3. Dimensions

Dimension tables are created for:

- zones
- modes
- commodities
- trade types
- distance bands

This standardizes categorical values before fact construction.

---

## 4. Staging

Raw records are loaded into `stg_faf`.

Purpose:

- preserve source-shaped data
- simplify transformations
- support debugging
- support validation
- enable chunked processing

---

## 5. Fact Build

The fact pipeline transforms staged records into `fact_faf`, enforcing the business grain and mapping all dimension keys.

Main goals:

- stable grain
- correct key mapping
- analytical usability
- reproducibility

---

## 6. Validation

Validation gates ensure trust in the warehouse.

Checks include:

- orphan foreign keys
- duplicate grain
- negative measures
- null key checks
- selected staging consistency checks

---

## 7. Performance Engineering

Performance scripts benchmark real analytical patterns using EXPLAIN ANALYZE.

Indexes are added only when supported by workload evidence.

This makes the warehouse usable on a laptop-grade MySQL environment.

---

## 8. Analytics Layer

The analytics layer packages the warehouse into decision-ready SQL.

Examples include:

- top corridors
- commodity concentration
- mode mix
- trade type mix
- distance-band analysis
- year-over-year deltas
- zone inflow / outflow balance

---

## 9. Business Interpretation

The final documentation turns SQL outputs into narratives and executive questions.

This makes the project usable for both:

- technical reviewers
- business stakeholders

---

## 10. Final Result

The repository demonstrates a complete analytics engineering workflow:

1. model the warehouse
2. build ETL
3. validate data
4. optimize performance
5. deliver business insight