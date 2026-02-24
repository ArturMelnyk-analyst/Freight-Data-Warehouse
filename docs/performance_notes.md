Performance Notes — PR#6

Query-Driven Composite Indexing with EXPLAIN ANALYZE



1\. Context



The fact\_faf table stores freight flow data at the following grain:



(origin\_zone\_id,

&nbsp;destination\_zone\_id,

&nbsp;mode\_id,

&nbsp;commodity\_id,

&nbsp;trade\_type\_id,

&nbsp;dist\_band\_id,

&nbsp;year)



Row volume: multi-million scale (full historical FAF dataset).



PR#5 introduced:



Primary key



Unique grain constraint (uk\_fact\_grain)



Single-column indexes for FK support



Validation stability improvements



However, analytics queries filtering by year and grouping by dimensions were extremely slow.



This PR (PR#6) focuses purely on performance optimization for analytics workloads using:



Query pattern analysis



Composite indexes aligned to WHERE + GROUP BY patterns



Measured benchmarking using EXPLAIN ANALYZE





2\. Baseline (Before Composite Indexes)



All benchmarks were executed using:



EXPLAIN ANALYZE



Filtered to:



WHERE year = 2024

Query 1 — Top Lanes by Value (Origin → Destination)



Group by:



(year, origin\_zone\_id, destination\_zone\_id)



Runtime (before): ~128 seconds



Query 2 — Mode + Commodity Mix (Year Slice)



Group by:



(year, mode\_id, commodity\_id)



Runtime (before): ~131 seconds



Query 3 — Trade Type Breakdown



Group by:



(year, trade\_type\_id)



Runtime (before): ~132 seconds



Query 4 — Distance Band Distribution



Group by:



(year, dist\_band\_id)



Runtime (before): ~133 seconds



Query 5 — Join with dim\_zone (Readable Lane Names)



Includes two joins to dim\_zone.



Runtime (before): ~4.3 seconds



Baseline Observation



Without composite indexes:



MySQL scanned large portions of fact\_faf



Heavy aggregation cost



Temporary table + filesort behavior



Long execution times (2+ minutes per query)



The bottleneck was not the join — it was the aggregation over year-level slices.





3\. Index Strategy (PR#6)



We introduced composite indexes aligned to real analytics access patterns.



Design principles:



Almost all dashboard queries filter by year



Grouping columns should follow the filter column



Composite indexes should match (WHERE + GROUP BY) order



Composite Indexes Added

1\) Year + Origin + Destination



Supports lane ranking queries:



CREATE INDEX ix\_fact\_year\_origin\_dest

ON fact\_faf (year, origin\_zone\_id, destination\_zone\_id);

2\) Year + Mode + Commodity



Supports modal mix dashboards:



CREATE INDEX ix\_fact\_year\_mode\_commodity

ON fact\_faf (year, mode\_id, commodity\_id);

3\) Year + Trade Type



Supports import/export/domestic splits:



CREATE INDEX ix\_fact\_year\_trade\_type

ON fact\_faf (year, trade\_type\_id);

4\) Year + Distance Band



Supports distance distribution analytics:



CREATE INDEX ix\_fact\_year\_dist\_band

ON fact\_faf (year, dist\_band\_id);



4\. Results (After Composite Indexes)



Same queries, same filter (year = 2024), re-measured with EXPLAIN ANALYZE.



Query 1 — Top Lanes by Value



Runtime (after): 2.594 seconds



Improvement:



~128s → 2.6s

≈ 49x faster

Query 2 — Mode + Commodity Mix



Runtime (after): 0.937 seconds



Improvement:



~131s → 0.94s

≈ 140x faster

Query 3 — Trade Type Breakdown



Runtime (after): 0.781 seconds



Improvement:



~132s → 0.78s

≈ 170x faster

Query 4 — Distance Band Distribution



Runtime (after): 2.141 seconds



Improvement:



~133s → 2.14s

≈ 62x faster

Query 5 — Join dim\_zone for Lane Names



Runtime (after): 0.078 seconds



Improvement:



~4.3s → 0.08s

≈ 55x faster



5\. Why the Improvement Is So Large



The composite indexes allow MySQL to:



Efficiently filter by year



Access grouped columns in index order



Reduce scanning cost dramatically



Avoid large temporary structures



Reduce filesort workload



This aligns index structure directly with:



WHERE year = ...

GROUP BY dimension\_keys



The optimizer now selects the composite indexes rather than relying on:



single-column indexes



full table scans



fallback to uk\_fact\_grain





6\. Technical Takeaways

What Worked



Composite indexes starting with year



Matching index column order to GROUP BY



Using EXPLAIN ANALYZE instead of guessing



What Didn’t Work (Previous Attempt)



Relying only on single-column FK indexes



Hoping MySQL would optimize large aggregates automatically





7\. Impact on Next Phases



With PR#6 complete:



Analytics queries now run in interactive time (<3s)



No more connection drops in Workbench



PR#7 (Analytics Layer) can safely build 10–15 decision-ready queries



No immediate need for summary tables or partitioning





8\. Final Assessment



PR#6 successfully transforms the warehouse from:



“Technically correct but operationally slow”



into:



“Analytics-ready and performant for year-sliced dashboard workloads”



All improvements are measurable, documented, and reproducible.

