\# Tableau Dashboard Notes — FAF Freight Data Warehouse



\## Overview



This dashboard visualizes selected outputs generated from the FAF Freight Data Warehouse.



The analytical logic intentionally remains upstream in the warehouse and SQL layer.



Tableau acts as a business-facing presentation interface rather than the analytical engine.



This follows common BI architecture patterns where:



\- MySQL handles storage

\- SQL handles transformations and aggregations

\- Tableau handles presentation



\---



\## Dashboard Views



The dashboard includes:



\- Top 10 freight corridors by shipment value

\- Transportation mode share by shipment value

\- Top 10 commodities by shipment value

\- Freight value distribution by distance band



\---



\## Public Dashboard



Interactive version:



\- \[View Tableau Dashboard](https://public.tableau.com/app/profile/artur.melnyk/viz/faf\_freight\_dashboard/FAFFreightAnalyticsDashboard)



\---



\## Data Flow



Raw FAF files



↓



Warehouse staging tables



↓



Fact and dimension model



↓



Optimized SQL aggregation layer



↓



CSV exports



↓



Tableau dashboard



\---



\## SQL Sources



\- sql/analytics/tableau/01\_tableau\_top\_corridors.sql

\- sql/analytics/tableau/02\_tableau\_mode\_mix.sql

\- sql/analytics/tableau/03\_tableau\_top\_commodities.sql

\- sql/analytics/tableau/04\_tableau\_distance\_bands.sql



\---



\## Exported CSV files



\- data/exports/tableau/top\_corridors\_2024.csv

\- data/exports/tableau/mode\_mix\_2024.csv

\- data/exports/tableau/top\_commodities\_2024.csv

\- data/exports/tableau/distance\_bands\_2024.csv



\---



\## Performance Results



| Export | Runtime |

|---|---:|

| Top corridors | 2.579 sec |

| Mode mix | 1.141 sec |

| Top commodities | 1.329 sec |

| Distance bands | 1.344 sec |



\---



\## Design Note



The dashboard is not intended to replace the warehouse.



The warehouse performs:



\- indexing

\- validation

\- modeling

\- aggregation



Tableau communicates selected outputs visually for stakeholders.

