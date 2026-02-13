\# How to Obtain FAF 5.7.1 Data



This project uses the official Freight Analysis Framework (FAF) 5.7.1 dataset

published by the U.S. Bureau of Transportation Statistics (BTS).



\## Step 1 — Download the CSV



1\. Visit the official BTS FAF page:

&nbsp;  https://www.bts.gov/faf



2\. Download the file:

&nbsp;  FAF5.7.1\_2018-2024.csv



&nbsp;  (Exact filename may vary slightly — keep the original name.)



\## Step 2 — Place the File Locally



Place the CSV file here:



data/raw/faf/FAF5.7.1\_2018-2024.csv



⚠ Important:

\- This file is intentionally excluded from Git version control.

\- The dataset is large (~2.5M rows).

\- Do NOT rename the file unless you also update the loader script.



\## Step 3 — Load into Staging



From project root:



python -m etl.03\_load\_staging



The loader will:

\- Read the file in chunks

\- Insert into the staging table

\- Log file fingerprint (MD5 hash)

\- Log row count

\- Record run status in etl\_run\_log



