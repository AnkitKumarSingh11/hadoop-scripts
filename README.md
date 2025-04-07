# HBase Table Size Reporter

This Python script reports the HDFS disk usage of HBase tables based on a given table name prefix. It calculates the table size with and without the replication factor and generates a **human-readable report** in both **text** and **CSV** formats.

---

## 🔧 Features

- Accepts one or more **table name prefixes** as input.
- Lists all HBase tables in HDFS that start with each prefix.
- Calculates:
  - Raw HDFS size
  - Size with replication factor (default is 3)
- Outputs:
  - Formatted console output
  - Consolidated `report.txt`
  - Consolidated `report.csv`
- Sizes shown with **up to 4 decimal places**

---

## 🧩 Prerequisites

- Python 3.x
- Hadoop command line tools configured (`hadoop fs` should work)
- `humanize` Python module:
  
    Install it via pip:
    ```bash
        pip install humanize
    ```