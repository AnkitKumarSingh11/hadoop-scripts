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

🚀 Usage
bash
```
python3 script.py <table_prefix1> <table_prefix2> ...
```
Example:
```
python3 script.py meta person
```

📂 Output
report.txt: Pretty-formatted human-readable report for all table prefixes.

report.csv: CSV file suitable for spreadsheets or further analysis.

Both files include:

Table name

Size without replication

Size with replication

Per-prefix totals

Final total at the bottom

🛠️ Configuration
You can change the HDFS base directory or replication factor by modifying these lines in the script:

```
HDFS_BASE_DIR = "/hbase/data/hbase"
REPLICATION_FACTOR = 3
```

📌 Notes
Tables with 0 bytes (likely empty or non-existent) are still shown in the output for completeness.

Final totals are computed separately for each prefix and also overall at the end.

<center>Built with ❤️ by Ankit </center>