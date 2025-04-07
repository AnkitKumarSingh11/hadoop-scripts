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

Example output:

```

=== Tables with mnemonic 'meta' ===
HBase size without RF    HBase size with RF       Table name
1.0459 K                 3.1377 K                 /hbase/data/hbase/meta
0 Bytes                  0 Bytes                  /hbase/data/hbase/meta-2
0 Bytes                  0 Bytes                  /hbase/data/hbase/meta-info
0 Bytes                  0 Bytes                  /hbase/data/hbase/meta_another_person
0 Bytes                  0 Bytes                  /hbase/data/hbase/meta_person
--------------------------------------------------------------------------------
Total for meta           3.1377 K                 


=== Tables with mnemonic 'person' ===
HBase size without RF    HBase size with RF       Table name
3.0557 K                 9.1670 K                 /hbase/data/hbase/person_record_stream
4.9058 M                 14.7175 M                /hbase/data/hbase/person_record_stream_0
1.0560 M                 3.1681 M                 /hbase/data/hbase/person_record_stream_1
--------------------------------------------------------------------------------
Total for person         17.8946 M                


```
