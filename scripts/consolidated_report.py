import subprocess
import humanize
import os
import re
import csv

HDFS_BASE_DIR = "/hbase/data/hbase"
REPLICATION_FACTOR = 3

def get_hdfs_size(path):
    try:
        output = subprocess.check_output(["hadoop", "fs", "-du", "-s", path])
        size_bytes = int(output.decode("utf-8").split()[0])
        return size_bytes
    except subprocess.CalledProcessError:
        return 0

def list_tables_with_prefix(prefix):
    try:
        output = subprocess.check_output(["hadoop", "fs", "-ls", HDFS_BASE_DIR])
        lines = output.decode("utf-8").strip().split("\n")
        tables = []
        for line in lines:
            parts = line.strip().split()
            if len(parts) >= 8:
                path = parts[-1]
                table_name = os.path.basename(path)
                if table_name.startswith(prefix):
                    tables.append((table_name, path))
        return tables
    except subprocess.CalledProcessError:
        return []

def bytes_to_human_readable(b):
    return humanize.naturalsize(b, binary=True, format="%.4f").replace('i', '')

def write_combined_output(all_table_data):
    txt_filename = "../output/hbase_report.txt"
    csv_filename = "../output/hbase_report.csv"

    with open(txt_filename, "w") as txt_file, open(csv_filename, "w", newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["Prefix", "Table Name", "Size Without RF (Bytes)", "Size With RF (Bytes)"])

        for prefix, table_data in all_table_data.items():
            txt_file.write(f"\n=== Tables with mnemonic '{prefix}' ===\n")
            txt_file.write(f"{'HBase size without RF':<25}{'HBase size with RF':<25}{'Table name'}\n")

            prefix_rf_total = 0

            for raw_size, rf_size, path, rf_bytes, raw_bytes in table_data:
                txt_file.write(f"{raw_size:<25}{rf_size:<25}{path}\n")
                csv_writer.writerow([prefix, path, raw_bytes, rf_bytes])
                prefix_rf_total += rf_bytes

            prefix_total_human = bytes_to_human_readable(prefix_rf_total).replace('B', '')
            txt_file.write("-" * 80 + "\n")
            txt_file.write(f"{'Total for ' + prefix:<25}{prefix_total_human:<25}\n\n")

    print(f"✔️ Output written to {txt_filename} and {csv_filename}")

def main(prefixes):
    all_table_data = {}

    for prefix in prefixes:
        tables = list_tables_with_prefix(prefix)
        table_data = []

        for table_name, path in tables:
            size_bytes = get_hdfs_size(path)
            size_rf_bytes = size_bytes * REPLICATION_FACTOR

            size_human = bytes_to_human_readable(size_bytes)
            size_rf_human = bytes_to_human_readable(size_rf_bytes)

            size_clean = re.sub(r'B$', '', size_human)
            size_rf_clean = re.sub(r'B$', '', size_rf_human)

            table_data.append((size_clean, size_rf_clean, path, size_rf_bytes, size_bytes))

        all_table_data[prefix] = table_data

    write_combined_output(all_table_data)

if __name__ == "__main__":
    import sys
    if len(sys.argv) == 1:
        print("Usage: python script.py <table_prefix_1> <table_prefix_2> ...")
        exit(1)

    prefixes = sys.argv[1:]
    main(prefixes)
