import subprocess
import humanize
import os
import re
import csv

# HDFS base dir and replication factor
HDFS_BASE_DIR = "/hbase/data/hbase"
REPLICATION_FACTOR = 3

def get_hdfs_size(path):
    """Returns size in bytes of given HDFS path."""
    try:
        output = subprocess.check_output(["hadoop", "fs", "-du", "-s", path])
        size_bytes = int(output.decode("utf-8").split()[0])
        return size_bytes
    except subprocess.CalledProcessError:
        return 0

def list_tables_with_prefix(prefix):
    """Lists all HDFS tables that start with the given prefix."""
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
    return humanize.naturalsize(b, binary=True).replace('i', '')

def print_table(table_data, prefix):
    txt_filename = os.path.join(os.curdir, 'output', "{}_hbase_report.txt".format(prefix))
    csv_filename = os.path.join(os.curdir, 'output', "{}_hbase_report.csv".format(prefix))

    total_rf_bytes = 0

    with open(txt_filename, "w") as txt_file, open(csv_filename, "w", newline='') as csv_file:
        # Write headers
        headers = ["HBase size without RF", "HBase size with RF", "Table name"]
        txt_file.write(f"{headers[0]:<25}{headers[1]:<25}{headers[2]}\n")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["Table Name", "Size Without RF (Bytes)", "Size With RF (Bytes)"])

        # Write data rows
        for raw_size, rf_size, path, rf_bytes, raw_bytes in table_data:
            txt_file.write(f"{raw_size:<25}{rf_size:<25}{path}\n")
            csv_writer.writerow([path, raw_bytes, rf_bytes])
            total_rf_bytes += rf_bytes

        total_rf_human = bytes_to_human_readable(total_rf_bytes).replace('B', '')
        txt_file.write("-" * 80 + "\n")
        txt_file.write(f"{'Total':<25}{total_rf_human:<25}\n")

    print(f"✔️  Output written to {txt_filename} and {csv_filename}")

def main(prefix):
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

    print_table(table_data, prefix)

if __name__ == "__main__":
    import sys
    if len(sys.argv) == 1:
        print("Usage: python script.py <table_prefix_1> <table_prefix_2> ...")
        exit(1)

    prefixes = sys.argv[1:]
    for prefix in prefixes:
        main(prefix)
