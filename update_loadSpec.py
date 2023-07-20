#!/usr/bin/env python3

import argparse
import base64
import binascii
import csv
import gzip
import json
import sys
from pathlib import Path
from typing import List, Optional, Union

SUPPORTED_STORAGE_TYPES = ["hdfs", "s3", "google", "azure", "local"]


# Parse command line arguments
def parse_args():
    ap = argparse.ArgumentParser(description="Tool for updating loadSpec in Druid segment "
                                             "metadata required when migrating the DeepStorage")
    ap.add_argument("-i", "--input", required=True,
                    help="path to input CSV file with Druid segments table. Can be gzipped.")
    ap.add_argument("-o", "--output", required=True, help="path to output CSV file. Can be gzipped.")
    ap.add_argument("-t", "--storage-type", required=True, choices=SUPPORTED_STORAGE_TYPES, help="new storage type")
    ap.add_argument("-b", "--bucket", required=False, help="new bucket name")
    ap.add_argument("--delimiter", default=";", help="CSV delimiter")
    if len(sys.argv[1:]) == 0:
        ap.print_help()
        ap.exit()
    return ap.parse_args()


class RowProcessor:
    def __init__(self, storage_type: str, bucket: str):
        self.storage_type = storage_type
        self.bucket = bucket

    def make_load_spec(self, current_spec):
        if current_spec["type"] == "s3_zip" and "key" in current_spec:
            path = current_spec["key"]
        elif current_spec["type"] == "azure" and "blobPath" in current_spec:
            path = current_spec["blobPath"]
        elif "path" in current_spec:
            path = current_spec["path"]
        else:
            raise ValueError(f"Unknown segment path in loadSpec {json.dumps(current_spec)}")
        if self.storage_type == "hdfs":
            return {
                "type": "hdfs",
                "path": path.replace(":", "_")
            }
        elif self.storage_type == "s3":
            return {
                "type": "s3_zip",
                "bucket": self.bucket,
                "key": path
            }
        elif self.storage_type == "local":
            return {
                "type": "local",
                "path": path
            }
        elif self.storage_type == "google":
            return {
                "type": "google",
                "bucket": self.bucket,
                "path": path
            }
        elif self.storage_type == "azure":
            return {
                "type": "azure",
                "containerName": self.bucket,
                "blobPath": path
            }
        else:
            raise ValueError(f"Unsupported loadSpec type: {self.storage_type}")

    def process_row(self, row: List[str]) -> List[str]:
        """Process a single row of the CSV data"""

        payload = row[8]
        if payload.startswith("{"):
            payload_encoding = "json"
        elif payload.startswith("\\x"):
            payload_encoding = "hex"
            payload = binascii.unhexlify(payload[2:]).decode('utf-8')
        else:
            payload_encoding = "base64"
            payload = base64.b64decode(payload).decode('utf-8')

        payload = json.loads(payload)
        payload["loadSpec"] = self.make_load_spec(payload["loadSpec"])
        new_payload = json.dumps(payload, separators=(",", ":"))

        if payload_encoding == "json":
            row[8] = new_payload
        elif payload_encoding == "hex":
            row[8] = "\\x" + binascii.hexlify(new_payload.encode('utf-8')).decode('utf-8')
        else:
            row[8] = base64.b64encode(new_payload.encode('utf-8')).decode('utf-8')
        return row


def open_file(path: Union[str, Path], mode: str = "r", newline: Optional[str] = None):
    if isinstance(path, str):
        path = Path(path)

    if path.suffix == ".gz":
        return gzip.open(path, mode + "t", newline=newline)
    else:
        return open(path, mode, newline=newline)


def main():
    args = parse_args()
    processor = RowProcessor(args.storage_type, args.bucket)

    with open_file(args.input, "r") as csv_input:
        with open_file(args.output, "w", newline="") as csv_output:
            reader = csv.reader(csv_input, delimiter=args.delimiter)
            writer = csv.writer(csv_output, delimiter=args.delimiter)
            for row in reader:
                updated_row = processor.process_row(row)
                writer.writerow(updated_row)


if __name__ == "__main__":
    main()
