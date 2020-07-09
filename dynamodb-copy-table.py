#!/usr/bin/env python3

import sys
import os
import yaml
import boto3
from collections.abc import Mapping
from time import sleep

if len(sys.argv) != 3:
    print("Usage: %s <source_table_name> <destination_table_name>" % sys.argv[0])
    sys.exit(1)

src_table = sys.argv[1]
dst_table = sys.argv[2]

ddbc_old = boto3.session.Session(profile_name=os.getenv("AWS_PROFILE_OLD")).client("dynamodb")
ddbc_new = boto3.session.Session(profile_name=os.getenv("AWS_PROFILE_NEW")).client("dynamodb")

# 1. Read and copy the target table to be copied
src = None
try:
    print("*** Reading key schema from %s table" % src_table)
    src = ddbc_old.describe_table(TableName=src_table)
    print(src)
except Error as e:
    print("Error:", e)
    sys.exit(1)

table = src["Table"]
print(table)
conf = yaml.dump(table)
conf_cleaned = ""
for line in conf.splitlines():
    if line.strip().startswith(
        (
            "ItemCount:",
            "CreationDateTime:",
            "NumberOfDecreasesToday:",
            "TableArn:",
            "TableId:",
            "TableSizeBytes:",
            "TableStatus:",
        )
    ):
        print("Skipping line:", line)
        continue
    conf_cleaned += line + "\n"
print(conf)
print(conf_cleaned)

# 2. Create the new table
try:
    ddbc_new.create_table(**yaml.safe_load(conf_cleaned))
    print("*** Waiting for the new table %s to become active" % dst_table)
    sleep(5)
except ddbc_new.exceptions.ResourceInUseException as e:
    print(e)
except Error as e:
    print("Unexpected error:", e)
    sys.exit(1)

while ddbc_new.describe_table(TableName=src_table)["Table"]["TableStatus"] != "ACTIVE":
    sleep(3)


if "DISABLE_DATACOPY" in os.environ:
    print("Copying of data from source table is disabled. Exiting...")
    sys.exit(0)

# 3. Add the items
start_key = True
while start_key:
    items = {"TableName": src_table, "Limit": 50}
    if isinstance(start_key, Mapping):
        items["ExclusiveStartKey"] = start_key
    print("args:", items)
    print("Scanning table...")
    out = ddbc_old.scan(**items)
    print("Uploading items...")
    for item in out["Items"]:
        print(".", end="", flush=True)
        ddbc_new.put_item(TableName=src_table, Item=item)
    print("done")
    if "LastEvaluatedKey" in out:
        start_key = out["LastEvaluatedKey"]
    else:
        start_key = False
print("We are done. Exiting...")
