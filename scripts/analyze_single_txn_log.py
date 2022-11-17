#! /usr/bin/python3
# parse single txn logs
import re
import sys
import csv
for path in sys.argv[1:]:
    file = open(path, "r")
    content = "".join(file.readlines())
    matches = re.findall(
        "txn_idx=([0-9]+).*reading_walltime=([0-9.]+).*execution_walltime=([0-9.]+).*reading_walltime_percentage=([0-9.]+).*", content)
    file = open(path+".csv", "w", newline="")
    writer = csv.writer(file)
    writer.writerow(["txn_idx", "reading_walltime",
                    "execution_walltime", "reading_walltime_percentage"])
    [writer.writerow(match) for match in matches]
