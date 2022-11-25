#! /usr/bin/python3
# parse single txn logs
import re
import sys
import pandas as pd
for path in sys.argv[1:]:
    file = open(path, "r")
    content = "".join(file.readlines())
    matches = re.findall(
        "txn_idx=([0-9]+).*reading_walltime=([0-9.]+).*execution_walltime=([0-9.]+).*reading_walltime_percentage=([0-9.]+).*", content)
    df = pd.DataFrame(matches, columns=[
                      "txn_idx", "reading_walltime", "execution_walltime", "reading_walltime_percentage"])
    df.to_csv(path+".csv", index=False)
