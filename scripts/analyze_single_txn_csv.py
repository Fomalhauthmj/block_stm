#! /usr/bin/python3
# parse single txn csv
import sys
import pandas as pd
for path in sys.argv[1:]:
    df = pd.read_csv(path)
    print("min\n", df.loc[:, df.columns != "txn_idx"].min())
    print("max\n", df.loc[:, df.columns != "txn_idx"].max())
    print("mean\n", df.loc[:, df.columns != "txn_idx"].mean())
    print("quantile(50%)\n", df.loc[:, df.columns != "txn_idx"].quantile())
    print("std\n", df.loc[:, df.columns != "txn_idx"].std())
