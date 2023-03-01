#! /usr/bin/python3
# parse logs
import os
import re
import sys
import numpy as np
patterns = ["read = ([0-9]+).*", "wait = ([0-9]+).*",
            "try_execute = ([0-9]+).*", "try_validate = ([0-9]+).*"]
print(sys.argv[1])
for root, dirs, files in os.walk(sys.argv[1]):
    for name in files:
        file = open(os.path.join(root, name), "r")
        content = "".join(file.readlines())
        for pattern in patterns:
            matches = re.findall(pattern, content)
            if len(matches) == 0:
                continue
            nums = [eval(i) for i in matches]
            print(file, pattern)
            print("min = ", np.min(nums))
            print("max = ", np.max(nums))
            print("mean = ", np.mean(nums))
            print("quantile(50%) = ", np.quantile(nums, 0.5))
            print("std = ", np.std(nums))
