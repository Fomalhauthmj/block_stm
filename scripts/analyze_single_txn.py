#! /usr/bin/python3
# read logs and parse each single vm execution duration,then plot a scatter.
import re
import sys
import matplotlib.pyplot as plt
for path in sys.argv[1:]:
    file = open(path, "r")
    content = "".join(file.readlines())
    matches = re.findall('[0-9.]+ms', content)
    indexes = [idx for idx, _ in enumerate(matches)]
    floats = [float(match[:-2]) for match in matches]
    plt.scatter(indexes, floats)
    plt.savefig(path+".png")
    plt.cla()
