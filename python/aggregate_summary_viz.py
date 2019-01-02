import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sys
import os
import subprocess

# input and output file path 
input1 = sys.argv[1]
input2 = sys.argv[2]
output = sys.argv[3]
years = sys.argv[4]

wd = subprocess.check_output('git rev-parse --show-toplevel', shell = True)
os.chdir(wd[:-1]) #-1 removes \n

# load into dataframes
df1 = pd.read_csv(input1, index_col=False)
df2 = pd.read_csv(input2, index_col=False)

results = (df2.iloc[0, 1:] - df1.iloc[0, 1:])/df1.iloc[0, 1:]

plt.figure(figsize=(15, 10))
plt.plot(range(21), results, color = 'blue', label= years)
plt.hlines(0, 0, 21, color = 'black')

for i in range(21):
    plt.vlines(i, min(results), max(results), color = '#e1e9f7', linestyles='dashed')

plt.xticks(range(21), df1.columns[1:], rotation=70)
plt.title('Aggregated Value Comparison {}'.format(years))
plt.savefig(output, bbox_inches='tight')