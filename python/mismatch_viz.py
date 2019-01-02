import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sys
import os
import subprocess

# input and output file path 
input1 = sys.argv[1]
output = sys.argv[2]
years = sys.argv[3]

# make sure we are at the top of the repo
wd = subprocess.check_output('git rev-parse --show-toplevel', shell = True)
os.chdir(wd[:-1]) #-1 removes \n

# load into dataframes 
results_df = pd.read_csv(input1, index_col=False)

# Concatenating

# Plotting: 
plt.figure(figsize=(6, 30))
plt.plot(results_df.iloc[0, :], range(87), label = years, color = 'blue')
label = list(results_df.iloc[0, :])

for i in range(len(label)):
    if label[i] >= 0.1:
        plt.text(x = label[i] , y = i-0.15, s = '{}%'.format(np.round(label[i]*100, 2)), size = 10, color = 'blue')
    else: 
        pass
    
plt.yticks(range(87), results_df.columns, rotation='horizontal')
plt.title('{}_pct_mismatch_comparison'.format(years))
plt.savefig(output, bbox_inches='tight')