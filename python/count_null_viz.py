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

# only compare intersecting columns
cols = [i for i in set(df1.columns).intersection(set(df2.columns))]

df1 = df1[cols]
df2 = df2[cols]

plt.figure(figsize=(6, 30))
difference = df2.iloc[0, :]- df1.iloc[0, :]

plt.plot(difference, range(87), label = years, color = 'blue')
plt.vlines(0, 0, 87) #0 reference line

for i in range(87):
    if abs(difference[i]) >= 10000:
        plt.text(x = difference[i] , y = i - 0.15, s = '{}'.format(difference[i]), size = 10, color = 'blue')
    else: 
        pass
    
plt.yticks(range(87), df1.columns, rotation='horizontal')
plt.title('Null 0 Counts Comparison {}'.format(years))
plt.savefig(output, bbox_inches='tight')
