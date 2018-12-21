from pyspark.sql.functions import mean, udf, col
import pandas as pd
import time

file1 = sys.argv[0]
file2 = sys.argv[1]
df1 = spark.read.csv('data/pluto_{}.csv'.format(file1), header=True)
df2 = spark.read.csv('data/pluto_{}.csv'.format(file2), header=True)

df1 = df1.select([col(A).alias(A+'_1') for A in df1.schema.names])
df1_names = df1.schema.names
df2_names = df2.schema.names
colnames = zip(df1_names, df2_names)

if file1 == '18v11':
    df = df1.join(df2, df1['bbl_1'] == df2['BBL'])
elif file2 == '18v11':
    df = df1.join(df2, df2['bbl_1'] == df1['BBL'])
else:
    df = df1.join(df2, df2['BBL_1'] == df1['BBL'])

@udf
def compare(col1,col2):
    if col1!=col2:
        return 1
    else:
        return 0

spark.sql('set spark.sql.caseSensitive=true')
for A,B in colnames:
    df = df.withColumn(A+'%', compare(col(A),col(B)))\
               .drop(A,B)

results = df.select(*[mean(col(A)).alias(A) for A in df.schema.names]).toPandas().to_csv('../output/{}{}.csv'.format(file1, file2))
