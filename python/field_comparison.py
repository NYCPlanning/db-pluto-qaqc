from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import mean, udf, col, round
from pyspark.sql.types import DoubleType
from pyspark.sql import *
import time
import sys

### Create User Defined functions (udf) for identifying mismatches
@udf
def compare(col1,col2):
    if col1!=col2:
        return 1
    else:
        return 0

# special access column comparison udf function
@udf
def compare_a(col1,col2):
    try:
        if abs(col1 - col2) > 10:
            return 1
        else:
            return 0
    except:
        return 0

if __name__=='__main__':
    start_time = time.time()
    sc = SparkContext()
    spark = SparkSession(sc)

    spark.sql('set spark.sql.caseSensitive=true')

    ### import data and prepare dataframe ###
    pluto_1 = sys.argv[0]
    pluto_2 = sys.argv[1]
    output_csv = sys.argv[2]

    ### Float type columns
    double_columns = ['bldgarea', 'facilfar','residfar', 'commfar',
                    'numbldgs', 'numfloors', 'bldgdepth', 'bldgfront',
                    'lotdepth', 'lotfront', 'exempttot', 'exemptland',
                    'assessland', 'assesstot', 'builtfar']

    ### Assess related columns --> apply compare_a
    access_cols = ['exemptland', 'exempttot', 'assessland', 'assesstot']

    df1 = spark.read.csv(pluto_1, header=True)
    df2 = spark.read.csv(pluto_2, header=True)

    for A in df1.columns:
            df1 = df1.withColumnRenamed(A, A.lower())

    for A in df2.columns:
            df2 = df2.withColumnRenamed(A, A.lower())

    # dbutils.fs.rm('data/df1.parquet', True)
    df1.write.parquet('data/df1.parquet')
    # df1 = df1.select([col(A).(A.lower()) for A in df1.schema.names])
    # df2 = df2.select([col(A).alias(A.lower()) for A in df2.schema.names])

    ### Select column intersections
    # print(df1.columns)
    # print('A', df2.columns)
    # print('B', df1.columns)

    # df11 = df1.select(df1.columns)
    # df22 = df2.select(df1.columns)

    ### Column type conversions
    # for A in double_columns:
    #     df1 = df1.withColumn(A, round(col(A).cast(DoubleType()), 2))
    #     df2 = df2.withColumn(A, round(col(A).cast(DoubleType()), 2))

    ### Add column name flag to distinguish two versions
    # df1 = df1.select([col(A).alias(A+'_1') for A in df1.schema.names])
    # colnames = zip(df1.columns, df2.columns)

    ### merging two dataframes on bbl (inner join)
    # df = df2.join(df1, df2['bbl'] == df1['bbl_1'], 'inner')
    #
    # ### Apply udf for field by field comparison
    # for A,B in colnames:
    #     if B in access_cols:
    #         df = df.withColumn(B+'%', compare_a(col(A),col(B)))\
    #             .drop(A,B)
    #     else:
    #         df = df.withColumn(B+'%', compare(col(A),col(B)))\
    #                 .drop(A,B)
    #
    # ### Reduce table by taking means of each column and export result table to csv
    # results = df.select(*[mean(col(A)).alias(A) for A in df.columns]).write.csv(output_csv)
    # elapsed_time = time.time() - start_time
    # print('This job took {}s to execute'.format(elapsed_time))
