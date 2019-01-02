from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, round, isnan, when, count
from pyspark.sql.types import DoubleType
import pandas as pd
import time
import sys

if __name__=='__main__':
        start_time = time.time()
        sc = SparkContext()
        spark = SparkSession(sc)

        spark.sql('set spark.sql.caseSensitive=true')

        input_path = sys.argv[1]
        output_path = sys.argv[2]

        ### import data and prepare dataframe ###
        df = spark.read.csv(input_path, header=True)
        df = df.select([col(A).alias(A.lower()) for A in df.columns])

        double_columns = ['bldgarea', 'facilfar',
                  'residfar', 'commfar', 'numbldgs', 'numfloors', 'bldgdepth', 
                  'bldgfront', 'lotdepth', 'lotfront', 
                  'exempttot', 'exemptland',  'assessland', 'assesstot', 'builtfar']

        for A in double_columns: 
            df = df.withColumn(A, round(col(A).cast(DoubleType()), 2))
        
        null_results = df.select([(count(when(isnan(c) | col(c).isNull(), 1))\
                      + count(when(col(c)==0,1))).alias(c) for c in df.columns]).toPandas()
                    
        null_results.to_csv(output_path, index=False)

        elapsed_time = time.time() - start_time
        print('\n\
               #################################################################\n\
               \n\
               \n\
                      This job took {}s to execute         \n\
               \n\
               \n\
               #################################################################'.format(elapsed_time))


# e.g. spark-submit python/count_null.py data/pluto_17v11.csv 17v11_null.csv