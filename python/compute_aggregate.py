from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, round
import sys
import time
import pandas as pd
import os
import psycopg2

DATABASE_URL = os.environ['DATABASE_URL']

conn = psycopg2.connect(DATABASE_URL, sslmode='require')

if __name__=='__main__':
        start_time = time.time()
        sc = SparkContext()
        spark = SparkSession(sc)

        spark.sql('set spark.sql.caseSensitive=true')

        ### Specifying file path
        input_path = sys.argv[1]
        output_path = sys.argv[2]

        ### import data and prepare dataframe ###
        df = spark.read.csv(input_path, header=True)

        ### Change all column names to lower case
        df = df.select([col(A).alias(A.lower()) for A in df.columns])
        
        ### Aggregate over dataframe
        summary = df.groupBy("version")\
                    .agg(sum("unitsres"),
                        sum("lotarea"),
                        sum("bldgarea"),
                        sum("comarea"),
                        sum("resarea"),
                        sum("officearea"),
                        sum("retailarea"),
                        sum("garagearea"),
                        sum("strgearea"),
                        sum("factryarea"),
                        sum("otherarea"),
                        sum("assessland"),
                        sum("assesstot"),
                        sum("exemptland"),
                        sum("exempttot"),
                        sum("firm07_flag"),
                        sum("pfirm15_flag")).toPandas()

        agg_cols = ['version','UnitsRes','LotArea','BldgArea','ComArea',
                    'ResArea','OfficeArea','RetailArea','GarageArea',
                    'StrgeArea','FactryArea','OtherArea','AssessLand',
                    'AssessTot','ExemptLand','ExemptTot','FIRM07_FLAG',
                    'PFIRM15_FLAG']
        
        ### Update column names
        summary.columns = agg_cols
        
        ### Export to csv
        summary.to_csv(output_path, index=False)

        elapsed_time = time.time() - start_time
        print('\n\
               #################################################################\n\
               \n\
               \n\
                      This job took {}s to execute         \n\
               \n\
               \n\
               #################################################################\n\
               \n\
               '.format(elapsed_time))

# e.g. spark-submit python/compute_aggregate.py data/pluto_17v11.csv 17v11_agg.csv