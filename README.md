# db-pluto-qaqc

## Objective:
This repo serves to compare historical versions of Pluto to check data consistency

## Methods:
1. generate summary statistics for each numeric column, including grand sum, sum product for some fields and variance(not yet implemented)
2. fieldwise comparison with BBL as unique key and find mismatch rates for each fields.

## Instructions:
1. To load the datasets, please specify the two versions of pluto you would like to compare. Enter command
```
sh 01_dataloading.sh version1 version2
```
in the case of comparing 18v1 and 18v11, do
```
sh 01_dataloading.sh 18v1 18v11
```
currently supported versions: ```18v1, 18v11, 17v1, 17v11```

## Update log:
* 12/21/2018
  * need to convert pyspark notebook to python script (something like below)
  ```
  spark-submit XX XX -py-files comparison.py 18v1 18v11
  ```
  * need to consider output format, including summary tables, plots
  * research statistical packages in pyspark
* 12/28/2018
  * in older version (17v11) of pluto, there's no padded 0 for the following columns: landuse, tract2010, and sanitdistrict, which resulted in high mismatch rate. However after converting them to integers, match rates are high. 
