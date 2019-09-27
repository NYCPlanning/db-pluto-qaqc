# db-pluto-qaqc

## Objective:
This repo serves to compare historical versions of Pluto to check data consistency

## Methods:
1. generate summary statistics for selected numeric column, including grand sum, sum product
2. field-wise comparison with BBL as unique key and find mismatch rates for each fields.
3. NUll or 0 counts comparison across different fields ("0" string version of 0 is also counted)

## Instructions:
1. To load the datasets, please specify the versions of pluto. Enter command:
```
sh 01_dataloading.sh version
```

  in the case of comparing 18v1 and 18v11, do

  ```
  sh 01_dataloading.sh 18v11
  sh 01_dataloading.sh 18v1
  ```
  currently supported versions: ```18v1, 18v11, 17v1, 17v11```

2. To generate QAQC plots, please refer to ```02_build.sh```, when comparing two versions of pluto (e.g. 18v1 18v2), enter command:
```
sh 02_build.sh 18v1 18v2
```
Note: please follow the oder OLD NEW to get the correct plots.

## Update log:
* 12/21/2018
  * ~~need to convert pyspark notebook to python script (something like below)~~
  ```
  spark-submit XX XX -py-files comparison.py 18v1 18v11
  ```
  * ~~need to consider output format, including summary tables, plots~~
  * ~~research statistical packages in pyspark~~
* 12/28/2018
  * in older version (17v11) of pluto, there's no padded 0 for the following columns: __landuse, tract2010, and sanitdistrict,__ which resulted in high mismatch rate. However after converting them to integers, match rates are high.
* 1/2/2019
  * moving from jupyter notebook to spark-submit (complete)
  * automate analysis and plotting
  * todo:
    * generate environment configuration
    * find the best tutorial for setting up spark on local and cloud
    * structure workflow using Pyspark:
      1. prototype using jupyter notebook and sample data
      2. deploy with sample data on local machine
      3. deploy with complete data on the cloud
    * Find out more about system hardware configuration or performance optimization

## Docker Setup Instructions::
1. Make sure you have docker installed
2. Pull the docker image
    ```
    docker pull jupyter/pyspark-notebook:latest
    ```
3. navigate to the db-pluto-qaqc directory
    ```
    cd db-pluto-qaqc
    ```
4. To run jupyter notebook at your current directory
    ```
    docker run -it --rm --name=qaqc            
            -v `pwd`:/home/db-pluto-qaqc            
            -w /home/db-pluto-qaqc            
            -p 8888:8888            
            jupyter/pyspark-notebook:latest
    ```
5. Replace the URLs http://xxxxxx:8888/?token=xxxxxxxxxxxxxxx by http://localhost:8888/?token=xxxxxxxxxxxxxxx