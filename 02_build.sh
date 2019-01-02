#!/bin/bash

###############################################################
#   Note: Please follow the convention $1 => Old, $2 => New   #
###############################################################

# loading data
# sh dataloading.sh $1
# sh dataloading.sh $2

# Unset environmental variables
unset PYSPARK_DRIVER_PYTHON

# Create output folder if didn't exist
mkdir -p output

# Step 1: Launch field wise comparison
spark-submit python/field_comparison.py data/pluto_$1.csv data/pluto_$2.csv output/mismatch_$1_$2.csv

## Visualization for field wise comparison 
python python/mismatch_viz.py output/mismatch_$1_$2.csv output/mismatch_$1_$2.png $1_$2

# Step 2: Generate aggregate summary 
spark-submit python/compute_aggregate.py data/pluto_$1.csv output/aggregate_summary_$1.csv
spark-submit python/compute_aggregate.py data/pluto_$2.csv output/aggregate_summary_$2.csv

## Visualization for aggregate summary comparison
python python/aggregate_summary_viz.py output/aggregate_summary_$1.csv output/aggregate_summary_$2.csv output/aggregate_summary_$1_$2.png $1_$2

# Step 3: Count null for each columns
spark-submit python/count_null.py data/pluto_$1.csv output/count_null_$1.csv
spark-submit python/count_null.py data/pluto_$2.csv output/count_null_$2.csv

## Visualization for null count comparison
python python/count_null_viz.py output/count_null_$1.csv output/count_null_$2.csv output/count_null_$1_$2.png $1_$2

## Step Last: Zipping all output into a report$1$2.zip
zip -r output_$1_$2.zip output

## Remove output directory
rm -r output
