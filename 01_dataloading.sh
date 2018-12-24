#!/bin/bash
#download data (by input datasets)
mkdir -p data
sh ./get_data/pluto_$1.sh
mv pluto_$1.csv ./data/pluto_$1.csv
