#!/bin/bash
#download data (by input datasets)
mkdir data
sh ./get_data/pluto_$1.sh
sh ./get_data/pluto_$2.sh
mv pluto_$1.csv ./data/pluto_$1.csv
mv pluto_$2.csv ./data/pluto_$2.csv
