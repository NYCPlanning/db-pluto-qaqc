mkdir temp
cd temp
curl -O https://www1.nyc.gov/assets/planning/download/zip/data-maps/open-data/nyc_pluto_18v2_csv.zip

#unzip file to temp
unzip *.zip

#move target csv to parent directory
mv nyc_pluto_18v2_csv.zip ../pluto_18v2.csv

#remove temp folder
cd ..
rm -rf temp