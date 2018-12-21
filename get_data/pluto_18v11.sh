# create temp folder and download file to temp
mkdir temp
cd temp
curl -O https://www1.nyc.gov/assets/planning/download/zip/data-maps/open-data/nyc_pluto_18v1_1_csv.zip

#unzip file to temp
unzip *.zip

#move target csv to parent directory
mv dcp_pluto_18v11.csv ../pluto_18v11.csv

#remove temp folder
cd ..
rm -rf temp
