
# create temp folder and download file to temp
mkdir temp
cd temp
curl -O https://www1.nyc.gov/assets/planning/download/zip/data-maps/open-data/nyc_pluto_18v1.zip

#unzip file to temp
unzip *.zip

#write first row to target csv
cat PLUTO_for_WEB/BK_18v1.csv | head -n1 > pluto_18v1.csv

#write starting from second row of all 5 boroughs into target csv
for f in PLUTO_for_WEB/*.csv; do cat "`pwd`/$f" | tail -n +2 >> pluto_18v1.csv; done

#move target csv to parent directory
mv pluto_18v1.csv ../

#remove temp folder
cd ..
rm -rf temp
