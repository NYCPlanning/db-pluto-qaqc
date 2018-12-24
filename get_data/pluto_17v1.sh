# create temp folder and download file to temp
echo 'Downloading pluto_17v1'
mkdir temp
cd temp
curl -O https://www1.nyc.gov/assets/planning/download/zip/data-maps/open-data/nyc_pluto_17v1.zip

echo 'Unzip pluto_17v1'
#unzip file to temp
unzip *.zip

echo 'Concat pluto_17v1'
#write first row to target csv
cat Borofiles_CSV/BK2017V1.csv | head -n1 > pluto_17v1.csv

#write starting from second row of all 5 boroughs into target csv
for f in Borofiles_CSV/*.csv; do cat "`pwd`/$f" | tail -n +2 >> pluto_17v1.csv; done

#move target csv to parent directory
mv pluto_17v1.csv ../

#remove temp folder
cd ..
rm -rf temp

echo 'pluto_17v1 is ready'
