# loading two pluto datasets
sh dataloading.sh $1 $2

#launch field wise comparison
mkdir outoput
spark-submit python/field_comparison.py $1 $2
#generate summary statistics
