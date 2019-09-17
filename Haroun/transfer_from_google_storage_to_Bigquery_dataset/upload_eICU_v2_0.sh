# This code has been modified from https://github.com/MIT-LCP/mimic-code/tree/master/buildmimic/bigquery
# The modifcations are:
    # names of bucket and dataset
    # use of schema auto-detection (as opposed to using JSON files for the schema)

#!/bin/bash

# Initialize parameters
bucket="ync_eicu_collaborative_research_database_20"
dataset="eICU_V2_0"



# Get the list of files in the bucket
FILES=$(gsutil ls gs://$bucket)

for file in $FILES
do

# Extract the table name from the file path (ex: gs://mimic3_v1_4/ADMISSIONS.csv.gz)
base=${file##*/}            # remove path
filename=${base%.*}         # remove .gz
tablename=${filename%.*}    # remove .csv

# Create table and populate it with data from the bucket
bq load --allow_quoted_newlines --skip_leading_rows=1 --source_format=CSV --autodetect $dataset.$tablename gs://$bucket/$tablename.csv.gz 

# Check for error
if [ $? -eq 0 ];then
    echo "OK....$tablename"
else 
    echo "FAIL..$tablename"
fi

done
exit 0
