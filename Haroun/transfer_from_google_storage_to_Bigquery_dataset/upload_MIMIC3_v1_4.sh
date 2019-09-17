# This code has been taken from https://github.com/MIT-LCP/mimic-code/tree/master/buildmimic/bigquery

#!/bin/bash

# Initialize parameters
bucket="ync_mimic-iii-clinical-database-14"
dataset="MIMIC3_V1_4"
schema_local_folder="/Users/harounchahed/mimic-code/buildmimic/bigquery/schemas"



# Get the list of files in the bucket
FILES=$(gsutil ls gs://$bucket)

for file in $FILES
do

# Extract the table name from the file path (ex: gs://mimic3_v1_4/ADMISSIONS.csv.gz)
base=${file##*/}            # remove path
filename=${base%.*}         # remove .gz
tablename=${filename%.*}    # remove .csv

# Create table and populate it with data from the bucket
bq load --allow_quoted_newlines --skip_leading_rows=1 --source_format=CSV $dataset.$tablename gs://$bucket/$tablename.csv.gz $schema_local_folder/$tablename.schema.json

# Check for error
if [ $? -eq 0 ];then
    echo "OK....$tablename"
else 
    echo "FAIL..$tablename"
fi

done
exit 0
