#!/bin/bash

partition=$1
class=$2
attribute=$3
start_year=$4
end_year=$5
start_month=$6
end_month=$7
regex=$8

month=(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dez)

if [ "$start_year" -ge "$end_year" ] && [ "$start_month" -eq "$end_month" ]; then
    echo "There has to be at least one month difference if the Year is the same and Start Year <= End Year"
    return 1
fi

while [ "$start_year" -ne "$end_year" ] || [ "$start_month" -ne "$end_month" ]; do

    days=$(cal $(date +"${start_month} ${start_year}") | awk 'NF {DAYS = $NF}; END {print DAYS}')

    ./data_to_csv "$partition" "$class" "$attribute" "$start_year" "${month[$start_month]}" "$days" "$regex"
    
    if [ "$start_month" -eq 12 ]; then
	start_month=1
	start_year=$((start_year+1))
    fi

    start_month=$((start_month+1))
done
