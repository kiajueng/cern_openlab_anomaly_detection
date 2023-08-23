#!/bin/bash

partition=$1
class=$2
attribute=$3
start_year=$4
end_year=$5
start_month=$6
end_month=$7
start_day=$8
end_day=$9
regex=${10}
pattern="$(cut -d'.' -f1 <<<"$regex")"

month=(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec)

if [ "$start_year" -ge "$end_year" ] && [ "$start_month" -gt "$end_month" ]; then
    echo "There has to be at least one month difference if the Year is the same and Start Year <= End Year"
    exit 1
fi

while [ "$start_year" -ne "$end_year" ] || [ "$start_month" -le "$end_month" ]; do

    month_days=$(cal $(date +"${start_month} ${start_year}") | awk 'NF {DAYS = $NF}; END {print DAYS}')

    while [ "$start_day" -le "$month_days" ]; do
	echo "${start_day}.${month[$((start_month-1))]}.${start_year}"
	source $BASE_DIR/scripts/data_to_csv.sh "$partition" "$class" "$attribute" "$start_year" "${month[$((start_month-1))]}" "$start_day" "$pattern"
	start_day=$((start_day+1))
	if [ "$start_year" -eq "$end_year" ] && [ "$start_month" -eq "$end_month" ] && [ "$start_day" -gt "$end_day" ]; then
	    exit 0
	fi
    done
    
    if [ "$start_month" -eq 12 ]; then
	start_month=1
	start_year=$((start_year+1))
    fi

    start_day=1
    start_month=$((start_month+1))
done
