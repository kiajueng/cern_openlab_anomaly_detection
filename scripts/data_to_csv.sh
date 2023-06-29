#!/bin/bash

partition=$1                                                                                                                                                                                   
class=$2                                                                                                                                                                                   
attribute=$3                                                                                                                                                                                  
year=$4                                                                                                                                                                             
month=$5
days=$6                                                                                                                                       
regex=$7     #Variables of certain modules from which to read                                                                                                                            
pattern="$(cut -d'.' -f1 <<<"$regex")" #  Pattern on which everything is split  

############################################################# Read data from pbeast and clean it #########################################################################################################
pbeast_read_repository -r /eos/atlas/atlascerngroupdisk/tdaq-opmon/pbeast -p "$partition" -c "$class" -a "$attribute" -s "${year}-${month}-1 0:00:00" -t "${year}-${month}-${days} 23:59:59" -O "$regex" > tmp.csv #Reads data from pbeast & save to tmp.csv

sed -i '1,/'"$pattern"'/ { /'"$pattern"'/!d }' tmp.csv # Delete all lines until first module variable
sed -i -e "/$pattern.*/s/://" tmp.csv # Deletes unnecessary : of each module it takes the values from                                                                                                     

############################################################ Save each individual variable in its own .csv file ##########################################################################################

mapfile -t lines < <(grep -n "$pattern.*" "tmp.csv" | cut -d: -f1) #Finds all lines with matching pattern

last_line_number=$(wc -l < "tmp.csv") #append last line of file --> array has one value too much so only need to loop over len(array - 1)
last_line_number=$((last_line_number+1)) #Increment by one so last line is included 
lines+=("$last_line_number")

array_length=(${#lines[@]})

if [[ ! -d "${BASE_DIR}/Data" ]]; then
    mkdir "${BASE_DIR}/Data"
fi

if [[ ! -d "${BASE_DIR}/Data/${year}" ]]; then
    mkdir "${BASE_DIR}/Data/${year}"
fi

if [[ ! -d "${BASE_DIR}/Data/${year}/{month}" ]]; then
    mkdir "${BASE_DIR}/Data/${year}/{month}"
fi

for ((i=0; i<$(($array_length - 1)); i+=1)); do
    matched_line="${lines[$i]}"
    next_line="${lines[$((i+1))]}"

    file_name="$(echo $(sed -n "${matched_line}p" tmp.csv) | sed 's/[./]/_/g').csv"  #Get line i and replace every "." and "/" with                                                                       
    # Do something with each matched line and the line that comes after it                    
    awk -v start="$matched_line" -v end="$next_line" 'NR>=start && NR<end' "tmp.csv" > "${BASE_DIR}/Data/${year}/${month}/${file_name}" #Save everything between two linenumbers in new file, excluding second line number 
    sed -i '1s/^/Date_Time,/' "${BASE_DIR}/Data/${year}/${month}/${file_name}"
    sed -i 's/    \[//g' "${BASE_DIR}/Data/${year}/${month}/${file_name}"
    sed -i 's/\] /,/g' "${BASE_DIR}/Data/${year}/${month}/${file_name}"
done

rm tmp.csv
