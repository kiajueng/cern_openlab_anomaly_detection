#!/bin/bash

partition=$1 #Partition                                                                                                                                                                                   
class=$2     #Class                                                                                                                                                                                   
attribute=$3 #Attribute                                                                                                                                                                                  
start=$4     #Start time                                                                                                                                                                                  
end=$5       #End Time                                                                                                                                             
regex=$6     #Variables of certain modules from which to read                                                                                                                            
pattern="$(cut -d'.' -f1 <<<"$regex")" #  Pattern on which everything is split  

############################################################# Read data from pbeast and clean it #########################################################################################################
pbeast_read_repository -r /eos/atlas/atlascerngroupdisk/tdaq-opmon/pbeast -p "$partition" -c "$class" -a "$attribute" -s "$start" -t "$end" -O "$regex" > tmp.csv #Reads data from pbeast & save to tmp.csv

sed -i '1,/'"$pattern"'/ { /'"$pattern"'/!d }' tmp.csv # Delete all lines until first module variable
sed -i -e "/$pattern.*/s/://" tmp.csv # Deletes unnecessary : of each module it takes the values from                                                                                                     

############################################################ Save each individual variable in its own .csv file ##########################################################################################

mapfile -t lines < <(grep -n "$pattern.*" "tmp.csv" | cut -d: -f1) #Finds all lines with matching pattern

last_line_number=$(wc -l < "tmp.csv") #append last line of file --> array has one value too much so only need to loop over len(array - 1)
lines+=("$last_line_number")

array_length=(${#lines[@]})

for ((i=0; i<$(($array_length - 1)); i+=1)); do
    matched_line="${lines[$i]}"
    next_line="${lines[$((i+1))]}"

    file_name="$(echo $(sed -n "${matched_line}p" tmp.csv) | sed 's/[./]/_/g').csv"  #Get line i and replace every "." and "/" with                                                                       
    # Do something with each matched line and the line that comes after it                    
    awk -v start="$matched_line" -v end="$next_line" 'NR>=start && NR<end' "tmp.csv" > "$file_name" #Save everything between two linenumbers in new file, excluding second line number 
    sed -i '1s/^/Date_Time,/' "$file_name"
    sed -i 's/    \[//g' "$file_name"
    sed -i 's/\] /,/g' "$file_name"
done
