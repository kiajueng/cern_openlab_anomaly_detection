#!/bin/bash

partition=$1  
class=$2
attribute=$3                                
year=$4               
month=$5
days=$6                                                               
pattern=$7    #Pattern on which to split data to files  

############################################################# Read data from pbeast and pipe to tmp.csv ##################################################################################################
pbeast_read_repository -r /eos/atlas/atlascerngroupdisk/tdaq-opmon/pbeast -p "$partition" -c "$class" -a "$attribute" -s "${year}-${month}-${days} 00:00:00" -t "${year}-${month}-${days} 23:59:59" -O "$regex" > "${BASE_DIR}/tmp.csv"
############################################################ Save each individual variable in its own .csv file ##########################################################################################
if [[ ! -d "${BASE_DIR}/Data" ]]; then
    mkdir "${BASE_DIR}/Data"
fi

if [[ ! -d "${BASE_DIR}/Data/${year}" ]]; then
    mkdir "${BASE_DIR}/Data/${year}"
fi

if [[ ! -d "${BASE_DIR}/Data/${year}/${month}" ]]; then
    mkdir "${BASE_DIR}/Data/${year}/${month}"
fi

if [[ ! -d "${BASE_DIR}/Data/${year}/${month}/${days}" ]]; then
    mkdir "${BASE_DIR}/Data/${year}/${month}/${days}"
fi

python3 "${BASE_DIR}/scripts/data_cleaning.py" -d "$days" -m "$month" -y "$year" -p "$pattern"

rm "${BASE_DIR}/Data/${year}/${month}/${days}/*.csv"
rm "${BASE_DIR}/tmp.csv"
