#!/bin/bash

echo "Setup tdaq-09-04-00"
#source  /cvmfs/atlas.cern.ch/repo/sw/tdaq/tools/cmake_tdaq/bin/cm_setup.sh tdaq-09-04-00

echo "Set location of this base dir as global variable BASE_DIR"
# Determine where this script is located
if [ "${BASH_SOURCE[0]}" != "" ]; then
    # This should work in bash.
    _src=${BASH_SOURCE[0]}
elif [ "${ZSH_NAME}" != "" ]; then
    # And this in zsh.
    _src=${(%):-%x}
elif [ "${1}" != "" ]; then
    # If none of the above works, we take it from the command line.
    _src="${1/setup.sh/}/setup.sh"
else
    echo  "Failed to determine the 'BASE_DIR'. Aborting ..."
    echo  "Give the source script location as additional argument"
    return 1
fi

# Set up MVA-Trainer environment variables that are picked up from the code.
export BASE_DIR="$(cd -P "$(dirname "${_src}")" && pwd)"

echo "The BASE_DIR is set to $BASE_DIR"
