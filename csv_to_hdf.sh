#!/bin/bash
set -eu

export PATH=/work/pa24/kpanag/unzip60:$PATH

export PATH=$PATH:/users/pa24/kpanag/.local/bin

if [ -z "${LD_LIBRARY_PATH+x}" ]; then
    export LD_LIBRARY_PATH=/users/pa24/kpanag/local/lib
else
    export LD_LIBRARY_PATH=/users/pa24/kpanag/local/lib:$LD_LIBRARY_PATH
fi

# Function to process a single month
process_month() {
    year=$1
    month=$(printf "%02d" $((10#$2)))
    hdf5_file="/work/pa24/kpanag/output/${year}${month}.h5"
    SAS_FILES=()
    declare -A SAS_FILES_GROUP
    declare -A SAS_FILES_TYPE
    for d in $(seq -f "%02g" 1 1 10); do
        for type in wct ; do
            file_pattern="/work/pa24/kpanag/ct_93_06/taq_msec${year}/m${year}${month}/taq.WCT_${year}${month}${d}.zip"
            for file_name in $file_pattern; do
                if [ -e "$file_name" ]; then
                    SAS_FILES+=("$file_name")
                    SAS_FILES_GROUP["$file_name"]="${d}"
                    SAS_FILES_TYPE["$file_name"]="$type"
                fi
            done
        done
    done

    if [ ${#SAS_FILES[@]} -eq 0 ]; then
        echo "No SAS files found for $year-$month."
        return 1
    fi

    for SAS_FILE in "${SAS_FILES[@]}"; do
        BASE_NAME=$(basename "$SAS_FILE" .zip)
        CSV_FILE="${BASE_NAME}.csv"
        TYPE_NAME="${SAS_FILES_TYPE["$SAS_FILE"]}"
        GROUP_NAME="day${SAS_FILES_GROUP["$SAS_FILE"]}"

        unzip -p "$SAS_FILE" "$CSV_FILE" |python3.11 /work/pa24/kpanag/scripts/corrected_run/taq93_06/wct/hdf_struct_wct.py "$hdf5_file" "$GROUP_NAME" "$TYPE_NAME"
        echo "$SAS_FILE to $hdf5_file."
        rm "$SAS_FILE"
    done
}

process_month "$1" "$2"
