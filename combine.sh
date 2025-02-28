#!/bin/bash


cat part-r-00000 part-r-00001 part-r-00002 | sort -n | awk '{print $2}' | tr '\n' ' ' > output.txt

sed -i 's/ $//' output.txt

echo "Merged Output = output.txt"

