#!/bin/bash

file1="567579_parsed.txt"
file2="output.txt"

if [[ ! -f "$file1" || ! -f "$file2" ]]; then
    echo "Error: One or both files do not exist!"
    exit 1
fi

words1=$(tr ' ' '\n' < "$file1" | sort | uniq)
words2=$(tr ' ' '\n' < "$file2" | sort | uniq)

only_in_file1=$(comm -23 <(echo "$words1") <(echo "$words2"))
count_only_in_file1=$(echo "$only_in_file1" | wc -l)

only_in_file2=$(comm -13 <(echo "$words1") <(echo "$words2"))
count_only_in_file2=$(echo "$only_in_file2" | wc -l)

echo "Words only in $file1 ($count_only_in_file1 words):"
echo "$only_in_file1"

echo -e "\nWords only in $file2 ($count_only_in_file2 words):"
echo "$only_in_file2"

