#! /bin/bash

count=1
while go test -race; do
    echo "Run $count passed"
    ((count++))
done
echo "Run $count FAILED"
