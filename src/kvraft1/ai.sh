#! /bin/bash

TEMP_FILE=$(mktemp)
LAB_URL="https://pdos.csail.mit.edu/6.824/labs/lab-kvraft1.html"

trap "rm -f '$TEMP_FILE'" EXIT

{
    echo "I am currently doing this homework for MIT Distributed Systems course."

    echo "These are the requirements:"
    echo "{$LAB_URL}"
    curl -s "$LAB_URL"
    echo -e "\n--- End of Requirements ---\n"

    echo "And the following is my progress."

    echo "client.go:"
    cat client.go

    echo "server.go:"
    cat server.go

    echo "rsm/rsm.go:"
    cat rsm/rsm.go

    echo -e "\n--- End of Files ---\n"

    echo "Am I on the right track? What could the next unit of work for me to implement be?"
    echo ""
} > "$TEMP_FILE"

cat "$TEMP_FILE" | pbcopy
echo "Success: Content copied to clipboard."
