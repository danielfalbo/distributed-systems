#! /bin/bash

TEMP_FILE=$(mktemp)
LAB_URL="https://pdos.csail.mit.edu/6.824/labs/lab-kvsrv1.html"

trap "rm -f '$TEMP_FILE'" EXIT

{
    echo "I am currently doing this homework for MIT Distributed Systems course."
    echo ""
    echo "These are the requirements:"
    echo "{$LAB_URL}"
    echo ""

    curl -s "$LAB_URL"

    echo -e "\n--- End of Requirements ---\n"
    echo "And the following is my progress."
    echo ""

    echo "client.go:" ; echo ""; cat "client.go" ; echo -e "\n"
    echo "server.go:" ; echo ""; cat "server.go" ; echo -e "\n"
    echo "rpc/rpc.go:"; echo ""; cat "rpc/rpc.go"; echo -e "\n"

    echo -e "\n--- End of Files ---\n"
    echo "Am I on the right track? What could the next unit of work for me to implement be?"
    echo ""
} > "$TEMP_FILE"

cat "$TEMP_FILE" | pbcopy
echo "Success: Content copied to clipboard."
