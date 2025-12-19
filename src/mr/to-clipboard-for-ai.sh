#! /bin/bash

# Create a secure temporary file
TEMP_FILE=$(mktemp)
LAB_URL="https://pdos.csail.mit.edu/6.824/labs/lab-mr.html"

# Ensure the temp file is deleted even if the script is interrupted (Ctrl+C)
trap "rm -f '$TEMP_FILE'" EXIT

# 1. Add the introductory header
{
    echo "I am currently doing the first Lab homework for MIT Distributed Systems course."
    echo ""
    echo "These are the requirements:"
    echo "{$LAB_URL}"
    echo ""

    # Fetch the URL content silently
    curl -s "$LAB_URL"

    echo -e "\n--- End of Requirements ---\n"
    echo "And the following is my progress."
    echo ""

    # 2. Iterate through .go files
    for file in *.go; do
        if [ -e "$file" ]; then
            echo "{$file}:"
            echo ""
            cat "$file"
            echo -e "\n"
        fi
    done

    # sequential example
    echo "../main/mrsequential.go"
    echo ""
    cat "../main/mrsequential.go"
    echo -e "\n"

    # Final questino
    echo -e "\n--- End of Files ---\n"
    echo "Am I on the right track? What could the next unit of work for me to implement be?"
    echo ""
} > "$TEMP_FILE"

# 3. Copy to clipboard
cat "$TEMP_FILE" | pbcopy

echo "Success: Content copied to clipboard using temp file: $TEMP_FILE"
