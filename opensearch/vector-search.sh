#!/bin/bash

# Loop 2000 times
for i in {1..2000}
do
  # Execute the Python script
  python3 cohere-embed-generator.py "Text input $i" "vector-index" >> ~/environment/result_default.txt

  # Wait for 2 seconds
  sleep 2
done