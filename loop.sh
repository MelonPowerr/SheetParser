#!/bin/bash

while true; do
    # Start the Python script in the background
    python3 main.py &
    PID=$!

    # Wait for 30 minutes
    sleep 30m

    # Kill the Python script
    kill $PID

    # Wait for 20 seconds
    sleep 20s
done
