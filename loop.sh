#!/bin/bash

while true; do
    python3 main.py &
    PID=$!

    sleep 2m

    kill $PID
    echo 'avoiding memory leak: restarting script'

    sleep 10s
done
