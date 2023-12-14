#!/bin/bash

while true; do
    python3 main.py &
    PID=$!

    sleep 2m

    kill -9 $PID

    sleep 10s
done
