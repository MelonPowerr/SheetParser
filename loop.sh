#!/bin/bash

while true; do
    python3 main.py &
    PID=$!

    sleep 1m

    kill -9 $PID

    sleep 20s
done
