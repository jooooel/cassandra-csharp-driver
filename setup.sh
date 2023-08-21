#!/bin/bash
for sub in {0..4}; do
    echo "Opening for 127.0.0.$sub"
    sudo ifconfig lo0 alias 127.0.0.$sub up;
done