#!/bin/bash

pkill -f master.py

pkill -f slave.py

screen -ls | grep -i detached | cut -d. -f1 | tr -d [:blank:]| xargs kill

wait