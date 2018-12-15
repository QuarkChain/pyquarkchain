#!/bin/bash

pkill -f master.py

pkill -f slave.py

pkill -f external_miner.py

pkill -f check_syncing_state.py

screen -ls | grep -i detached | cut -d. -f1 | tr -d [:blank:] | xargs kill

wait