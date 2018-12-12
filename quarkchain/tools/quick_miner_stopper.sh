#!/bin/bash


pkill pypy3

screen -ls | grep -i detached | cut -d. -f1 | tr -d [:blank:]| xargs kill