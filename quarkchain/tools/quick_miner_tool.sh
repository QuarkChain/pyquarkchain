#!/bin/bash

clear
echo "Step 1: Enter or generate your coinbase address!"

miner_tools_path="$(pwd -P)/quarkchain/tools/miner_address.py"
pypy3 $miner_tools_path

echo -n "Enter your coinbase address again!"   
echo -n "Paste your address:"                
read  address                                   
    



screen -dmS jobs

screen -S jobs -X screen bash

screen -S jobs -p bash -X title cluster

screen -S jobs -p cluster -X stuff $'pypy3 quarkchain/cluster/cluster.py --cluster_config "$(pwd -P)/testnet/2/cluster_config_template.json" 
'

echo "Dear quarkchain miner, your tQkc address is $address." 


echo "Step 2: Download a snapshot of the database. Your cluster only need to sync
the blocks mined in the past 12 hours or less."



curl https://s3-us-west-2.amazonaws.com/testnet2/data/22/`curl https://s3-us-west-2.amazonaws.com/testnet2/data/22/LATEST`.tar.gz --output data.tar.gz 

tar xvfz data.tar.gz

miner_data_path="$(pwd -P)/quarkchain/cluster/data"

rm -rf $miner_data_path

mv data $miner_data_path

seconds_left=20

echo "Step 3: Initial the cluster and start synchorizing blocks in the past 12 hours or less. Please wait for time: ${seconds_left} seconds……"
while [ $seconds_left -gt 0 ];do
    echo -n "${seconds_left} seconds left"
    sleep 1
    seconds_left=$(($seconds_left - 1))
    echo -ne "\r     \r" 
done

echo "Step 4: Start synchorizing blocks in the past 12 hours or less. It takes about five minutes. Be patient!"
pypy3 quarkchain/tools/check_syncing_state.py





while true; do
    read -p "Step 4: Do you wish to start mining now?(Y or N)" yn
    case $yn in
        [Yy]* ) bash quarkchain/tools/external_miner_manager.sh -c "$(pwd -P)/testnet/2/cluster_config_template.json" -p 9 -t 1 -h localhost;
                break;;
        [Nn]* ) bash quarkchain/tools/quick_miner_stopper.sh
                exit;;
        * ) echo "Please answer Y or N.";;
    esac
done

screen -rd