#!/bin/bash

screen -dmS jobs

clear

echo "Step 1: Enter or generate your coinbase address!"

while true; do
    read -p "Do you wish to set the coinbase address?(Y or N)" yn
    case $yn in
        [Yy]* )
                miner_tools_path="$(pwd -P)/quarkchain/tools/miner_address.py"
                python3 $miner_tools_path
                break;;
        [Nn]* ) 
                break;;
        * ) echo "Please answer Y or N.";;
    esac
done





echo "Step 2: Download a snapshot of the database. Your cluster only need to sync
the blocks mined in the past 12 hours or less."


curl https://s3-us-west-2.amazonaws.com/testnet2/data/23/`curl https://s3-us-west-2.amazonaws.com/testnet2/data/23/LATEST`.tar.gz --output data.tar.gz 

tar xvfz data.tar.gz

miner_data_path="$(pwd -P)/quarkchain/cluster/data"

rm -rf $miner_data_path

mv data $miner_data_path


screen -S jobs -X screen bash

screen -S jobs -p bash -X title cluster

screen -S jobs -p cluster -X stuff $'python3 quarkchain/cluster/cluster.py --cluster_config "$(pwd -P)/testnet/2/cluster_config_template.json" 
'


seconds_left=20

echo "Step 3: Initiate the cluster and start synchorizing blocks in the past 12 hours or less. Please wait for time: ${seconds_left} seconds……"
while [ $seconds_left -gt 0 ];do
    echo -n "${seconds_left} seconds left"
    sleep 1
    seconds_left=$(($seconds_left - 1))
    echo -ne "\r     \r" 
done

echo "Step 4: Start synchorizing blocks in the past 12 hours or less. It takes about five minutes. Be patient!"
python3 quarkchain/tools/check_syncing_state.py


while true; do
    read -p "Step 4: Do you wish to start mining now?(Y or N)" yn
    case $yn in
        [Yy]* )
                screen -S jobs -X screen bash;
                screen -S jobs -p bash -X title miners;
                screen -S jobs -p miners -X stuff $'bash quarkchain/tools/external_miner_manager.sh -c "$(pwd -P)/testnet/2/cluster_config_template.json" -p 8 -h localhost 
                ';
                break;;
        [Nn]* ) bash quarkchain/tools/quick_miner_stopper.sh
                exit;;
        * ) echo "Please answer Y or N.";;
    esac
done

screen -rd

wait
