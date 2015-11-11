#!/bin/bash

if [ $1 = "start" ]; then

    if [ -n "$(screen -list | grep onlineDQM)" ]; then
	echo "Online DQM already running."
	echo "Escape sequence: Ctrl-a d"
	sleep 2
	screen -r onlineDQM
	exit
    fi

    screen -d -m -S onlineDQM $HOME/DQM.new/p5tools/runOnlineDQM.sh $2 $3

elif [ $1 = "stop" ]; then

    screen -r onlineDQM -X quit

else
    
    echo "Invalid command $1"

fi
