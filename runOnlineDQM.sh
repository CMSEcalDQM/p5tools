#!/bin/bash

source $HOME/DQM/cmssw.sh

python $HOME/DQM/p5tools/onlineDQM.py $1 $2
