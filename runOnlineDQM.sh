#!/bin/bash

source $HOME/DQM.new/cmssw.sh

python $HOME/DQM.new/p5tools/onlineDQM.py $1 $2
