#!/usr/bin/env python

import os
import sys
import array
import glob
import subprocess

#RUNS=[284165,284167]
#RUNS=[284191,284193,284195,284196]
#RUNS=[284230,284231]
#RUNS=[284777,284779,284781,284783]
#RUNS=[285102,285103,285106,285109,285111,285112,285113]
#RUNS=[285149,285150]
#RUNS=[285317,285320,285322,285324]
RUNS=[285563,285566,285568]
LOGDIR='/data/dqm-data_/logs'
#DATDIR='/data/test/dqmminidaq'
DATDIR='/dqmminidaq'
WRKFLOW='/All/Run2016/MiniDAQ'
TMPDIR='/data/dqm-data/tmp'
#TMPDIR='/data/dqm-data_/tmp'

for run in RUNS:
    print " >>> Processing run",run
    log = open(LOGDIR+'/ecalcalib_dqm_sourceclient-privlive_cfg.log','a')
    #proc = subprocess.call("source ~ecalpro/DQM/cmssw.sh; exec cmsRun ecalcalibstandalone_dqm_sourceclient-privlive_cfg.py runNumber=%d runInputDir=%s scanOnce=True" % (run,DATDIR), shell=True,stdout=log,stderr=subprocess.STDOUT)
    proc = subprocess.call("source ~ecalpro/DQM/cmssw.sh; exec cmsRun /nfshome0/ecalpro/DQM/p5tools/ecalConfigBuilder.py runNumber=%d runInputDir=%s workflow=%s environment=PrivLive outputPath=%s verbosity=0 cfgType=CalibrationStandalone scanOnce=True prescaleFactor=%d" % (run,DATDIR,WRKFLOW,TMPDIR,1), shell=True,stdout=log,stderr=subprocess.STDOUT)
    if proc == 0:
        print " ... process exited normally. Moving to closed/"
        fName = 'DQM_V0002_R000%d__All__Run2016__MiniDAQ.root' % run
        os.rename(TMPDIR+'/'+fName,TMPDIR+'/closed/'+fName)
    else:
        print "... abnormal termination:",proc
