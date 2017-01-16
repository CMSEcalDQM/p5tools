#!/usr/bin/env python

import os
import subprocess

from ecaldqmconfig import config
from conddb import RunParameterDB

# RUN RANGE
#RUNS=[284165,284167]
#RUNS=[284191,284193,284195,284196]
#RUNS=[284230,284231]
#RUNS=[284777,284779,284781,284783]
#RUNS=[285102,285103,285106,285109,285111,285112,285113]
#RUNS=[285149,285150]
#RUNS=[285317,285320,285322,285324]
#RUNS=[285563,285566,285568]
#RUNS=[285892,285894,285895]
#RUNS=[286207,286208,286231,286232,286233]
#RUNS=[286208,286268,286270]
#RUNS=[286549,286551,286560,286561]
RUNS=[286585,286586]

# INITIAL PARAMETERS
LOGDIR='/data/dqm-data/logs'
#LOGDIR='/data/dqm-data_/logs'
#DATADIR='/data/test/dqmminidaq'
DATADIR='/dqmminidaq'
#WRKFLOW='/All/Run2016/MiniDAQ'
WRKFLOW='/All/Run2017/MiniDAQ'
TMPDIR='/data/dqm-data/tmp'
#TMPDIR='/data/dqm-data_/tmp'
PRESCALE=1

for run in RUNS:
    print " >>> Processing run",run
    # Check DB for ECAL/ES presence in run
    runParamDB = RunParameterDB(config.dbread.dbName, config.dbread.dbUserName, config.dbread.dbPassword)
    ecalIn = runParamDB.getRunParameter(run, 'CMS.LVL0:ECAL').lower() == 'in'
    esIn = runParamDB.getRunParameter(run, 'CMS.LVL0:ES').lower() == 'in'
    if not ecalIn and not esIn:
        print " >>> ECAL/ES not detected in run %d. Skipping..." % run
        continue

    ## ================ BEGIN OFFLINE DQM PROCESSING ================== ##
    open('%s/run%d/run%d_ls0000_EoR.jsn' % (DATADIR,run,run), 'w').close() # scanOnce=True will terminate DQM processing automatically as long as EoR is present

    # ECAL miniDAQ run
    if ecalIn:
        print " >>> Detected ECAL..."
        log = open(LOGDIR+'/ecalcalib_dqm_sourceclient-privlive_cfg.log','a')  # cmsRun log file
        proc = subprocess.call("source ~ecalpro/DQM/cmssw.sh; exec cmsRun /nfshome0/ecalpro/DQM/p5tools/ecalConfigBuilder.py runNumber=%d runInputDir=%s workflow=%s environment=PrivLive outputPath=%s verbosity=0 cfgType=CalibrationStandalone scanOnce=True prescaleFactor=%d" % (run,DATADIR,WRKFLOW,TMPDIR,PRESCALE), shell=True,stdout=log,stderr=subprocess.STDOUT)
        if proc == 0:
            print " ... process exited normally. Moving to closed/"
            #fName = 'DQM_V0002_R000%d__All__Run2016__MiniDAQ.root' % run
            fName = 'DQM_V0002_R000%d__All__Run2017__MiniDAQ.root' % run
            os.rename(TMPDIR+'/'+fName,TMPDIR+'/closed/'+fName)
        else:
            print "... abnormal termination:",proc

    # ES miniDAQ run
    if esIn:
        print " >>> Detected ES..."
        log = open(LOGDIR+'/es_dqm_sourceclient-privlive_cfg.log','a')  # cmsRun log file
        proc = subprocess.call("source ~ecalpro/DQM/cmssw.sh; exec cmsRun /nfshome0/ecalpro/DQM/p5tools/es_dqm_sourceclient-privlive_cfg.py runNumber=%d runInputDir=%s workflow=%s scanOnce=True prescaleFactor=%d" % (run,DATADIR,WRKFLOW,PRESCALE), shell=True,stdout=log,stderr=subprocess.STDOUT)
        if proc == 0:
            print " ... process exited normally. Moving to closed/"
            #fName = 'DQM_V0003_R000%d__All__Run2016__MiniDAQ.root' % run
            fName = 'DQM_V0003_R000%d__All__Run2017__MiniDAQ.root' % run
            os.rename(TMPDIR+'/'+fName,TMPDIR+'/closed/'+fName)
        else:
            print "... abnormal termination:",proc
