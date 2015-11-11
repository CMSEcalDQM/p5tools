import os
import sys
import time
import subprocess
import threading
import re
import traceback
import shutil

from ecaldqmconfig import config
from conddb import EcalCondDB, RunParameterDB
from logger import Logger

VERBOSITY = 0

def startDQM(run, startLumi, daq, dqmRunKey, ecalIn, esIn, logFile):
    """
    Collect the run parameters and start the DQM job if a run is detected.
    """

    logFile.write('Processing run', run)

    if dqmRunKey == 'cosmic_run':
        workflowBase = 'Cosmics'
    elif dqmRunKey == 'pp_run':
        workflowBase = 'Protons'
    elif dqmRunKey == 'hi_run':
        workflowBase = 'HeavyIons'
    else:
        workflowBase = 'All'

    procs = {}

    if daq == 'central':
        return {}

    elif daq == 'minidaq':
#        if not os.path.isdir('/dqmminidaq/run%d' % run):
        if not os.path.isdir('/tmp/dqmminidaq/run%d' % run):
            logFile.write('DQM stream was not produced')
            return {}

#        commonOptions = 'runNumber={run} runInputDir={inputDir} workflow=/{dataset}/{period}/MiniDAQ'.format(run = run, inputDir = '/dqmminidaq', dataset = workflowBase, period = config.period)
        commonOptions = 'runNumber={run} runInputDir={inputDir} workflow=/{dataset}/{period}/MiniDAQ'.format(run = run, inputDir = '/tmp/dqmminidaq', dataset = workflowBase, period = config.period)

        if ecalIn:
        
            ecalOptions = 'environment=PrivLive outputPath={outputPath} verbosity={verbosity}'.format(outputPath = config.tmpoutdir, verbosity = VERBOSITY)
            
            log = open(config.logdir + '/ecalcalib_dqm_sourceclient-privlive_cfg.log', 'a')
            log.write('\n\n\n')
            command = 'source $HOME/DQM/test_msun/new/cmssw.sh; exec cmsRun {conf} {common} {ecal} {spec}'.format(conf = config.workdir + '/ecalConfigBuilder.py', common = commonOptions, ecal = ecalOptions, spec = 'cfgType=CalibrationStandalone')
            proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
            logFile.write(command)
            procs['Calibration'] = (proc, log)

        if esIn:
            log = open(config.logdir + '/es_dqm_sourceclient-privlive_cfg.log', 'a')
            log.write('\n\n\n')
            command = 'source $HOME/DQM/test_msun/new/cmssw.sh; exec cmsRun {conf} {common}'.format(conf = config.workdir + '/es_dqm_sourceclient-privlive_cfg.py', common = commonOptions)
            proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
            logFile.write(command)
            procs['ES'] = (proc, log)

    logFile.write('Running configurations:', sorted(procs.keys()))

    return procs


RUNNING = 0
SUCCESS = 1
FAILED = 2

def checkProcess(process):

    if process.poll() is None: return RUNNING

    if process.returncode == 0: return SUCCESS
    else: return FAILED

def writeDB(run, condDB, runParamDB, logFile):
    logFile.write('Start DB writing.')

    daq = runParamDB.getDAQType(run)
    if daq == 'central':
        generalTag = 'PHYSICS'
    elif daq == 'minidaq':
        generalTag = 'GLOBAL'
    else:
        logFile.write('DAQ type undefined')
        return FAILED

#    condDB.setMonRunOutcome(run, 'dqmdone')
    logFile.write('Start Subprocess')
    # now write DQM results to DB using CMSSW
    inputFiles = ''
    MGPAgain=''
    MGPAgain += ' MGPAGains='+ str(1) + " MGPAGains="+ str(6) + " MGPAGains=" + str(12)
    for fileName in os.listdir(config.tmpoutdir):
        if '%09d' % run in fileName:
            inputFiles += ' inputFiles=' + config.tmpoutdir + '/' + fileName

        command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {inputFiles} {MGPAgain}'.format('/nfshome0/ecalpro/DQM/test_msun/writeDB_cfg.py', inputFiles = inputFiles, MGPAgain=MGPAgain)
        logFile.write(command)
        log = open(config.logdir + '/dbwrite.log', 'a')
        proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)

        while proc.poll() is None:
            time.sleep(10)

        log.close()

    return SUCCESS
#    try:
#        logFile.write('MonRun IOV:', condDB.getMonRunIOV(condDB.getRunIOV(run)).values['IOV_ID'])
#        return SUCCESS
#    except:
#        logFile.write('DB writing failed.')
#        return FAILED

#    return SUCCESS

UNKNOWN = -1
WAIT = 0
NEXT = 1
KILLED = 2

def processRun(currentRun, startLumi, logFile, ecalCondDB, runParamDB, isLatestRun, stopFlag, returnValue):

    logFile.write('Checking ECAL presence in run', currentRun)

    ecalIn = runParamDB.getRunParameter(currentRun, 'CMS.LVL0:ECAL').lower() == 'in'
    esIn = runParamDB.getRunParameter(currentRun, 'CMS.LVL0:ES').lower() == 'in'

    if not ecalIn and not esIn:
        logFile.write('ECAL not in the run')
        returnValue[0] = NEXT
        return

    daq = runParamDB.getDAQType(currentRun)

    logFile.write('DAQ type: ', daq)

    if daq == 'central':
        logFile.write('Central DAQ run: currently not avaiable')
        returnValue[0] = NEXT
        return

    dqmRunKey = runParamDB.getRunParameter(currentRun, 'CMS.LVL0:DQM_RUNKEY_AT_CONFIGURE').lower()

    logFile.write('DQM key: ', dqmRunKey if dqmRunKey else 'Undefined')

    procs = startDQM(currentRun, startLumi, daq, dqmRunKey, ecalIn, esIn, logFile)

    if len(procs) == 0:
        logFile.write('No CMSSW job to execute')
        returnValue[0] = WAIT
        return

    if daq == 'central':
        runDir ='' 
    else:
#        runDir = '/dqmminidaq/run%d' % currentRun
        runDir = '/tmp/dqmminidaq/run%d' % currentRun

    results = {}

    while len(results) != len(procs):
        if stopFlag.isSet():
            print 'Soft kill DQM processes.'
            open(runDir + '/run%d_ls0000_EoR.jsn' % currentRun, 'w').close()
            time.sleep(5)
            for pname, (proc, log) in procs.items():
                if proc.poll() is None:
                    print 'Hard kill', pname
                    proc.kill()
                    proc.wait()
                
            returnValue[0] = KILLED
            return

        if isLatestRun:
            if currentRun < runParamDB.getLatestEcalRun():
                open(runDir + '/run%d_ls0000_EoR.jsn' % currentRun, 'w').close()
        else:
            if daq == 'central':
                returnValue[0] = NEXT
                return

        for pname, (proc, log) in procs.items():
            status = checkProcess(proc)
            if status != RUNNING:
                results[pname] = status
                log.close()

    if daq == 'central':
        returnValue[0] = NEXT
        return

    if stopFlag.isSet():
        returnValue[0] = KILLED
        return
    
    if FAILED not in results.values():
        logFile.write('CMSSW job successfully returned.')

#        result = writeDB(currentRun, ecalCondDB, runParamDB, logFile)

        result = SUCCESS
        if result == SUCCESS:
            logFile.write('Copying ROOT file to closed')

            runname = 'R%09d'%currentRun 
            for filename in os.listdir('/data/ecalod-disk01/dqm-data/tmp/'):
                if runname in filename:
                    source_file='/data/ecalod-disk01/dqm-data/tmp/'+ filename
                    destination_file='/data/ecalod-disk01/dqm-data/tmp/closed/'+ filename
                    try:
                        os.rename(source_file,destination_file)
                    except OSError:
                        pass

    else:
        logFile.write('CMSSW job failed.')
#                ecalCondDB.setMonRunOutcome(currentRun, 'dqmfail')

    returnValue[0] = NEXT
    return


def takeCommands(stopFlag):
    while True:
        sys.stdout.write('DQM Control> ')
        sys.stdout.flush()
        command = sys.stdin.readline().strip()
        if command == 'quit':
            print 'Terminating DQM..'
            stopFlag.set()
            return
        else:
            print 'Invalid command', command


if __name__ == '__main__':

    from optparse import OptionParser

    parser = OptionParser(usage = 'Usage: onlineDQM.py [-r [-w]] startRun [startLumi]')

    parser.add_option('-r', '--reprocess', dest = 'reprocess', action = 'store_true', help = 'Reprocess a run instead of running as daemon.')
    parser.add_option('-w', '--write-database', dest = 'writeDatabase', action = 'store_true', help = 'Write to database in reprocess. DB writing is automatic for online DQM.')

    (options, args) = parser.parse_args()

    try:
        runParamDB = RunParameterDB(config.dbread.dbName, config.dbread.dbUserName, config.dbread.dbPassword)
        ecalCondDB = EcalCondDB(config.dbwrite.dbName, config.dbwrite.dbUserName, config.dbwrite.dbPassword)

        latest = runParamDB.getLatestEcalRun()

        print 'latest run', latest 
        try:
            currentRun = int(args[0])
            if currentRun > latest: currentRun = latest
        except:
            print 'Run number not specified in command line.'
            currentRun = latest

        print currentRun

        isLatestRun = currentRun == latest
    
        try:
            startLumi = int(args[1])
        except:
            startLumi = 0
    
        try:
            os.rename(config.logdir + '/reprocess.log', config.logdir + '/old/reprocess.log')
        except OSError:
            pass

        logFile = Logger(config.logdir + '/onlineDQM.log', 'w')
        logFile.write('ECAL Online DQM')

        stopFlag = threading.Event()

        controlThread = threading.Thread(target = takeCommands, args = (stopFlag,))
        controlThread.daemon = True
        controlThread.start()

        while True:
            logFile.write('')
            logFile.write('*** Run ' + str(currentRun) + ' ***')

            returnValue = [UNKNOWN]
            dqmThread = threading.Thread(target = processRun, args = (currentRun, startLumi, logFile, ecalCondDB, runParamDB, isLatestRun, stopFlag, returnValue))
            dqmThread.start()

            while returnValue[0] == UNKNOWN:
                dqmThread.join(5)

            if stopFlag.isSet() or options.reprocess:
                break

            if returnValue[0] == WAIT:
                time.sleep(60)
                currentRun = runParamDB.getLatestEcalRun()
            else:
                while currentRun == runParamDB.getLatestEcalRun():
                    isLatestRun = True
                    time.sleep(60)

                currentRun += 1
                startLumi = 0

        logFile.close()

    except:
        raise

    finally:
        runParamDB.close()
        ecalCondDB.close()
