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

class GlobalRunFileCopyDaemon(object):
    # TEMPORARY SOLUTION (SEE BELOW)
    def __init__(self, run, startLumi, runInputDir, sources, logFile):
        self.run = run
        self.startLumi = startLumi
        self.targetDir = runInputDir + '/run%s' % run
        self.sources = sources # {stream: (node, dir, suffix), ..}
        self._stop = threading.Event()
        self.allLumis = dict([(stream, set([])) for stream in self.sources.keys()])
        self.logFile = logFile

        try:
            shutil.rmtree(self.targetDir, True)
        except:
            pass

        try:
            os.makedirs(self.targetDir)
        except OSError:
            pass

        self.updateList()


    def __del__(self):
        self._stop.set()
        try:
            shutil.rmtree(self.targetDir, True)
        except:
            pass

    def updateList(self):
        for stream, (node, directory, suffix) in self.sources.items():
            self.allLumis[stream].clear()

            proc = subprocess.Popen('ssh {node} "ls {rundir}/run{run}/run{run}_ls*"'.format(node = node, rundir = directory, run = self.run), shell = True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
            data = proc.communicate()[0]
    
            if proc.poll() != 0:
                return

            fullPaths = data.split()

            deleted = []
            for filename in fullPaths:
                matches = re.search('run{run}_ls([0-9]+)_stream{stream}_{suffix}[.]dat[.]deleted$'.format(run = self.run, stream = stream, suffix = suffix), filename)
                if matches:
                    deleted.append(int(matches.group(1)))

            for filename in fullPaths:
                matches = re.search('run{run}_ls([0-9]+)_stream{stream}_{suffix}[.]jsn$'.format(run = self.run, stream = stream, suffix = suffix), filename)
                if not matches: continue
                lumi = int(matches.group(1))
                if lumi >= self.startLumi and lumi not in deleted:
                    self.allLumis[stream].add(lumi)

    def copy(self, stream, lumi):
        node, directory, suffix = self.sources[stream]

        fileName = 'run%d_ls%04d_stream%s_%s' % (self.run, lumi, stream, suffix)

        if os.path.exists(self.targetDir + '/' + fileName + '.jsn'): return True
        
        proc = subprocess.Popen(['scp', '{node}:{directory}/run{run}/'.format(node = node, directory = directory, run = self.run) + fileName + '.dat', self.targetDir], stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
        proc.communicate()

        proc = subprocess.Popen(['scp', '{node}:{directory}/run{run}/'.format(node = node, directory = directory, run = self.run) + fileName + '.jsn', self.targetDir], stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
        try:
            proc.communicate()
        except KeyboardInterrupt:
            proc.terminate()
            proc.communicate()
            raise

        if proc.returncode == 0:
            self.logFile.write('Copied ' + fileName + ' from ' + node)
        else:
            self.logFile.write('Failed to copy ' + fileName + ' from ' + node)

    def start(self):
        copied = dict([(stream, set([])) for stream in self.sources.keys()])

        try:
            os.unlink('{rundir}/run{run}/run{run}_ls0000_EoR.jsn'.format(rundir = self.targetDir, run = self.run))
        except:
            pass

        while True:
            nEnded = 0
            for stream, allLumis in self.allLumis.items():
                lumis = sorted(list(allLumis - copied[stream]))

                if len(lumis) == 0: continue # potentially cause hang-up if EoR is somehow not written..

                if lumis[0] == 0:
                    nEnded += 1
                    lumis.pop(0)

                self.logFile.write(('Lumis to copy for stream %s: ' % stream) + str(lumis))
    
                for lumi in lumis:
                    if self._stop.isSet():
                        break
    
                    if lumi < self.startLumi:
                        copied[stream].add(lumi)
                        continue
    
                    self.copy(stream, lumi)
                    copied[stream].add(lumi)
    
                if self._stop.isSet():
                    break

            if nEnded == len(self.allLumis) or self._stop.isSet():
                break

            time.sleep(60)

            self.updateList()


    def stop(self):
        self._stop.set()



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
#        commonOptions = 'runNumber={run} runInputDir={inputDir} workflow=/{dataset}/{period}/CentralDAQ'.format(run = run, inputDir = '/tmp/onlineDQM', dataset = workflowBase, period = config.period)

#        if ecalIn:
#            ecalOptions = 'environment=PrivLive outputPath={outputPath} verbosity={verbosity}'.format(outputPath = config.tmpoutdir, verbosity = VERBOSITY)
#
#            log = open(config.logdir + '/ecal_dqm_sourceclient-privlive_cfg.log', 'a')
#            log.write('\n\n\n')
#            command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {common} {ecal} {spec}'.format(conf = config.workdir + '/ecalConfigBuilder.py', common = commonOptions, ecal = ecalOptions, spec = 'cfgType=Physics')
#            proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
#            logFile.write(command)
#            procs['Physics'] = (proc, log)
    
#            log = open(config.logdir + '/ecalcalib_dqm_sourceclient-privlive_cfg.log', 'a')
#            log.write('\n\n\n')
#            command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {common} {ecal} {spec}'.format(conf = config.workdir + '/ecalConfigBuilder.py', common = commonOptions, ecal = ecalOptions, spec = 'cfgType=Calibration')
#            proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
#            logFile.write(command)
#            procs['Calibration'] = (proc, log)

        if esIn:
            log = open(config.logdir + '/es_dqm_sourceclient-privlive_cfg.log', 'a')
            log.write('\n\n\n')
            command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {common}'.format(conf = config.workdir + '/es_dqm_sourceclient-privlive_cfg.py', common = commonOptions)
            proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
            logFile.write(command)
            procs['ES'] = (proc, log)

    elif daq == 'minidaq':
        if not os.path.isdir('/dqmminidaq/run%d' % run):
            logFile.write('DQM stream was not produced')
            return {}

        commonOptions = 'runNumber={run} runInputDir={inputDir} workflow=/{dataset}/{period}/MiniDAQ'.format(run = run, inputDir = '/dqmminidaq', dataset = workflowBase, period = config.period)

        if ecalIn:
        
            ecalOptions = 'environment=PrivLive outputPath={outputPath} verbosity={verbosity}'.format(outputPath = config.tmpoutdir, verbosity = VERBOSITY)
            
            log = open(config.logdir + '/ecalcalib_dqm_sourceclient-privlive_cfg.log', 'a')
            log.write('\n\n\n')
            command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {common} {ecal} {spec}'.format(conf = config.workdir + '/ecalConfigBuilder.py', common = commonOptions, ecal = ecalOptions, spec = 'cfgType=CalibrationStandalone')
            proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
            logFile.write(command)
            procs['Calibration'] = (proc, log)

        if esIn:
            log = open(config.logdir + '/es_dqm_sourceclient-privlive_cfg.log', 'a')
            log.write('\n\n\n')
            command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {common}'.format(conf = config.workdir + '/es_dqm_sourceclient-privlive_cfg.py', common = commonOptions)
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

    runType = ' runType='
    type = runParamDB.getRunParameter(currentRun, 'CMS.ECAL:LOCAL_CONFIGURATION_KEY').lower()
    if 'pedestal' in type:
        runType += 'PEDESTAL'
    elif 'cosmic' in type:
        runType += 'COSMIC'
    elif 'physics' in type:
        runType += 'PHYSICS'
    else:
        logFile.write("Run type undefined. Use PHYSICS")
        runType += 'PHYSICS'

#    condDB.setMonRunOutcome(run, 'dqmdone')
    logFile.write('Start Subprocess')
# now write DQM results to DB using CMSSW
    inputFiles = ''
    MGPAgain=''
    MGPAgain += ' MGPAGains='+ str(1) + " MGPAGains="+ str(6) + " MGPAGains=" + str(12)
    for fileName in os.listdir(config.tmpoutdir):
        if 'R%09d'%run in fileName:
            inputFiles += ' inputFiles=' + config.tmpoutdir + '/' + fileName

            command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {inputFiles} {MGPAgain} {runType}'.format(conf = config.workdir + '/writeDB_cfg.py', inputFiles = inputFiles, MGPAgain=MGPAgain, runType=runType)
            logFile.write(command)
            log = open(config.logdir + '/dbwrite.log', 'a')
            proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
            logFile.write(command)

            while proc.poll() is None:
                time.sleep(10)

            log.close()

    return SUCCESS
    try:
        logFile.write('MonRun IOV:', condDB.getMonRunIOV(condDB.getRunIOV(run)).values['IOV_ID'])
        return SUCCESS
    except:
        logFile.write('DB writing failed.')
        return FAILED

    return SUCCESS

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

    runType = runParamDB.getRunParameter(currentRun, 'CMS.ECAL:LOCAL_CONFIGURATION_KEY').lower() 
    logFile.write('local config key: ', runType)

    if daq == 'central':
#        copyDaemon = GlobalRunFileCopyDaemon(currentRun, startLumi, '/tmp/onlineDQM', {'LookArea': ('bu-c2f13-29-01', '/fff/output/lookarea', 'StorageManager')}, logFile)
#        if sum(map(len, copyDaemon.allLumis.values())) == 0:
#            logFile.write('No files to be copied.')
#            if isLatestRun:
#                returnValue[0] = WAIT
#            else:
#                returnValue[0] = NEXT
#            return

#        logFile.write('Starting file copy daemon')
#        copyThread = threading.Thread(target = GlobalRunFileCopyDaemon.start, args = (copyDaemon,))
#        copyThread.start()
        logFile.write('Central DAQ run: currently not avaiable')
        returnValue[0] = NEXT
        return

    dqmRunKey = runParamDB.getRunParameter(currentRun, 'CMS.LVL0:DQM_RUNKEY_AT_CONFIGURE').lower()

    logFile.write('DQM key: ', dqmRunKey if dqmRunKey else 'Undefined')

    procs = startDQM(currentRun, startLumi, daq, dqmRunKey, ecalIn, esIn, logFile)

    if len(procs) == 0:
        logFile.write('No CMSSW job to execute')
#        if daq == 'central':
#            copyDaemon.stop()

        returnValue[0] = WAIT
        return

    if daq == 'central':
        runDir = '/tmp/onlineDQM/run%d' % currentRun
    else:
        runDir = '/dqmminidaq/run%d' % currentRun

    results = {}

    while len(results) != len(procs):
        if stopFlag.isSet():
#            if daq == 'central':
#                print 'Stopping copy daemon.'
#                copyDaemon.stop()
#                copyThread.join()

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
#        else:
#            if daq == 'central' and not copyThread.isAlive():
#                open(runDir + '/run%d_ls0000_EoR.jsn' % currentRun, 'w').close()

        for pname, (proc, log) in procs.items():
            status = checkProcess(proc)
            if status != RUNNING:
                results[pname] = status
                log.close()

#    if daq == 'central':
#        copyDaemon.stop()
#        copyThread.join()

    if stopFlag.isSet():
        returnValue[0] = KILLED
        return
    
    if FAILED not in results.values():
        logFile.write('CMSSW job successfully returned.')

        if daq == 'minidaq':
            result = writeDB(currentRun, ecalCondDB, runParamDB, logFile)
        elif daq == 'central':
            result = SUCCESS
        if result == SUCCESS:
            logFile.write('Copying ROOT file to closed')

            runname = 'R%09d'%currentRun 
            for filename in os.listdir(config.tmpoutdir):
                if runname in filename:
                    source_file=config.tmpoutdir+'/'+filename
                    destination_file=config.tmpoutdir+'/closed/'+filename
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
