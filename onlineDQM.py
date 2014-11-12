import os
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
        self.sources = sources # [(node, dir, stream, suffix), ..]
        self._stop = threading.Event()
        self.allLumis = set([])
        self.logFile = logFile

        try:
            shutil.rmtree(self.targetDir, True)
        except:
            pass

        try:
            os.mkdir(self.targetDir)
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
        self.allLumis = set([])

        for node, directory, stream, suffix in self.sources:
            proc = subprocess.Popen('ssh {node} "ls {rundir}/run{run}/run{run}_ls*"'.format(node = node, rundir = directory, run = self.run), shell = True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
            data = proc.communicate()[0]
    
            if proc.poll() != 0:
                return

            fullPaths = data.split()
            for filename in fullPaths:
                matches = re.search('run%d_ls([0-9]+)_.*[.]jsn$' % self.run, filename)
                if not matches: continue
                lumi = int(matches.group(1))
                if lumi >= self.startLumi:
                    self.allLumis.add(int(matches.group(1)))

            for filename in fullPaths:
                matches = re.search('run%d_ls([0-9]+)_.*[.]dat[.]deleted$' % self.run, filename)
                if not matches: continue
                try:
                    self.allLumis.remove(int(matches.group(1)))
                except:
                    pass


    def copy(self, lumi):
        for node, directory, stream, suffix in self.sources:
            jsnFile = 'run%d_ls%04d_stream%s_%s.jsn' % (self.run, lumi, stream, suffix)
            if os.path.exists(self.targetDir + '/' + jsnFile): return True
            
            proc = subprocess.Popen(['scp', '%s:%s/run%d/' % (node, directory, self.run) + jsnFile.replace('jsn', '*'), self.targetDir], stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
            try:
                proc.communicate()
    
            except KeyboardInterrupt:
                proc.terminate()
                proc.communicate()
                raise

            if proc.returncode == 0:
                self.logFile.write('Copied ' + jsnFile + ' from ' + node)
            else:
                self.logFile.write('Failed to copy ' + jsnFile + ' from ' + node)


    def start(self):
        copied = set([])

        try:
            os.unlink('{rundir}/run{run}/run{run}_ls0000_EoR.jsn'.format(rundir = self.targetDir, run = self.run))
        except:
            pass

        while True:
            lumis = sorted(list(self.allLumis - copied))

            self.logFile.write('Lumis to copy: ' + str(lumis))

            EOR = (len(lumis) == 0 or lumis[0] == 0)

            for lumi in lumis:
                if lumi < self.startLumi:
                    copied.add(lumi)
                    continue

                self.copy(lumi)
                copied.add(lumi)
    
            if EOR:
                break

            if self._stop.isSet():
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
        commonOptions = 'runNumber={run} runInputDir={inputDir} workflow=/{dataset}/{period}/CentralDAQ'.format(run = run, inputDir = '/tmp/onlineDQM', dataset = workflowBase, period = config.period)

        if ecalIn:
            ecalOptions = 'environment=PrivLive outputPath={outputPath} verbosity={verbosity}'.format(outputPath = config.tmpoutdir, verbosity = VERBOSITY)

            log = open(config.logdir + '/ecal_dqm_sourceclient-privlive_cfg.log', 'a')
            log.write('\n\n\n')
            command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {common} {ecal} {spec}'.format(conf = config.workdir + '/ecalConfigBuilder.py', common = commonOptions, ecal = ecalOptions, spec = 'cfgType=Physics')
            proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
            logFile.write(command)
            procs['Physics'] = (proc, log)
    
            log = open(config.logdir + '/ecalcalib_dqm_sourceclient-privlive_cfg.log', 'a')
            log.write('\n\n\n')
            command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {common} {ecal} {spec}'.format(conf = config.workdir + '/ecalConfigBuilder.py', common = commonOptions, ecal = ecalOptions, spec = 'cfgType=Calibration')
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

#                condDB.setMonRunOutcome(run, 'dqmdone')
#
#                # now write DQM results to DB using CMSSW
#                inputFiles = ''
#                for fileName in os.listdir(config.tmpoutdir):
#                    if '%09d' % run in fileName:
#                        inputFiles += ' inputFiles=' + config.tmpoutdir + '/' + fileName
#
#                command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {inputFiles}'.format(conf = config.workdir + '/writeDB_cfg.py', inputFiles = inputFiles)
#                log = open(config.logdir + '/dbwrite.log', 'a')
#                proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
#
#                while proc.poll() is None:
#                    time.sleep(10)
#
#                log.close()

#    try:
#        logFile.write('MonRun IOV:', condDB.getMonRunIOV(condDB.getRunIOV(run)).values['IOV_ID'])
#        return SUCCESS
#    except:
#        logFile.write('DB writing failed.')
#        return FAILED

    return SUCCESS


WAIT = 0
NEXT = 1

def runLoop(currentRun, startLumi, logFile, ecalCondDB, runParamDB, isLatestRun):

    logFile.write('Checking ECAL presence in run', currentRun)

    ecalIn = runParamDB.getRunParameter(currentRun, 'CMS.LVL0:ECAL').lower() == 'in'
    esIn = runParamDB.getRunParameter(currentRun, 'CMS.LVL0:ES').lower() == 'in'

    if not ecalIn and not esIn:
        logFile.write('ECAL not in the run')
        return NEXT

    daq = runParamDB.getDAQType(currentRun)

    logFile.write('DAQ type: ', daq)

    if daq == 'central':
        copyDaemon = GlobalRunFileCopyDaemon(currentRun, startLumi, '/tmp/onlineDQM', [('fu-c2f13-39-01', '/fff/BU0/ramdisk', 'DQM', 'mrg-c2f13-35-01'), ('fu-c2f13-39-01', '/fff/BU0/ramdisk', 'DQMCalibration', 'mrg-c2f13-35-01')], logFile)
        if len(copyDaemon.allLumis) == 0:
            logFile.write('No files to be copied.')
            if isLatestRun:
                return WAIT
            else:
                return NEXT

        logFile.write('Starting file copy daemon')
        copyThread = threading.Thread(target = GlobalRunFileCopyDaemon.start, args = (copyDaemon,))
        copyThread.start()

    dqmRunKey = runParamDB.getRunParameter(currentRun, 'CMS.LVL0:DQM_RUNKEY_AT_CONFIGURE').lower()

    logFile.write('DQM key: ', dqmRunKey if dqmRunKey else 'Undefined')

    procs = startDQM(currentRun, startLumi, daq, dqmRunKey, ecalIn, esIn, logFile)

    if len(procs) == 0:
        logFile.write('No CMSSW job to execute')
        if daq == 'central':
            copyDaemon.stop()
        return WAIT

    if daq == 'central':
        runDir = '/tmp/onlineDQM/run%d' % currentRun
    else:
        runDir = '/dqmminidaq/run%d' % currentRun

    results = {}

    while len(results) != len(procs):
        if isLatestRun:
            if currentRun < runParamDB.getLatestEcalRun():
                open(runDir + '/run%d_ls0000_EoR.jsn' % currentRun, 'w').close()
        else:
            if daq == 'central' and not copyThread.isAlive():
                open(runDir + '/run%d_ls0000_EoR.jsn' % currentRun, 'w').close()

        for pname, (proc, log) in procs.items():
            status = checkProcess(proc)
            if status != RUNNING:
                results[pname] = status
                log.close()

    if daq == 'central':
        copyDaemon.stop()
        copyThread.join()
    
    if FAILED not in results.values():
        logFile.write('CMSSW job successfully returned.')

        result = writeDB(currentRun, ecalCondDB, runParamDB, logFile)

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

    return NEXT


if __name__ == '__main__':

    import sys
    # close stdio so this can run as daemon
    sys.stdout.close()
    sys.stderr.close()
    sys.stdin.close()
    from optparse import OptionParser

    parser = OptionParser(usage = 'Usage: onlineDQM.py [-r [-w]] startRun [startLumi]')

    parser.add_option('-r', '--reprocess', dest = 'reprocess', action = 'store_true', help = 'Reprocess a run instead of running as daemon.')
    parser.add_option('-w', '--write-database', dest = 'writeDatabase', action = 'store_true', help = 'Write to database in reprocess. DB writing is automatic for online DQM.')

    (options, args) = parser.parse_args()

    try:
        runParamDB = RunParameterDB(config.dbread.dbName, config.dbread.dbUserName, config.dbread.dbPassword)
        ecalCondDB = EcalCondDB(config.dbwrite.dbName, config.dbwrite.dbUserName, config.dbwrite.dbPassword)

        latest = runParamDB.getLatestEcalRun()
    
        try:
            currentRun = int(args[0])
            if currentRun > latest: currentRun = latest
        except:
            currentRun = latest

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

        while True:
            logFile.write('')
            logFile.write('*** Run ' + str(currentRun) + ' ***')

            action = runLoop(currentRun, startLumi, logFile, ecalCondDB, runParamDB, isLatestRun)

            if options.reprocess:
                break

            if action == WAIT:
                time.sleep(60)
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
