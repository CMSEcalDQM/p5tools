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

class GlobalRunFileCopyDaemon(object):
    # TEMPORARY SOLUTION (SEE BELOW)
    def __init__(self, run, startLumi, runInputDir, sources, logFile):
        self.run = run
        self.startLumi = startLumi
        self.targetDir = runInputDir + '/run%s' % run
        self.sources = sources # [(node, dir, stream, suffix), ..]
        self._stop = threading.Event()
        self.files = []
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
        for node, directory, stream, suffix in self.sources:
            proc = subprocess.Popen('ssh %s "ls %s/run%d"' % (node, directory, self.run), shell = True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
            data = ''
            while proc.poll() is None:
                data += proc.communicate()[0]
                time.sleep(2)
    
            try:
                data += proc.communicate()[0]
            except:
                pass
    
            if proc.poll() != 0:
                return

        self.files = data.split()
        self.logFile.write('New files: ' + str(self.files))

    def copy(self, lumi):
        for node, directory, stream, suffix in self.sources:
            jsnFile = 'run%d_ls%04d_stream%s_%s.jsn' % (self.run, lumi, stream, suffix)
            if os.path.exists(self.targetDir + '/' + jsnFile): return True
            
            proc = subprocess.Popen(['scp', '%s:%s/run%d/' % (node, directory, self.run) + jsnFile.replace('jsn', '*'), self.targetDir], stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
            try:
                while proc.poll() is None:
                    proc.communicate()
                    time.sleep(2)
    
            except KeyboardInterrupt:
                proc.terminate()
                while proc.poll() is None:
                    proc.communicate()
                    time.sleep(1)
                raise

            try:
                proc.communicate()
            except:
                pass

            self.logFile.write('Copied ' + jsnFile + ' from ' + node)

        return proc.returncode == 0

    def start(self):
        copied = set([])

        try:
            os.unlink('{rundir}/run{run}/run{run}_ls0000_EoR.jsn'.format(rundir = self.targetDir, run = self.run))
        except:
            pass

        while True:
            jsonFiles = filter(lambda name: name.endswith('.jsn'), self.files)
    
            lumis = map(lambda name: int(re.match('run%d_ls([0-9]+)' % self.run, name).group(1)), jsonFiles)
            lumis = sorted(list(set(lumis) - copied))

            if len(lumis):
    
                if lumis[0] == 0:
                    EOR = True
                else:
                    EOR = False
    
                for lumi in lumis:
                    if lumi < self.startLumi:
                        copied.add(lumi)
                        continue

                    if self.copy(lumi):
                        copied.add(lumi)
    
                if EOR:
                    for node, directory, stream, suffix in self.sources:
                        proc = subprocess.Popen(['scp', '{node}:{rundir}/run{run}/run{run}_ls0000_EoR.jsn'.format(node = node, rundir = directory, run = self.run), self.targetDir], stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
                        while proc.poll() is None:
                            proc.communicate()
                            time.sleep(2)
    
                        if proc.returncode == 0: break
    
                    break
    
                if self._stop.isSet():
                    break

            time.sleep(60)

            self.updateList()

    def stop(self):
        self._stop.set()


INVALID = 0
SUCCESS = 1
FAILED = 2

def runDQM(run, startLumi, paramDB, logFile):
    """
    Collect the run parameters and start the DQM job if a run is detected.
    """

    logFile.write('Checking ECAL presence in run', run)

    ecalIn = paramDB.getRunParameter(run, 'CMS.LVL0:ECAL').lower() == 'in'
    esIn = paramDB.getRunParameter(run, 'CMS.LVL0:ES').lower() == 'in'

    if not ecalIn and not esIn:
        return INVALID

    logFile.write('Processing run', run)

    daq = paramDB.getDAQType(run)

    if not daq:
        return INVALID

    logFile.write('DAQ type: ', daq)

    dqmRunKey = paramDB.getRunParameter(run, 'CMS.LVL0:DQM_RUNKEY_AT_CONFIGURE').lower()

    logFile.write('DQM key: ', dqmRunKey if dqmRunKey else 'Undefined')

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
        copyDaemon = GlobalRunFileCopyDaemon(run, startLumi, '/tmp/onlineDQM', [('fu-c2f13-39-01', '/fff/BU0/ramdisk', 'DQM', 'mrg-c2f13-35-01'), ('bu-c2f13-27-01', '/store/lustre/mergeMacro', 'Calibration', 'StorageManager')], logFile)
        if len(copyDaemon.files) == 0:
            logFile.write('Source directory empty')
            return INVALID

        commonOptions = 'runNumber={run} runInputDir={inputDir} workflow=/{dataset}/{period}/CentralDAQ'.format(run = run, inputDir = '/tmp/onlineDQM', dataset = workflowBase, period = config.period)

        if ecalIn:
            ecalOptions = 'environment=PrivLive outputPath={outputPath} verbosity=1'.format(outputPath = config.tmpoutdir)

            log = open(config.logdir + '/ecal_dqm_sourceclient-privlive_cfg.log', 'a')
            log.write('\n\n\n')
            command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {common} {ecal} {spec}'.format(conf = config.workdir + '/ecalConfigBuilder.py', common = commonOptions, ecal = ecalOptions, spec = 'cfgType=Physics')
            proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
            logFile.write(command)
            procs['Physics'] = (proc, log, int(time.time()))
    
            log = open(config.logdir + '/ecalcalib_dqm_sourceclient-privlive_cfg.log', 'a')
            log.write('\n\n\n')
            command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {common} {ecal} {spec}'.format(conf = config.workdir + '/ecalConfigBuilder.py', common = commonOptions, ecal = ecalOptions, spec = 'cfgType=Calibration')
            proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
            logFile.write(command)
            procs['Calibration'] = (proc, log, int(time.time()))

        if esIn:
            log = open(config.logdir + '/es_dqm_sourceclient-privlive_cfg.log', 'a')
            log.write('\n\n\n')
            command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {common}'.format(conf = config.workdir + '/es_dqm_sourceclient-privlive_cfg.py', common = commonOptions)
            proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
            logFile.write(command)
            procs['ES'] = (proc, log, int(time.time()))

        logFile.write('Starting file copy daemon')
        copyThread = threading.Thread(target = GlobalRunFileCopyDaemon.start, args = (copyDaemon,))
        copyThread.start()

    elif daq == 'minidaq':
        if not os.path.isdir('/dqmminidaq/run%d' % run):
            logFile.write('DQM stream was not produced')
            return INVALID

        commonOptions = 'runNumber={run} runInputDir={inputDir} workflow=/{dataset}/{period}/MiniDAQ'.format(run = run, inputDir = '/dqmminidaq', dataset = workflowBase, period = config.period)

        if ecalIn:
            ecalOptions = 'environment=PrivLive outputPath={outputPath} verbosity=1'.format(outputPath = config.tmpoutdir)
            
            log = open(config.logdir + '/ecalcalib_dqm_sourceclient-privlive_cfg.log', 'a')
            log.write('\n\n\n')
            command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {common} {ecal} {spec}'.format(conf = config.workdir + '/ecalConfigBuilder.py', common = commonOptions, ecal = ecalOptions, spec = 'cfgType=CalibrationStandalone')
            proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
            logFile.write(command)
            procs['Calibration'] = (proc, log, int(time.time()))

        if esIn:
            log = open(config.logdir + '/es_dqm_sourceclient-privlive_cfg.log', 'a')
            log.write('\n\n\n')
            command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {common}'.format(conf = config.workdir + '/es_dqm_sourceclient-privlive_cfg.py', common = commonOptions)
            proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
            logFile.write(command)
            procs['ES'] = (proc, log, int(time.time()))

    logFile.write('Running configurations:', sorted(procs.keys()))

    while len(filter(lambda x : x[0].poll() is None, procs.values())) != 0:
        time.sleep(10)
        if daq == 'minidaq' and not os.path.exists('/dqmminidaq/run{run}/run{run}_ls0000_EoR.jsn'):
            stopTimeStr = paramDB.getRunParameter(run, 'CMS.LVL0:STOP_TIME_T')
            if not stopTimeStr:
                continue

            stopTime = time.mktime(time.strptime(stopTimeStr.replace(' UTC', ''), '%m/%d/%y %I:%M:%S %p')) + time.timezone
            if stopTime < time.time():
                open('/dqmminidaq/run{run}/run{run}_ls0000_EoR.jsn'.format(run = run), 'w').close()

    for proc, log, start in procs.values():
        log.close()

    if daq == 'central':
        copyThread.join()
        pass

    success = len(filter(lambda x : x[0].returncode != 0, procs.values())) == 0

    if success:
        return SUCCESS
    else:
        return FAILED


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


if __name__ == '__main__':

    import sys
    from optparse import OptionParser

    parser = OptionParser(usage = 'Usage: onlineDQM.py [-r [-w]] startRun')

    parser.add_option('-r', '--reprocess', dest = 'reprocess', action = 'store_true', help = 'Reprocess a run instead of running as daemon.')
    parser.add_option('-w', '--write-database', dest = 'writeDatabase', action = 'store_true', help = 'Write to database in reprocess. DB writing is automatic for online DQM.')

    (options, args) = parser.parse_args()

    runParamDB = RunParameterDB(config.dbread.dbName, config.dbread.dbUserName, config.dbread.dbPassword)

    try:
        currentRun = int(args[0])
    except:
        currentRun = runParamDB.getLatestRun()

    try:
        startLumi = int(args[1])
    except:
        startLumi = 0

    if options.reprocess:
        try:
            os.rename(config.logdir + '/reprocess.log', config.logdir + '/old/reprocess.log')
        except OSError:
            pass

        logFile = Logger(config.logdir + '/reprocess.log')
        logFile.write('ECAL Online DQM')

        result = runDQM(currentRun, startLumi, runParamDB, logFile)

        if result == SUCCESS:
            logFile.write('CMSSW job successfully returned.')

            if options.writeDatabase:
                ecalCondDB = EcalCondDB(config.dbwrite.dbName, config.dbwrite.dbUserName, config.dbwrite.dbPassword)
                result = writeDB(currentRun, ecalCondDB, runParamDB, logFile)

        logFile.close()
            
    else:
        try:
            os.rename(config.logdir + '/onlineDQM.log', config.logdir + '/old/onlineDQM.log')
        except OSError:
            pass
    
        logFile = Logger(config.logdir + '/onlineDQM.log')
        logFile.write('ECAL Online DQM')
    
        ecalCondDB = EcalCondDB(config.dbwrite.dbName, config.dbwrite.dbUserName, config.dbwrite.dbPassword)
    
        while True:
            logFile.write('Monitoring for a new run')
    
            try:
                result = runDQM(currentRun, startLumi, runParamDB, logFile)
                
                if result == SUCCESS:
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
    
                elif result == FAILED:
                    logFile.write('CMSSW job failed.')
    #                ecalCondDB.setMonRunOutcome(currentRun, 'dqmfail')

                if currentRun == runParamDB.getLatestRun():
                    time.sleep(60)
                    continue

                currentRun += 1
    
            except KeyboardInterrupt:
                logFile.write('Quit')
                break
    
            except:
                logFile.write('Exception caught:')
                traceback.print_exc(None, logFile._file)
                break

            startLumi = 0
    
        logFile.close()
    
        ecalCondDB.close()

    runParamDB.close()


