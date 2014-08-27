import os
import time
import subprocess
import threading
import re
import traceback

from ecaldqmconfig import config
from conddb import EcalCondDB
from wbm import WBM
from logger import Logger

class GlobalRunFileCopyDaemon(object):
    # TEMPORARY SOLUTION (SEE BELOW)
    def __init__(self, run, sourceNode, sourceDir, runInputDir):
        self.run = run
        self.targetDir = runInputDir + '/run%s' % run
        self.sourceNode = sourceNode
        self.sourceDir = sourceDir
        self._stop = threading.Event()
        self.files = []

        try:
            os.mkdir(self.targetDir)
        except OSError:
            pass

        proc = subprocess.Popen(['ssh', self.sourceNode, '"ls %s/run%d"' % (self.sourceDir, self.run)], stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
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

    def __del__(self):
        self._stop.set()

    def copy(self, startLumi):
        jsonFiles = filter(lambda name: name.endswith('.jsn'), self.files)

        lumis = map(lambda name: int(re.match('run%d_ls([0-9]+)' % self.run, name).group(1)), jsonFiles)
        lumis = sorted(list(set(lumis)))

        for lumi in lumis:
            if lumi < startLumi: continue

            proc = subprocess.Popen(['scp', '%s:%s/run%d/run%d_ls%04d_*' % (self.sourceNode, self.sourceDir, self.run, self.run, lumi), self.targetDir])
            try:
                while proc.poll() is None:
                    time.sleep(2)

            except KeyboardInterrupt:
                proc.terminate()
                while proc.poll() is None:
                    time.sleep(1)
                    
                raise

        if lumis[0] == 0:
            #EOR
            return 0

        return lumis[-1]

    def start(self):
        lumi = 1
        while True:
            lumi = self.copy(lumi)
            if lumi == 0:
                break

            if self._stop.isSet():
                break

            break

            time.sleep(25)

    def stop(self):
        self._stop.set()


def onlineDQM(logFile_, run_, wbm_):
    """
    Monitor the online streams for ECAL data, and start the DQM job if a run is detected.
    """

    logFile_.write('Processing run', run_)

    wbmdata = wbm_.getRunParameters(run_)

    ecalIn = wbmdata['CMS.LVL0:ECAL'].lower() == 'in'
    esIn = wbmdata['CMS.LVL0:ES'].lower() == 'in'

    try:
        dqmRunKey = wbmdata['CMS.LVL0:DQM_RUN_KEY_AT_CONFIGURE'].lower()
    except KeyError:
        dqmRunKey = ''

    if dqmRunKey == 'cosmic_run':
        workflowBase = 'Cosmics'
    elif dqmRunKey == 'pp_run':
        workflowBase = 'Protons'
    elif dqmRunKey == 'hi_run':
        workflowBase = 'HeavyIons'
    else:
        workflowBase = 'All'

    matches = re.match('/global_configuration_map/cms/(central|minidaq)/', wbmdata['CMS.LVL0:GLOBAL_CONF_KEY'].lower())

    logFile_.write('Found entries in WBM.\n CONFKEY: ' + wbmdata['CMS.LVL0:GLOBAL_CONF_KEY'])

    if not matches:
        return ''

    daq = matches.group(1)

    logFile_.write('DAQ type: ', daq)

    procs = {}
    if daq == 'central':
        copyDaemon = GlobalRunFileCopyDaemon(run_, 'fu-c2f13-39-01', '/fff/BU0/ramdisk', '/tmp/onlineDQM')
        if len(copyDaemon.files) == 0:
            logFile_.write('Source directory empty')
            return ''

        commonOptions = 'runNumber=%d runInputDir=%s' % (run_, '/tmp/onlineDQM')

        config.var.workflow = '/{dataset}/{period}/CentralDAQ'.format(dataset = workflowBase, period = config.period)

        if ecalIn:
            ecalOptions = 'environment=PrivLive workflow={workflow} outputPath={outputPath}'.format(workflow = config.var.workflow, outputPath = config.tmpoutdir)

            log = open(config.logdir + '/ecal_dqm_sourceclient-privlive_cfg.log', 'a')
            command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {common} {ecal} {spec}'.format(conf = config.workdir + '/ecalConfigBuilder.py', common = commonOptions, ecal = ecalOptions, spec = 'cfgType=Physics')
            proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
            logFile_.write(command)
            procs['Physics'] = (proc, log, int(time.time()))
    
            log = open(config.logdir + '/ecalcalib_dqm_sourceclient-privlive_cfg.log', 'a')
            command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {common} {ecal} {spec}'.format(conf = config.workdir + '/ecalConfigBuilder.py', common = commonOptions, ecal = ecalOptions, spec = 'cfgType=Calibration')
            proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
            logFile_.write(command)
            procs['Calibration'] = (proc, log, int(time.time()))

        if esIn:
            log = open(config.logdir + '/es_dqm_sourceclient-privlive_cfg.log', 'a')
            command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {common}'.format(conf = config.workdir + '/es_dqm_sourceclient-privlive_cfg.py', common = commonOptions)
            proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
            logFile_.write(command)
            procs['ES'] = (proc, log, int(time.time()))

        generalTag = 'PHYSICS'

        logFile_.write('Starting file copy daemon')
        copyThread = threading.Thread(target = GlobalRunFileCopyDaemon.start, args = (copyDaemon,))
        copyThread.start()

    elif daq == 'minidaq':
        if not os.path.isdir('/dqmminidaq/run%d' % run_):
            logFile_.write('DQM stream was not produced')
            return ''

        config.var.workflow = '/{dataset}/{period}/CentralDAQ'.format(dataset = workflowBase, period = config.period)

        commonOptions = 'runNumber=%d runInputDir=%s' % (run_, '/dqmminidaq')

        if ecalIn:
            ecalOptions = 'environment=PrivLive workflow={workflow} outputPath={outputPath}'.format(workflow = config.var.workflow, outputPath = config.tmpoutdir)
            
            log = open(config.logdir + '/ecalcalib_dqm_sourceclient-privlive_cfg.log', 'a')
            command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {common} {ecal} {spec}'.format(conf = config.workdir + '/ecalConfigBuilder.py', common = commonOptions, ecal = ecalOptions, spec = 'cfgType=CalibrationStandalone')
            proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
            logFile_.write(command)
            procs['Calibration'] = (proc, log, int(time.time()))

        if esIn:
            log = open(config.logdir + '/es_dqm_sourceclient-privlive_cfg.log', 'a')
            command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {common}'.format(conf = config.workdir + '/es_dqm_sourceclient-privlive_cfg.py', common = commonOptions)
            proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
            logFile_.write(command)
            procs['ES'] = (proc, log, int(time.time()))

        generalTag = 'GLOBAL'

    logFile_.write('Running configurations:', sorted(procs.keys()))

    while len(filter(lambda x : x[0].poll() is None, procs.values())) != 0:
        if daq == 'minidaq':
            wbmdata = wbm_.getRunParameters(run_)
            if 'CMS.LVL0:STOP_TIME_T' in wbmdata:
                stopTime = time.mktime(time.strptime(wbmdata['CMS.LVL0:STOP_TIME_T'].replace(' UTC', ''), '%m/%d/%y %I:%M:%S %p')) + time.timezone
                if stopTime < time.time():
                    open('/dqmminidaq/run{run}/run{run}_ls0000_EoR.jsn'.format(run = run_), 'w').close()

        time.sleep(10)

    for proc, log, start in procs.values():
        log.close()

    if daq == 'central':
        copyThread.join()
        pass

    success = len(filter(lambda x : x[0].returncode != 0, procs.values())) == 0

    if success:
        return generalTag
    else:
        return ''


if __name__ == '__main__':

    import sys
    from optparse import OptionParser

    parser = OptionParser(usage = 'Usage: onlineDQM.py [options] startRun')

    (options, args) = parser.parse_args()

    try:
        firstRun = int(args[0])
    except:
        parser.print_usage()
        sys.exit(1)

    try:
        os.rename(config.logdir + '/onlineDQM.log', config.logdir + '/onlineDQM.log.old')
    except OSError:
        pass

    logFile = Logger(config.logdir + '/onlineDQM.log')
    logFile.write('ECAL Online DQM')

    db = EcalCondDB(config.dbwrite.dbName, config.dbwrite.dbUserName, config.dbwrite.dbPassword)
    wbm = WBM()

    currentRun = firstRun

    while True:
        try:
            newRun = 0
            while newRun == 0:
                newRun, ecalIn, esIn = wbm.findNewRun(currentRun)
                time.sleep(20)

            currentRun = newRun

            generalTag = onlineDQM(logFile, currentRun, wbm)

            if generalTag:
                logFile.write('CMSSW job successfully returned. Start DB writing.')
                db.setMonRunOutcome(currentRun, 'dqmdone')

                # now write DQM results to DB using CMSSW
                inputFiles = ''
                for fileName in os.listdir(config.tmpoutdir):
                    if '%09d' % int(run) in fileName:
                        inputFiles += ' inputFiles=' + config.tmpoutdir + '/' + fileName

                command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {inputFiles}'.format(conf = config.workdir + '/writeDB_cfg.py', inputFiles = inputFiles)
                log = open(config.logdir + '/dbwrite.log', 'a')
                proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)

                while proc.poll() is None:
                    time.sleep(10)

                log.close()

                # outcome is set to success automatically in the CMSSW job upon completion
            else:
                logFile.write('CMSSW job failed.')
                db.setMonRunOutcome(run, 'dqmfail')

        except KeyboardInterrupt:
            logFile.write('Quit')
            break

        except:
            logFile.write('Exception caught:')
            traceback.print_exc(None, logFile._file)
            break

    logFile.close()

    db.close()
    wbm.close()
