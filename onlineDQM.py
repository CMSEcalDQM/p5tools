import sys
import os
import time
import subprocess
import re
import traceback

if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ecaldqmconfig import config
from htmlnode import HTMLNode, wbmRunParameters
from conddb import EcalCondDB
from logger import Logger

SETENV = 'export SCRAM_ARCH={scram_arch};cd {cmssw_base};eval `scram runtime -sh`;cd {workdir};'.format(scram_arch = config.scram_arch, cmssw_base = config.cmssw_base, workdir = config.workdir)

def onlineDQM(logFile_):
    """
    Monitor the online streams for ECAL data, and start the DQM job if a run is detected.
    """
    
    monitorProcs = {}
    for source in ['central', 'minidaq']:
        monitorProcs[source] = subprocess.Popen(SETENV + 'cmsRun runcheck_cfg.py source={source}'.format(source = source), shell = True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)

    logFile_.write('Waiting for a run.')
    
    while len(filter(lambda p : p.poll() is not None, monitorProcs.values())) == 0:
        time.sleep(10)

    run = '0'
    for source, proc in monitorProcs.items():
        try:
            if proc.poll() is None:
                proc.terminate()
                continue
            
            response = proc.stdout.readline()
            logFile_.write('RunDetector for ' + source + ' says: ' + response)
            matches = re.match('Found ECAL data in run ([0-9]+)', response.strip())
            if not matches:
                matches = re.match('Run ([0-9]+) does not contain ECAL data', response.strip())
            if matches:
                run = matches.group(1)

        except OSError:
            pass

    logFile_.write('Detected run ' + run + '.')

    if run == '0': return (run, '', False)

    wbmdata = wbmRunParameters(run)

    if wbmdata['CMS.LVL0:ECAL'].lower() != 'in': return (run, '', False)

    try:
        dqmRunKey = wbmdata['CMS.LVL0:DQM_RUN_KEY_AT_CONFIGURE'].lower()
    except KeyError:
        dqmRunKey = ''

    if dqmRunKey == 'cosmic_run':
        workflowBase = '/Cosmics'
    elif dqmRunKey == 'pp_run':
        workflowBase = '/Protons'
    elif dqmRunKey == 'hi_run':
        workflowBase = '/HeavyIons'
    else:
        workflowBase = '/All'

    matches = re.match('/global_configuration_map/cms/(central|minidaq)/', wbmdata['CMS.LVL0:GLOBAL_CONF_KEY'].lower())

    logFile_.write('Found entries in WBM.\n  ECAL:' + wbmdata['CMS.LVL0:ECAL'] + '\n  CONFKEY: ' + wbmdata['CMS.LVL0:GLOBAL_CONF_KEY'])

    if not matches: return (run, '', False)

    daq = matches.group(1)

    jobOptions = {}
    commonOptions = 'globalTag=GR_H_V32::All verbosity=1 outputPath=/data/ecalod-disk01/dqm-data/tmp'
    if daq == 'central':
        commonOptions += ' inputFiles=http://dqm-c2d07-30.cms:22100/urn:xdaq-application:lid=30'
        jobOptions['A'] = commonOptions + ' environment=PrivLive cfgType=Physics workflow=' + workflowBase + '/' + config.period + '/CentralDAQ'
        jobOptions['Calibration'] = commonOptions + ' environment=PrivLive cfgType=Calibration workflow=' + workflowBase + '/' + config.period + '/CentralDAQ'
        generalTag = 'PHYSICS'
    elif daq == 'minidaq':
        jobOptions['Calibration'] = commonOptions + ' inputFiles=http://cmsdisk1.cms:15100/urn:xdaq-application:lid=50 environment=PrivLive cfgType=CalibrationStandalone workflow=' + workflowBase + '/' + config.period + '/MiniDAQ rawDataCollection=hltEcalCalibrationRaw'
        generalTag = 'GLOBAL'

    logFile_.write('Start CMSSW jobs.')

    runProcs = []
    for stream, jobOption in jobOptions.items():
        logName = config.logdir + '/online_' + daq + '_' + stream + '.log'
        try:
            os.rename(logName, logName + '.old')
        except OSError:
            pass

        runProcs.append(subprocess.Popen(SETENV + 'cmsRun ecalConfigBuilder.py maxEvents=100 ' + jobOption, shell = True, stdout = open(logName, 'a'), stderr = subprocess.STDOUT))

    while len(filter(lambda p : p.poll() is None, runProcs)) != 0:
        time.sleep(60)

    result = len(filter(lambda p : p.returncode != 0, runProcs)) == 0

    return (run, generalTag, result)


if __name__ == '__main__':

    try:
        os.rename(config.logdir + '/onlineDQM.log', config.logdir + '/onlineDQM.log.old')
    except OSError:
        pass

    db = EcalCondDB(config.dbwrite.name, config.dbwrite.user, config.dbwrite.password)

    logFile = Logger(config.logdir + '/onlineDQM.log')

    currentRun = 220000

    while True:
        try:
            if db.getNewRunNumber(currentRun) == 0:
                time.sleep(60)
                continue

            run, generalTag, success = onlineDQM(logFile)

            if run != '0':
                currentRun = int(run)

            if success:
                logFile.write('CMSSW job successfully returned. Start DB writing.')
                db.setMonRunOutcome(run, 'dqmdone')

                # now write DQM results to DB using CMSSW
                inputFiles = ''
                for fileName in os.listdir(config.tmpoutdir):
                    if '%09d' % int(run) in fileName:
                        inputFiles += ' inputFiles=' + config.tmpoutdir + '/' + fileName

                proc = subprocess.Popen(SETENV + 'cmsRun writeDB_cfg.py' + inputFiles, shell = True, stdout = open(config.logdir + '/dbwrite.log', 'a'), stderr = subprocess.STDOUT)

                while proc.poll() is None:
                    time.sleep(10)

                # outcome is set to success automatically in the CMSSW job upon completion
            elif generalTag:
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
