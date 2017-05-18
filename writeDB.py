import os
import sys
import time
import subprocess

from ecaldqmconfig import config
from conddb import EcalCondDB, RunParameterDB

# Initialize exit codes
RUNNING = 0
SUCCESS = 1
FAILED = 2

## MAIN ##
def main():

    ecalCondDB = EcalCondDB(config.dbwrite.dbName, config.dbwrite.dbUserName, config.dbwrite.dbPassword)
    runParamDB = RunParameterDB(config.dbread.dbName, config.dbread.dbUserName, config.dbread.dbPassword)
    print 'Initializing DB writing... '

    thisRun = int(sys.argv[1])
    daq = runParamDB.getDAQType(thisRun)
    result = ''
    # if daq == 'minidaq':
    #     print '  >> miniDAQ run:',thisRun
    #     ecalIn = runParamDB.getRunParameter(thisRun, 'CMS.LVL0:ECAL').lower() == 'in'
    #     if ecalIn:
    #         result = writeDB(thisRun, ecalCondDB, runParamDB)
    #         print '  >> Outcome of writeDB:',result
    #     else:
    #         print '  >> ECAL not in run. Exiting.'
    # else:
    #     print " >> Not a miniDAQ run. Exiting..."
    # print '  >> miniDAQ run:',thisRun
    # ecalIn = runParamDB.getRunParameter(thisRun, 'CMS.LVL0:ECAL').lower() == 'in'
    # if ecalIn:
    result = writeDB(thisRun, ecalCondDB, runParamDB)
    print '  >> Outcome of writeDB:',result
    # else:
    #     print '  >> ECAL not in run. Exiting.'
    runParamDB.close()
    ecalCondDB.close()
    '''
    if result == SUCCESS:
        print 'Transferring ROOT file for upload to GUI...'
        runname = 'R%09d'%thisRun 
        for filename in os.listdir(config.tmpoutdir):
            if runname in filename:
                source_file=config.tmpoutdir+'/'+filename
                destination_file=config.tmpoutdir+'/closed/'+filename
                try:
                    print 'cp',source_file,destination_file
                    os.rename(source_file,destination_file)
                except OSError:
                    pass

    '''

#_________ FCN: Write to DB ________#
def writeDB(run, condDB, runParamDB):

    daq = runParamDB.getDAQType(run)
    if daq == 'central':
        generalTag = 'PHYSICS'
    elif daq == 'minidaq':
        generalTag = 'GLOBAL'
    elif daq=="":
        print "Unable to get daq type for run; setting generalTag to GLOBAL"
        generalTag = 'GLOBAL'
    else:
        return FAILED

    runType = ' runType='
    type = runParamDB.getRunParameter(run, 'CMS.ECAL:LOCAL_CONFIGURATION_KEY').lower()
    print "runType = {type}".format(**locals())
    if 'pedestal' in type:
        runType += 'PEDESTAL'
    elif 'cosmic' in type:
        runType += 'COSMIC'
    elif 'physics' in type:
        runType += 'PHYSICS'
    elif 'testpulse' in type:
        runType += 'TEST_PULSE'
    else:
        print '  >> runType undefined: Use PEDESTAL'
        runType += 'PEDESTAL'

    print '  >> Start DB writing...'
    # Write DQM results to ECAL DB
    inputFiles = ''
    MGPAgain=''
    MGPAgain += ' MGPAGains='+ str(1) + " MGPAGains="+ str(6) + " MGPAGains=" + str(12)

    #for fileName in os.listdir(config.tmpoutdir):
    for fileName in os.listdir(config.tmpoutdir+'/../root'):
        if 'R%09d'%run in fileName:
            #inputFiles += ' inputFiles=' + config.tmpoutdir + '/' + fileName
            inputFiles += ' inputFiles=' + config.tmpoutdir + '/../root/' + fileName

            command = 'source $HOME/DQM/cmssw.sh; exec cmsRun {conf} {inputFiles} {MGPAgain} {runType}'.format(conf = config.workdir + '/writeDB_cfg.py', inputFiles = inputFiles, MGPAgain=MGPAgain, runType=runType)
            #logFile.write(command)
            print command
            log = open(config.logdir + '/dbwrite.log', 'a')
            proc = subprocess.Popen(command, shell = True, stdout = log, stderr = subprocess.STDOUT)
            #logFile.write(command)

            while proc.poll() is None:
                time.sleep(10)
            log.close()
            print '  >> Done writing. Log file at:',log.name

    try:
        print '  >> Uploaded to DB... MonRun IOV:',condDB.getMonRunIOV( condDB.getRunIOV(run) ).values['IOV_ID']
        return SUCCESS
    except:
        print '  >> DB query failed: IOV not found in DB!'
        return FAILED

#_____ Call main() ______#
if __name__ == '__main__':
    main()
