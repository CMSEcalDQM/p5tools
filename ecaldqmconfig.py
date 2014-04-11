class ConfigNode:
    pass

config = ConfigNode()

config.workdir = '/nfshome0/ecalpro/DQM/p5tools'

config.wbm = ConfigNode()
config.wbm.host = 'cmswbm2.cms'
config.wbm.app = '/cmsdb/servlet/RunParameters'

config.period = 'Run2014'

config.logdir = '/data/ecalod-disk01/dqm-data/logs'
config.tmpoutdir = '/data/ecalod-disk01/dqm-data/tmp'

config.scram_arch = 'slc5_amd64_gcc462'
config.cmssw_base = '/data/ecalod-disk01/dqm-data/CMSSW_5_2_4_patch4'

config.dbwrite = ConfigNode()
conf = {}
confFile = open('/nfshome0/ecalpro/DQM/.ecal_db_read.conf', 'r')
for line in confFile:
    # lines are in format
    # key = value
    conf[line.strip().split()[0]] = line.strip().split()[2]
confFile.close()

config.dbwrite.name = conf['dbName']
config.dbwrite.user = conf['dbUserName']
config.dbwrite.password = conf['dbPassword']

config.dbread = ConfigNode()
conf = {}
confFile = open('/nfshome0/ecalpro/DQM/.ecal_db_read.conf', 'r')
for line in confFile:
    conf[line.strip().split()[0]] = line.strip().split()[2]
confFile.close()

config.dbread.name = conf['dbName']
config.dbread.user = conf['dbUserName']
config.dbread.password = conf['dbPassword']
