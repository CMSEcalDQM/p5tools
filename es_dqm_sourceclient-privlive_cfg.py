import sys

import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing

from ecaldqmconfig import config

onlineSourceArgs = []
for arg in sys.argv[2:]:
    if 'runNumber=' in arg or 'runInputDir' in arg or 'skipFirstLumi' in arg or 'runtype' in arg or 'runkey' in arg:
        onlineSourceArgs.append(arg)

for arg in onlineSourceArgs:
    sys.argv.remove(arg)

options = VarParsing("analysis")
options._tags.pop('numEvent%d')
options._tagOrder.remove('numEvent%d')

options.register("workflow", default = "", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "offline workflow")
options.register("prescaleFactor", default = "10", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.int, info = "prescale factor")
options.register("scanOnce", default = False, mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.bool, info = "Don't repeat file scans: use what was found during the initial scan. EOR file is ignored and the state is set to 'past end of run")

options.parseArguments()

sys.argv = sys.argv[:2]
sys.argv += onlineSourceArgs

process = cms.Process("ESDQM")

#process.load('Configuration.StandardSequences.Services_cff')
process.load("FWCore.Modules.preScaler_cfi")

process.MessageLogger = cms.Service("MessageLogger",
    destinations = cms.untracked.vstring('cerr'),
    categories = cms.untracked.vstring('EcalDQM', 'fileAction'),
    cerr = cms.untracked.PSet(
        threshold = cms.untracked.string("WARNING"),
        noLineBreaks = cms.untracked.bool(True),
        noTimeStamps = cms.untracked.bool(True),
        default = cms.untracked.PSet(
            limit = cms.untracked.int32(-1)
        ),
        fileAction = cms.untracked.PSet(
            limit = cms.untracked.int32(10)
        )
    )
)

# for live online DQM in P5
process.load("DQM.Integration.config.inputsource_cfi")
process.source.endOfRunKills = False

# for testing in lxplus
#process.load("DQM.Integration.config.fileinputsource_cfi")

# Condition for P5 cluster
process.load("DQM.Integration.config.FrontierCondition_GT_cfi")
# Condition for lxplus
#process.load("DQM.Integration.config.FrontierCondition_GT_Offline_cfi") 

process.load("EventFilter.ESRawToDigi.esRawToDigi_cfi")
#process.ecalPreshowerDigis = EventFilter.ESRawToDigi.esRawToDigi_cfi.esRawToDigi.clone()
process.esRawToDigi.sourceTag = 'source'
process.esRawToDigi.debugMode = False

process.load('RecoLocalCalo.EcalRecProducers.ecalPreshowerRecHit_cfi')
process.ecalPreshowerRecHit.ESGain = cms.int32(2)
process.ecalPreshowerRecHit.ESBaseline = cms.int32(0)
process.ecalPreshowerRecHit.ESMIPADC = cms.double(50)
process.ecalPreshowerRecHit.ESdigiCollection = cms.InputTag("esRawToDigi")
process.ecalPreshowerRecHit.ESRecoAlgo = cms.int32(0)

process.preScaler.prescaleFactor = options.prescaleFactor

#process.dqmInfoES = cms.EDAnalyzer("DQMEventInfo",
#                                   subSystemFolder = cms.untracked.string('EcalPreshower')
#                                   )

#process.load("DQMServices.Core.DQM_cfg")
#process.load("DQMServices.Components.DQMEnvironment_cfi")

process.load("DQM.Integration.config.environment_cfi")
process.dqmEnv.subSystemFolder = 'EcalPreshower'
process.DQMStore.referenceFileName = '/dqmdata/dqm/reference/es_reference.root'
# for local test

##### EDIT mandrews Sep 04 2015
process.load("DQMServices.Components.DQMFileSaver_cfi")

##### EDIT yiiyama Aug 08 2014
process.dqmSaver.dirName = config.tmpoutdir
process.dqmSaver.workflow = options.workflow
process.dqmSaver.referenceHandling = "skip"
process.dqmSaver.version = 3
process.dqmSaver.convention = "Offline"
process.dqmSaver.producer = "DQM"

process.DQM.collectorPort = 9190
process.DQM.collectorHost = "fu-c2f11-21-02"
##### EDIT yiiyama Aug 08 2014

process.load("DQM.EcalPreshowerMonitorModule.EcalPreshowerMonitorTasks_cfi")
process.ecalPreshowerIntegrityTask.ESDCCCollections = cms.InputTag("esRawToDigi")
process.ecalPreshowerIntegrityTask.ESKChipCollections = cms.InputTag("esRawToDigi")
process.ecalPreshowerIntegrityTask.ESDCCCollections = cms.InputTag("esRawToDigi")
process.ecalPreshowerIntegrityTask.ESKChipCollections = cms.InputTag("esRawToDigi")
process.ecalPreshowerOccupancyTask.DigiLabel = cms.InputTag("esRawToDigi")
process.ecalPreshowerPedestalTask.DigiLabel = cms.InputTag("esRawToDigi")
process.ecalPreshowerRawDataTask.ESDCCCollections = cms.InputTag("esRawToDigi")
process.ecalPreshowerTimingTask.DigiLabel = cms.InputTag("esRawToDigi")
process.ecalPreshowerTrendTask.ESDCCCollections = cms.InputTag("esRawToDigi")

process.load("DQM.EcalPreshowerMonitorClient.EcalPreshowerMonitorClient_cfi")
del process.dqmInfoES
process.p = cms.Path(process.preScaler*
               process.esRawToDigi*
               process.ecalPreshowerRecHit*
               process.ecalPreshowerDefaultTasksSequence*
               process.dqmEnv*
               process.ecalPreshowerMonitorClient*
               process.dqmSaver)


process.esRawToDigi.sourceTag = cms.InputTag("rawDataCollector")
process.ecalPreshowerRawDataTask.FEDRawDataCollection = cms.InputTag("rawDataCollector")
#--------------------------------------------------
# Heavy Ion Specific Fed Raw Data Collection Label
#--------------------------------------------------

print "Running with run type = ", process.runType.getRunType()

if (process.runType.getRunType() == process.runType.hi_run):
    process.esRawToDigi.sourceTag = cms.InputTag("rawDataRepacker")
    process.ecalPreshowerRawDataTask.FEDRawDataCollection = cms.InputTag("rawDataRepacker")

# HACK Aug 9 yiiyama
#process.source.minEventsPerLumi = 1000
process.source.minEventsPerLumi = cms.untracked.int32(-1)

process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(-1)
)

process.source.nextLumiTimeoutMillis = 5000
process.source.delayMillis = 2000

if options.scanOnce:
    process.source.endOfRunKills = False
    process.source.nextLumiTimeoutMillis = 0
