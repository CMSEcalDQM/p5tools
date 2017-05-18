### AUTO-GENERATED CMSRUN CONFIGURATION FOR ECAL DQM ###
import FWCore.ParameterSet.Config as cms

process = cms.Process("process")

### Load cfis ###

process.load("Configuration.StandardSequences.GeometryRecoDB_cff")
process.load("RecoLocalCalo.EcalRecProducers.ecalGlobalUncalibRecHit_cfi")
process.load("RecoLocalCalo.EcalRecProducers.ecalDetIdToBeRecovered_cfi")
process.load("RecoLocalCalo.EcalRecProducers.ecalRecHit_cfi")
process.load("RecoLocalCalo.EcalRecAlgos.EcalSeverityLevelESProducer_cfi")
process.load("CalibCalorimetry.EcalLaserCorrection.ecalLaserCorrectionService_cfi")
process.load("DQM.EcalMonitorTasks.EcalCalibMonitorTasks_cfi")
process.load("DQM.EcalMonitorTasks.EcalMonitorTask_cfi")
process.load("DQM.EcalMonitorClient.EcalCalibMonitorClient_cfi")
process.load("DQM.Integration.config.environment_cfi")
process.load("DQM.Integration.config.online_customizations_cfi")
process.load("DQMServices.Components.DQMFileSaver_cfi")
process.load("FWCore.Modules.preScaler_cfi")
process.load("DQM.Integration.config.FrontierCondition_GT_cfi")
process.load("DQM.Integration.config.inputsource_cfi")

### Individual module setups ###

process.MessageLogger = cms.Service("MessageLogger",
    categories = cms.untracked.vstring('EcalDQM', 
        'EcalLaserDbService'),
    cerr = cms.untracked.PSet(
        default = cms.untracked.PSet(
            limit = cms.untracked.int32(0)
        ),
        noLineBreaks = cms.untracked.bool(True),
        noTimeStamps = cms.untracked.bool(True),
        threshold = cms.untracked.string('WARNING')
    ),
    cout = cms.untracked.PSet(
        EcalDQM = cms.untracked.PSet(
            limit = cms.untracked.int32(-1)
        ),
        EcalLaserDbService = cms.untracked.PSet(
            limit = cms.untracked.int32(10)
        ),
        default = cms.untracked.PSet(
            limit = cms.untracked.int32(0)
        ),
        threshold = cms.untracked.string('INFO')
    ),
    destinations = cms.untracked.vstring('cerr', 
        'cout')
)

process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(-1)
)

process.ecalLaserLedUncalibRecHit = cms.EDProducer("EcalUncalibRecHitProducer",
    EBdigiCollection = cms.InputTag("ecalDigis","ebDigis"),
    EBhitCollection = cms.string('EcalUncalibRecHitsEB'),
    EEdigiCollection = cms.InputTag("ecalDigis","eeDigis"),
    EEhitCollection = cms.string('EcalUncalibRecHitsEE'),
    algo = cms.string('EcalUncalibRecHitWorkerFixedAlphaBetaFit'),
    algoPSet = cms.PSet(
        MinAmplBarrel = cms.double(12.0),
        MinAmplEndcap = cms.double(16.0)
    )
)

process.ecalLaserLedFilter = cms.EDFilter("EcalMonitorPrescaler",
    EcalRawDataCollection = cms.InputTag("ecalDigis"),
    laser = cms.untracked.uint32(1),
    led = cms.untracked.uint32(1)
)

process.ecalPedestalFilter = cms.EDFilter("EcalMonitorPrescaler",
    EcalRawDataCollection = cms.InputTag("ecalDigis"),
    pedestal = cms.untracked.uint32(1)
)

process.ecalTestPulseFilter = cms.EDFilter("EcalMonitorPrescaler",
    EcalRawDataCollection = cms.InputTag("ecalDigis"),
    testPulse = cms.untracked.uint32(1)
)

process.ecalDigis = cms.EDProducer("EcalRawToDigi",
    DoRegional = cms.bool(False),
    FEDs = cms.vint32(601, 602, 603, 604, 605, 
        606, 607, 608, 609, 610, 
        611, 612, 613, 614, 615, 
        616, 617, 618, 619, 620, 
        621, 622, 623, 624, 625, 
        626, 627, 628, 629, 630, 
        631, 632, 633, 634, 635, 
        636, 637, 638, 639, 640, 
        641, 642, 643, 644, 645, 
        646, 647, 648, 649, 650, 
        651, 652, 653, 654),
    FedLabel = cms.InputTag("listfeds"),
    InputLabel = cms.InputTag("rawDataCollector"),
    eventPut = cms.bool(True),
    feIdCheck = cms.bool(True),
    feUnpacking = cms.bool(True),
    forceToKeepFRData = cms.bool(False),
    headerUnpacking = cms.bool(True),
    memUnpacking = cms.bool(True),
    numbTriggerTSamples = cms.int32(1),
    numbXtalTSamples = cms.int32(10),
    orderedDCCIdList = cms.vint32(1, 2, 3, 4, 5, 
        6, 7, 8, 9, 10, 
        11, 12, 13, 14, 15, 
        16, 17, 18, 19, 20, 
        21, 22, 23, 24, 25, 
        26, 27, 28, 29, 30, 
        31, 32, 33, 34, 35, 
        36, 37, 38, 39, 40, 
        41, 42, 43, 44, 45, 
        46, 47, 48, 49, 50, 
        51, 52, 53, 54),
    orderedFedList = cms.vint32(601, 602, 603, 604, 605, 
        606, 607, 608, 609, 610, 
        611, 612, 613, 614, 615, 
        616, 617, 618, 619, 620, 
        621, 622, 623, 624, 625, 
        626, 627, 628, 629, 630, 
        631, 632, 633, 634, 635, 
        636, 637, 638, 639, 640, 
        641, 642, 643, 644, 645, 
        646, 647, 648, 649, 650, 
        651, 652, 653, 654),
    silentMode = cms.untracked.bool(True),
    srpUnpacking = cms.bool(True),
    syncCheck = cms.bool(True),
    tccUnpacking = cms.bool(True)
)

process.ecalTestPulseUncalibRecHit = cms.EDProducer("EcalUncalibRecHitProducer",
    EBdigiCollection = cms.InputTag("ecalDigis","ebDigis"),
    EBhitCollection = cms.string('EcalUncalibRecHitsEB'),
    EEdigiCollection = cms.InputTag("ecalDigis","eeDigis"),
    EEhitCollection = cms.string('EcalUncalibRecHitsEE'),
    algo = cms.string('EcalUncalibRecHitWorkerMaxSample'),
    algoPSet = cms.PSet(

    )
)

process.ecalCalibMonitorClient.verbosity = 0
process.ecalCalibMonitorClient.workers = ['IntegrityClient', 'RawDataClient', 'PedestalClient', 'TestPulseClient', 'LaserClient', 'LedClient', 'PNIntegrityClient', 'SummaryClient', 'CalibrationSummaryClient', 'PresampleClient']
process.ecalCalibMonitorClient.workerParameters.SummaryClient.params.activeSources = ['Integrity', 'RawData', 'Presample']
process.ecalCalibMonitorClient.commonParameters.onlineMode = True

process.preScaler.prescaleFactor = 1

process.DQMStore.referenceFileName = "/data/dqm-data/online-DQM/ecalcalib_reference.root"

process.ecalPedestalMonitorTask.verbosity = 0
process.ecalPedestalMonitorTask.commonParameters.onlineMode = True

process.dqmSaver.referenceHandling = "skip"
process.dqmSaver.producer = "DQM"
process.dqmSaver.workflow = "/All/Run2016/MiniDAQ"
process.dqmSaver.version = 2
process.dqmSaver.convention = "Offline"
#process.dqmSaver.dirName = "/data/dqm-data_/tmp"
process.dqmSaver.dirName = "/data/dqm-data/tmp"
process.source.minEventsPerLumi = cms.untracked.int32(-1)

threads = 16
#process.options.numberOfThreads = cms.untracked.uint32(threads)
#process.options.numberOfStreams = cms.untracked.uint32(threads)
#process.options.sizeOfStackForThreadsInKB = cms.untracked.uint32(16*1024)

process.ecalMonitorTask.workers = ['IntegrityTask', 'RawDataTask', 'PresampleTask']
process.ecalMonitorTask.collectionTags.Source = "rawDataCollector"

process.ecalLaserLedMonitorTask.verbosity = 0
process.ecalLaserLedMonitorTask.collectionTags.EBLaserLedUncalibRecHit = "ecalLaserLedUncalibRecHit:EcalUncalibRecHitsEB"
process.ecalLaserLedMonitorTask.collectionTags.EELaserLedUncalibRecHit = "ecalLaserLedUncalibRecHit:EcalUncalibRecHitsEE"
process.ecalLaserLedMonitorTask.commonParameters.onlineMode = True

process.DQM.collectorPort = 9190
process.DQM.collectorHost = "fu-c2f11-21-02"

process.GlobalTag.toGet = cms.VPSet(cms.PSet(
    connect = cms.string('frontier://(proxyurl=http://localhost:3128)(serverurl=http://localhost:8000/FrontierOnProd)(serverurl=http://localhost:8000/FrontierOnProd)(retrieve-ziplevel=0)(failovertoserver=no)/CMS_CONDITIONS'),
    record = cms.string('EcalDQMChannelStatusRcd'),
    tag = cms.string('EcalDQMChannelStatus_v1_hlt')
), 
    cms.PSet(
        connect = cms.string('frontier://(proxyurl=http://localhost:3128)(serverurl=http://localhost:8000/FrontierOnProd)(serverurl=http://localhost:8000/FrontierOnProd)(retrieve-ziplevel=0)(failovertoserver=no)/CMS_CONDITIONS'),
        record = cms.string('EcalDQMTowerStatusRcd'),
        tag = cms.string('EcalDQMTowerStatus_v1_hlt')
    ))

process.ecalTestPulseMonitorTask.verbosity = 0
process.ecalTestPulseMonitorTask.commonParameters.onlineMode = True

process.ecalRecHit.EEuncalibRecHitCollection = "ecalGlobalUncalibRecHit:EcalUncalibRecHitsEE"
process.ecalRecHit.EBuncalibRecHitCollection = "ecalGlobalUncalibRecHit:EcalUncalibRecHitsEB"

process.ecalPNDiodeMonitorTask.verbosity = 0
process.ecalPNDiodeMonitorTask.commonParameters.onlineMode = True

process.dqmEnv.subSystemFolder = cms.untracked.string('EcalCalibration')

### Sequences ###

process.ecalRecoSequence = cms.Sequence((process.ecalGlobalUncalibRecHit+process.ecalDetIdToBeRecovered+process.ecalRecHit))
process.ecalPreRecoSequence = cms.Sequence(process.ecalDigis)

### Paths ###

process.ecalLaserLedPath = cms.Path(process.preScaler+process.ecalPreRecoSequence+process.ecalLaserLedFilter+process.ecalRecoSequence+process.ecalLaserLedUncalibRecHit+process.ecalLaserLedMonitorTask+process.ecalPNDiodeMonitorTask)
process.ecalTestPulsePath = cms.Path(process.preScaler+process.ecalPreRecoSequence+process.ecalTestPulseFilter+process.ecalRecoSequence+process.ecalTestPulseUncalibRecHit+process.ecalTestPulseMonitorTask+process.ecalPNDiodeMonitorTask)
process.ecalPedestalPath = cms.Path(process.preScaler+process.ecalPreRecoSequence+process.ecalPedestalFilter+process.ecalRecoSequence+process.ecalPedestalMonitorTask+process.ecalPNDiodeMonitorTask)
process.ecalMonitorPath = cms.Path(process.ecalPreRecoSequence+process.ecalMonitorTask)
process.ecalClientPath = cms.Path(process.ecalCalibMonitorClient)

process.dqmEndPath = cms.EndPath(process.dqmEnv)
process.dqmOutputPath = cms.EndPath(process.dqmSaver)

### Schedule ###

process.schedule = cms.Schedule(process.ecalLaserLedPath,process.ecalTestPulsePath,process.ecalPedestalPath,process.ecalMonitorPath,process.ecalClientPath,process.dqmEndPath,process.dqmOutputPath)
