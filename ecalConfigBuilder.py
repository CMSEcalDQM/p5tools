import sys

def buildEcalDQMProcess(process, options):
    """
    Build a standalone cms.Process that can be used for (quasi-)online DQM and local testing
    """

    ### SET OPTION FLAGS ###
    
    if options.environment not in ['CMSLive', 'PrivLive', 'PrivOffline', 'LocalLive', 'LocalOffline']:
        raise RuntimeError("environment value " + options.environment + " not correct")
    
    if options.cfgType not in ['Physics', 'Calibration', 'CalibrationOnly', 'Laser']:
        raise RuntimeError("cfgType value " + options.cfgType + " not correct")

    if not options.source:
        raise RuntimeError("source name empty")

    if not options.rawDataCollection:
        raise RuntimeError("rawDataCollection name not given")

    central = (options.environment == 'CMSLive')
    privEcal = ('Priv' in options.environment)
    local = ('Local' in options.environment)
    live = ('Live' in options.environment)

    p5 = privEcal or central
           
    physics = (options.cfgType == 'Physics')
    calib = (options.cfgType == 'Calibration' or options.cfgType == 'CalibrationOnly')
    laser = (options.cfgType == 'Laser')

    verbosity = options.verbosity
    if verbosity < 0:
        if local: verbosity = 2
        else: verbosity = 0


    ### RECONSTRUCTION MODULES ###
    
    process.load("Geometry.CaloEventSetup.CaloGeometry_cfi")
    process.load("Geometry.CaloEventSetup.CaloTopology_cfi")
    process.load("Geometry.CaloEventSetup.EcalTrigTowerConstituents_cfi")
    process.load("Geometry.CMSCommonData.cmsIdealGeometryXML_cfi")
    process.load("Geometry.EcalMapping.EcalMapping_cfi")
    process.load("Geometry.EcalMapping.EcalMappingRecord_cfi")
    
    if not laser:
        from EventFilter.EcalRawToDigi.EcalUnpackerData_cfi import ecalEBunpacker
        process.ecalDigis = ecalEBunpacker.clone(
            InputLabel = cms.InputTag(options.rawDataCollection)
        )
    
        process.load("RecoLocalCalo.EcalRecProducers.ecalGlobalUncalibRecHit_cfi")
        process.load("RecoLocalCalo.EcalRecProducers.ecalDetIdToBeRecovered_cfi")
        process.load("RecoLocalCalo.EcalRecProducers.ecalRecHit_cfi")
        process.ecalRecHit.killDeadChannels = True
        process.ecalRecHit.ChannelStatusToBeExcluded = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 78, 142]
        process.load("RecoLocalCalo.EcalRecAlgos.EcalSeverityLevelESProducer_cfi")
        process.load("CalibCalorimetry.EcalLaserCorrection.ecalLaserCorrectionService_cfi")

    if physics:
        process.load("RecoEcal.EgammaClusterProducers.ecalClusteringSequence_cff")
        process.load("SimCalorimetry.EcalTrigPrimProducers.ecalTriggerPrimitiveDigis_cfi")
        process.simEcalTriggerPrimitiveDigis.Label = "ecalDigis"
        process.simEcalTriggerPrimitiveDigis.InstanceEB = "ebDigis"
        process.simEcalTriggerPrimitiveDigis.InstanceEE = "eeDigis"
    elif calib:
        from RecoLocalCalo.EcalRecProducers.ecalFixedAlphaBetaFitUncalibRecHit_cfi import ecalFixedAlphaBetaFitUncalibRecHit
        process.ecalLaserLedUncalibRecHit = ecalFixedAlphaBetaFitUncalibRecHit.clone(
            MinAmplBarrel = 12.,
            MinAmplEndcap = 16.
        )
        from RecoLocalCalo.EcalRecProducers.ecalMaxSampleUncalibRecHit_cfi import ecalMaxSampleUncalibRecHit
        process.ecalTestPulseUncalibRecHit = ecalMaxSampleUncalibRecHit.clone(
            EBdigiCollection = "ecalDigis:ebDigis",
            EEdigiCollection = "ecalDigis:eeDigis"
        )

        
    ### ECAL DQM MODULES ###
    
    process.load("DQM.EcalCommon.EcalDQMBinningService_cfi")

    if physics:
        process.load("DQM.EcalBarrelMonitorTasks.EcalMonitorTask_cfi")
        process.ecalMonitorTask.workers = ["ClusterTask", "EnergyTask", "IntegrityTask", "OccupancyTask", "RawDataTask", "TimingTask", "TrigPrimTask", "PresampleTask", "SelectiveReadoutTask"]
        process.ecalMonitorTask.collectionTags.Source = options.rawDataCollection
        process.ecalMonitorTask.workerParameters.common.hltTaskMode = 0
        process.ecalMonitorTask.workerParameters.TrigPrimTask.runOnEmul = True
        process.load("DQM.EcalBarrelMonitorClient.EcalMonitorClient_cfi")
        process.ecalMonitorClient.workers = ["IntegrityClient", "OccupancyClient", "PresampleClient", "RawDataClient", "TimingClient", "SelectiveReadoutClient", "TrigPrimClient", "SummaryClient"]
        process.ecalMonitorClient.workerParameters.SummaryClient.activeSources = ["Integrity", "RawData", "Presample", "TriggerPrimitives", "Timing", "HotCell"]
        if live:
            process.ecalMonitorTask.online = True
            process.ecalMonitorClient.online = True
            
        if local:
            process.ecalMonitorTask.verbosity = verbosity
            process.ecalMonitorClient.verbosity = verbosity
            
    elif calib:
        process.load("DQM.EcalBarrelMonitorTasks.EcalCalibMonitorTasks_cfi")
        process.ecalLaserLedMonitorTask.workerParameters.common.laserWavelengths = options.laserWavelengths
        process.ecalPedestalMonitorTask.workerParameters.common.MGPAGains = options.MGPAGains
        process.ecalPedestalMonitorTask.workerParameters.common.MGPAGainsPN = options.MGPAGainsPN
        process.ecalTestPulseMonitorTask.workerParameters.common.MGPAGains = options.MGPAGains
        process.ecalTestPulseMonitorTask.workerParameters.common.MGPAGainsPN = options.MGPAGainsPN
        process.ecalPNDiodeMonitorTask.workerParameters.common.MGPAGainsPN = options.MGPAGainsPN
        process.load("DQM.EcalBarrelMonitorClient.EcalCalibMonitorClient_cfi")
        process.ecalCalibMonitorClient.workerParameters.common.laserWavelengths = options.laserWavelengths
        process.ecalCalibMonitorClient.workerParameters.common.MGPAGains = options.MGPAGains
        process.ecalCalibMonitorClient.workerParameters.common.MGPAGainsPN = options.MGPAGainsPN

        if options.cfgType == 'Calibration':
            process.load("DQM.EcalBarrelMonitorTasks.EcalMonitorTask_cfi")
            process.ecalMonitorTask.workers = ["IntegrityTask", "RawDataTask"]
            process.ecalMonitorTask.collectionTags.Source = options.rawDataCollection
            process.ecalCalibMonitorClient.workerParameters.SummaryClient.activeSources = ["Integrity", "RawData"]
            if options.calibType == 'PEDESTAL':
                process.ecalCalibMonitorClient.workers = ["IntegrityClient", "RawDataClient", "PedestalClient", "PNIntegrityClient", "SummaryClient", "CalibrationSummaryClient"]
            elif options.calibType == 'TEST_PULSE':
                process.ecalCalibMonitorClient.workers = ["IntegrityClient", "RawDataClient", "TestPulseClient", "PNIntegrityClient", "SummaryClient", "CalibrationSummaryClient"]
            else:
                process.ecalCalibMonitorClient.workers = ["IntegrityClient", "RawDataClient", "PedestalClient", "TestPulseClient", "LaserClient", "LedClient", "PNIntegrityClient", "SummaryClient", "CalibrationSummaryClient"]

        if local:
            process.ecalPNDiodeMonitorTask.verbosity = verbosity
            process.ecalPedestalMonitorTask.verbosity = verbosity
            process.ecalLaserLedMonitorTask.verbosity = verbosity
            process.ecalTestPulseMonitorTask.verbosity = verbosity
            process.ecalCalibMonitorClient.verbosity = verbosity
            
        #Need to configure the source for calib summary!!
    elif laser:
        # load laser monitor client
        process.ecalLaserMonitorClient.clientParameters.LightChecker.matacqPlotsDir = "/data/ecalod-disk01/dqm-data/laser"
    
    
    ### DQM COMMON MODULES ###
    
    process.load("DQMServices.Core.DQM_cfg")
    process.load("DQMServices.Components.DQMEnvironment_cfi")
    
    if physics:
        process.dqmEnv.subSystemFolder = cms.untracked.string("Ecal")
        process.dqmQTest = cms.EDAnalyzer("QualityTester",
            reportThreshold = cms.untracked.string("red"),
            prescaleFactor = cms.untracked.int32(1),
            qtList = cms.untracked.FileInPath("DQM/EcalCommon/data/EcalQualityTests.xml"),
            getQualityTestsFromFile = cms.untracked.bool(True),
            qtestOnEndLumi = cms.untracked.bool(True),
            qtestOnEndRun = cms.untracked.bool(True)
        )
    else:
        process.dqmEnv.subSystemFolder = cms.untracked.string("EcalCalibration")

    if central:
        referencePath = '/dqmdata/dqm/reference/'
    elif privEcal:
        referencePath = '/data/ecalod-disk01/dqm-data/online-DQM/'
    else:
        referencePath = ''

    if referencePath:
        if physics:
            process.DQMStore.referenceFileName = referencePath + 'ecal_reference.root'
        else:
            process.DQMStore.referenceFileName = referencePath + 'ecalcalib_reference.root'

    if options.doOutput:
        process.dqmSaver.convention = "Offline"
        process.dqmSaver.referenceHandling = "skip"
        process.dqmSaver.workflow = options.workflow

        if central:
            # copied from DQM.Integration.test.environment_cfi
            process.dqmSaver.convention = 'Online'
            process.dqmSaver.referenceHandling = 'all'
            process.dqmSaver.producer = 'DQM'
            process.dqmSaver.saveByTime = 1
            process.dqmSaver.saveByLumiSection = -1
            process.dqmSaver.saveByMinute = 8
            process.dqmSaver.saveByRun = 1
            process.dqmSaver.saveAtJobEnd = False
            process.dqmSaver.dirName = '/home/dqmprolocal/output'
        elif privEcal:
            process.dqmSaver.saveByTime = -1
            process.dqmSaver.saveByMinute = -1
            process.dqmSaver.dirName = "/data/ecalod-disk01/dqm-data/tmp"            
            # for privEcal online DQM, output will be stored as Offline files with versions
            if live:
                if physics:
                    process.dqmSaver.version = 1
                else:
                    process.dqmSaver.version = 2

        elif live:
            process.dqmSaver.convention = "Online"

        if options.outputDir:
            process.dqmSaver.dirName = options.outputDir

    if central:
        # copied from DQM.Integration.test.environment_cfi
        process.DQM.collectorHost = 'dqm-prod-local.cms'
        process.DQM.collectorPort = 9090
    elif live and privEcal:
        process.DQM.collectorHost = "ecalod-web01.cms"
        process.DQM.collectorPort = 9190
    elif live and local:
        process.DQM.collectorHost = "localhost"
        process.DQM.collectorPort = 8061
    else:
        process.DQM.collectorHost = ""

    if options.collector:
        process.DQM.collectorHost = options.collector.split(':')[0]
        process.DQM.collectorPort = int(options.collector.split(':')[1])
    
    ### FILTERS ###
    
    process.load("FWCore.Modules.preScaler_cfi")

    if physics:
        process.ecalPhysicsFilter = cms.EDFilter("EcalMonitorPrescaler",
            EcalRawDataCollection = cms.InputTag("ecalDigis"),
            clusterPrescaleFactor = cms.untracked.int32(1)
        )
    elif calib:
        process.ecalCalibrationFilter = cms.EDFilter("EcalMonitorPrescaler",
            EcalRawDataCollection = cms.InputTag("ecalDigis"),
            laserPrescaleFactor = cms.untracked.int32(1),
            ledPrescaleFactor = cms.untracked.int32(1),
            pedestalPrescaleFactor = cms.untracked.int32(1),
            testpulsePrescaleFactor = cms.untracked.int32(1)
        )
        process.ecalLaserLedFilter = cms.EDFilter("EcalMonitorPrescaler",
            EcalRawDataCollection = cms.InputTag("ecalDigis"),
            laserPrescaleFactor = cms.untracked.int32(1),
            ledPrescaleFactor = cms.untracked.int32(1)
        )
        process.ecalTestPulseFilter = cms.EDFilter("EcalMonitorPrescaler",
            EcalRawDataCollection = cms.InputTag("ecalDigis"),
            testpulsePrescaleFactor = cms.untracked.int32(1)
        )
        process.ecalPedestalFilter = cms.EDFilter("EcalMonitorPrescaler",
            EcalRawDataCollection = cms.InputTag("ecalDigis"),
            pedestalPrescaleFactor = cms.untracked.int32(1)
        )

    
    ### JOB PARAMETERS ###
    
    process.maxEvents = cms.untracked.PSet(
      input = cms.untracked.int32(-1)
    )

    if central:
        process.load("DQM.Integration.test.FrontierCondition_GT_cfi")
    else:
        process.load("Configuration.StandardSequences.FrontierConditions_GlobalTag_cff")

    process.GlobalTag.globaltag = options.globalTag

    frontier = options.frontier

    if frontier:
        process.GlobalTag.connect = frontier + "/CMS_COND_31X_GLOBALTAG"
    else:
        if p5:
            frontier = 'frontier://(proxyurl=http://localhost:3128)(serverurl=http://localhost:8000/FrontierOnProd)(serverurl=http://localhost:8000/FrontierOnProd)(retrieve-ziplevel=0)'
        else:
            frontier = 'frontier://FrontierProd'
   
    process.GlobalTag.toGet = cms.VPSet(
        cms.PSet(
            record = cms.string("EcalDQMChannelStatusRcd"),
            tag = cms.string("EcalDQMChannelStatus_v1_hlt"),
            connect = cms.untracked.string(frontier + "/CMS_COND_34X_ECAL")
        ),
        cms.PSet(
            record = cms.string("EcalDQMTowerStatusRcd"),
            tag = cms.string("EcalDQMTowerStatus_v1_hlt"),
            connect = cms.untracked.string(frontier + "/CMS_COND_34X_ECAL")
        )
    )

    process.MessageLogger = cms.Service("MessageLogger",
      cout = cms.untracked.PSet(
        threshold = cms.untracked.string("WARNING"),
        noLineBreaks = cms.untracked.bool(True),
        noTimeStamps = cms.untracked.bool(True),
        default = cms.untracked.PSet(
          limit = cms.untracked.int32(0)
        )
      ),
      destinations = cms.untracked.vstring("cout")
    )

    
    ### SEQUENCES AND PATHS ###
    
    if not laser:
        process.ecalPreRecoSequence = cms.Sequence(
            process.preScaler +
            process.ecalDigis
        )
    
        process.ecalRecoSequence = cms.Sequence(
            process.ecalGlobalUncalibRecHit +
            process.ecalDetIdToBeRecovered +
            process.ecalRecHit
        )
    
    if physics:
        process.ecalClusterSequence = cms.Sequence(
            process.hybridClusteringSequence +
            process.multi5x5ClusteringSequence
        )
        process.ecalClusterSequence.remove(process.multi5x5SuperClustersWithPreshower)

        process.ecalMonitorPath = cms.Path(
            process.ecalPreRecoSequence +
            process.ecalPhysicsFilter +
            process.ecalRecoSequence +
            process.ecalClusterSequence +
            process.simEcalTriggerPrimitiveDigis +
            process.ecalMonitorTask
        )

        process.ecalClientPath = cms.Path(
            process.ecalPreRecoSequence +
            process.ecalPhysicsFilter +
            process.ecalMonitorClient
        )
    elif calib:
        process.ecalLaserLedPath = cms.Path(
            process.ecalPreRecoSequence +
            process.ecalLaserLedFilter +    
            process.ecalRecoSequence +
            process.ecalLaserLedUncalibRecHit +
            process.ecalLaserLedMonitorTask +
            process.ecalPNDiodeMonitorTask
        )

        process.ecalTestPulsePath = cms.Path(
            process.ecalPreRecoSequence +
            process.ecalTestPulseFilter +    
            process.ecalRecoSequence +
            process.ecalTestPulseUncalibRecHit +
            process.ecalTestPulseMonitorTask +
            process.ecalPNDiodeMonitorTask
        )

        process.ecalPedestalPath = cms.Path(
            process.ecalPreRecoSequence +
            process.ecalPedestalFilter +    
            process.ecalRecoSequence +
            process.ecalPedestalMonitorTask +
            process.ecalPNDiodeMonitorTask
        )

        process.ecalClientPath = cms.Path(
            process.ecalPreRecoSequence +
            process.ecalCalibrationFilter +
            process.ecalCalibMonitorClient
        )

        if options.cfgType == 'Calibration':
            process.ecalMonitorPath = cms.Path(
                process.ecalPreRecoSequence +
                process.ecalMonitorTask
            )
    elif laser:
        process.ecalMonitorPath = cms.Path(
            process.ecalLaserMonitorClient
        )

    process.dqmEndPath = cms.EndPath(process.dqmEnv)
    if physics:
        process.dqmEndPath.insert(1, process.dqmQTest)
        
    if options.doOutput:
        process.dqmOutputPath = cms.EndPath(process.dqmSaver)

    if physics:
        process.schedule = cms.Schedule(
            process.ecalMonitorPath,
            process.ecalClientPath,
            process.dqmEndPath
        )
    elif calib:
        process.schedule = cms.Schedule(
            process.ecalLaserLedPath,
            process.ecalTestPulsePath,
            process.ecalPedestalPath,
        )
        if options.cfgType == 'Calibration':
            process.schedule.append(process.ecalMonitorPath)

        process.schedule.extend([process.ecalClientPath, process.dqmEndPath])
    elif laser:
        process.schedule = cms.Schedule(
            process.ecalMonitorPath,
            process.dqmEndPath
        )

    if options.doOutput:
        process.schedule.append(process.dqmOutputPath)
    
    ### SOURCE ###
    
    if live:
        process.load("DQM.Integration.test.inputsource_cfi")
        process.source.headerRetryInterval = 10

        if not central:
            # Central online DQM storage manager: http://dqm-c2d07-30.cms:22100/urn:xdaq-application:lid=30"    
            process.source.sourceURL = options.source

        if options.rawDataCollection == 'hltEcalCalibrationRaw':
            process.source.SelectHLTOutput = 'hltOutputCalibration'
            process.source.SelectEvents = cms.untracked.PSet(SelectEvents = cms.vstring("HLT_EcalCalibration_v*"))
        else:
            process.source.SelectHLTOutput = 'hltOutputA'

        if physics:
            process.source.consumerName = cms.untracked.string("Ecal DQM Consumer")
        else:
            process.source.consumerName = cms.untracked.string("EcalCalibration DQM Consumer")

    else:
        if '.dat' in options.source:
            process.source = cms.Source("NewEventStreamFileReader")
        else:
            process.source = cms.Source("PoolSource")

        process.source.fileNames = cms.untracked.vstring(options.source.split(','))

#def buildEcalDQMProcess
    
if 'FWCore.ParameterSet.Config' not in sys.modules:
    # cmsRun executes import FWCore.ParameterSet.Config as cms before running the configuration file via execfile()
    # __name__ == '__main__' check does not work, because any file executed through execfile is a main
    
    import FWCore.ParameterSet.Config as cms
    from optparse import OptionParser, OptionGroup
    
    # get options
    optparser = OptionParser()

    configOpts = OptionGroup(optparser, "Job configuration options", "Options passed to the job configuration")
    configOpts.add_option("-e", "--env", dest = "environment", default = "LocalOffline", help = "ENV=(CMSLive|PrivLive|PrivOffline|LocalLive|LocalOffline)", metavar = "ENV")
    configOpts.add_option("-c", "--cfg-type", dest = "cfgType", default = "Physics", help = "CONFIG=(Physics|Calibration|Laser)", metavar = "CFGTYPE")
    configOpts.add_option("-s", "--source", dest = "source", default = "", help = "source file name (comma separated) or URL", metavar = "SOURCE")
    configOpts.add_option("-r", "--rawdata", dest = "rawDataCollection", default = "rawDataCollector", help = "collection name", metavar = "RAWDATA")
    configOpts.add_option("-F", "--frontier-URL", dest = "frontier", default = "", help = "frontier URL", metavar = "URL")
    configOpts.add_option("-g", "--global-tag", dest = "globalTag", default = "", help = "global tag", metavar = "TAG")
    configOpts.add_option("-n", "--no-output", dest = "doOutput", action = "store_false", default = True, help = "turn off DQM ROOT file output")
    configOpts.add_option("-w", "--workflow", dest = "workflow", default = "", help = "offline workflow", metavar = "WORKFLOW")
    configOpts.add_option("-t", "--type", dest = "calibType", default = "", help = "ECAL run type", metavar = "CALIBTYPE")
    configOpts.add_option("-l", "--laser", dest = "laserWavelengths", default = "1,2,3,4", help = "Laser wavelengths", metavar = "WAVELENGTHS")
    configOpts.add_option("-m", "--mgpa", dest = "MGPAGains", default = "1,6,12", help = "MGPA gains", metavar = "GAINS")
    configOpts.add_option("-p", "--pn", dest = "MGPAGainsPN", default = "1,16", help = "PN MGPA gains", metavar = "GAINS")
    configOpts.add_option("-C", "--collector", dest = "collector", default = "", help = "Collector configuration", metavar = "HOST:PORT")
    configOpts.add_option("-o", "--output-dir", dest = "outputDir", default = "", help = "DQMFileSaver output directory", metavar = "DIR")
    configOpts.add_option("-v", "--verbosity", type = "int", dest = "verbosity", default = 0, help = "ECAL DQM verbosity", metavar = "VAL")
    optparser.add_option_group(configOpts)

    writeOpts = OptionGroup(optparser, "Write options", "Options for writing the job configuration file")
    writeOpts.add_option("-f", "--file", dest = "file", default = "", help = "write to FILE", metavar = "FILE")
    optparser.add_option_group(writeOpts)
    
    (options, args) = optparser.parse_args()

    process = cms.Process("DQM")
    
    buildEcalDQMProcess(process, options)

    # write cfg file
    fileName = options.file
    if not fileName:
        if options.cfgType == 'Physics':
            c = 'ecal'
        elif 'Calibration' in options.cfgType:
            c = 'ecalcalib'
        elif options.cfgType == 'Laser':
            c = 'ecallaser'
    
        if options.environment == 'CMSLive':
            e = 'live'
        elif options.environment == 'PrivLive':
            e = 'privlive'
        elif options.environment == 'LocalLive':
            e = 'locallive'
        else:
            e = 'data'
    
        fileName = c + '_dqm_sourceclient-' + e + '_cfg.py'
    
    cfgfile = file(fileName, "w")

    cfgfile.write("### AUTO-GENERATED CMSRUN CONFIGURATION FOR ECAL DQM ###")

    # VarParsing and OptionParser cannot coexist
    # copied from DQM.Integration.test.environment_cfi
    cfgfile.write("""
from FWCore.ParameterSet.VarParsing import VarParsing
from DQM.Integration.test.dqmPythonTypes import *

options = VarParsing('analysis')
options.register('runkey', 'pp_run', VarParsing.multiplicity.singleton, VarParsing.varType.string, 'Run Keys of CMS')

options.parseArguments()

runType = RunType(['pp_run','cosmic_run','hi_run','hpu_run'])
if not options.runkey.strip():
    options.runkey = 'pp_run'

runType.setRunType(options.runkey.strip())

""")
        
    cfgfile.write(process.dumpPython())

    if options.cfgType == 'Physics':
        cfgfile.write("""
### Run type specific ###

referenceFileName = process.DQMStore.referenceFileName.pythonValue()
if runType.getRunType() == runType.pp_run:
    process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_pp.root')
elif runType.getRunType() == runType.cosmic_run:
    process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_cosmic.root')
    process.dqmEndPath.remove(process.dqmQTest)
    process.ecalMonitorTask.workers = ['EnergyTask', 'IntegrityTask', 'OccupancyTask', 'RawDataTask', 'TrigPrimTask', 'PresampleTask', 'SelectiveReadoutTask']
    process.ecalMonitorClient.workers = ['IntegrityClient', 'OccupancyClient', 'PresampleClient', 'RawDataClient', 'SelectiveReadoutClient', 'TrigPrimClient', 'SummaryClient']
    process.ecalMonitorClient.workerParameters.SummaryClient.activeSources = ['Integrity', 'RawData', 'Presample', 'TriggerPrimitives', 'HotCell']
elif runType.getRunType() == runType.hi_run:
    process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_hi.root')
elif runType.getRunType() == runType.hpu_run:
    process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_hpu.root')
    process.source.SelectEvents = cms.untracked.PSet(SelectEvents = cms.vstring('*'))
    
""")

    cfgfile.write("""
if len(options.inputFiles):
    process.source.fileNames = options.inputFiles
""")
    
    cfgfile.close()

else:
    from FWCore.ParameterSet.VarParsing import VarParsing
    from DQM.Integration.test.dqmPythonTypes import *

    options = VarParsing("analysis")
    options.register("environment", default = "LocalOffline", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "ENV=(CMSLive|PrivLive|PrivOffline|LocalLive|LocalOffline)")
    options.register("cfgType", default = "Physics", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "CONFIG=(Physics|Calibration|Laser)")
    options.register("source", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "source file name (comma separated) or URL")
    options.register("rawDataCollection", default = "rawDataCollector", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "collection name")
    options.register("frontier", default = "", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "Frontier URL")
    options.register("globalTag", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "GlobalTag")
    options.register("doOutput", default = True, mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.bool, info = "output DQM ROOT file")
    options.register("workflow", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "offline workflow")
    options.register("calibType", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "ECAL run type")
    options.register("laserWavelengths", default = '1, 2, 3, 4', mult = VarParsing.multiplicity.list, mytype = VarParsing.varType.int, info = "Laser wavelengths")
    options.register("MGPAGains", default = '1, 6, 12', mult = VarParsing.multiplicity.list, mytype = VarParsing.varType.int, info = "MGPA gains")
    options.register("MGPAGainsPN", default = '1, 16', mult = VarParsing.multiplicity.list, mytype = VarParsing.varType.int, info = "PN MGPA gains")
    options.register('runkey', default = 'pp_run', mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = 'Run Keys of CMS')
    options.register('collector', mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = 'Collector configuration (host:port)')
    options.register('outputDir', mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = 'DQM output directory')
    options.register('verbosity', mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.int, info = 'ECAL DQM verbosity')

    options.clearList('laserWavelengths')
    options.clearList('MGPAGains')
    options.clearList('MGPAGainsPN')
    options.setDefault('laserWavelengths', '1, 2, 3, 4')
    options.setDefault('MGPAGains', '1, 6, 12')
    options.setDefault('MGPAGainsPN', '1, 16')

    options.parseArguments()

    process = cms.Process("DQM")

    buildEcalDQMProcess(process, options)

    if options.cfgType == 'Physics':
        runType = RunType(['pp_run','cosmic_run','hi_run','hpu_run'])
        if not options.runkey.strip():
            options.runkey = 'pp_run'

        runType.setRunType(options.runkey.strip())
    
        referenceFileName = process.DQMStore.referenceFileName.pythonValue()
        if runType.getRunType() == runType.pp_run:
            process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_pp.root')
        elif runType.getRunType() == runType.cosmic_run:
            process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_cosmic.root')
            process.dqmEndPath.remove(process.dqmQTest)
            process.ecalMonitorTask.workers = ['EnergyTask', 'IntegrityTask', 'OccupancyTask', 'RawDataTask', 'TrigPrimTask', 'PresampleTask', 'SelectiveReadoutTask']
            process.ecalMonitorClient.workers = ['IntegrityClient', 'OccupancyClient', 'PresampleClient', 'RawDataClient', 'SelectiveReadoutClient', 'TrigPrimClient', 'SummaryClient']
            process.ecalMonitorClient.workerParameters.SummaryClient.activeSources = ['Integrity', 'RawData', 'Presample', 'TriggerPrimitives', 'HotCell']
        elif runType.getRunType() == runType.hi_run:
            process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_hi.root')
        elif runType.getRunType() == runType.hpu_run:
            process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_hpu.root')
            process.source.SelectEvents = cms.untracked.PSet(SelectEvents = cms.vstring('*'))
