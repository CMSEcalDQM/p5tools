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

    if not options.rawDataCollection:
        raise RuntimeError("rawDataCollection name not given")

    isSource = ('source' in options.steps)
    isClient = ('client' in options.steps)

    if not isSource and not isClient:
        raise RuntimeError("job is neither source nor client")

    central = (options.environment == 'CMSLive')
    privEcal = ('Priv' in options.environment)
    local = ('Local' in options.environment)
    live = ('Live' in options.environment)

    if not central and (len(options.inputFiles) == 0 or not isSource):
        raise RuntimeError("Live mode requires a source to run")

    p5 = privEcal or central
           
    physics = (options.cfgType == 'Physics')
    calib = (options.cfgType == 'Calibration' or options.cfgType == 'CalibrationOnly')
    laser = (options.cfgType == 'Laser')

    verbosity = options.verbosity
    if verbosity < 0:
        if local: verbosity = 2
        else: verbosity = 0


    ### RECONSTRUCTION MODULES ###

    if isSource:
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
            process.load("RecoLocalCalo.EcalRecAlgos.EcalSeverityLevelESProducer_cfi")
            process.load("CalibCalorimetry.EcalLaserCorrection.ecalLaserCorrectionService_cfi")
    
        if physics:
            if options.useGEDClusters:
                process.load('Configuration.StandardSequences.MagneticField_38T_cff')
                process.load("Configuration.Geometry.GeometryIdeal_cff")
                process.load("RecoVertex.BeamSpotProducer.BeamSpot_cff")
                process.load("EventFilter.SiPixelRawToDigi.SiPixelRawToDigi_cfi")
                process.load("EventFilter.SiStripRawToDigi.SiStripDigis_cfi")
                process.siPixelDigis.InputLabel = 'rawDataCollector'
                process.load("RecoLocalTracker.Configuration.RecoLocalTracker_cff")
                process.load("RecoPixelVertexing.Configuration.RecoPixelVertexing_cff")
                process.load("RecoTracker.Configuration.RecoTracker_cff")
                process.iterTracking.remove(process.earlyMuons)
                process.iterTracking.remove(process.muonSeededSeedsInOut)
                process.iterTracking.remove(process.muonSeededTrackCandidatesInOut)
                process.iterTracking.remove(process.muonSeededSeedsOutIn)
                process.iterTracking.remove(process.muonSeededTrackCandidatesOutIn)
                process.load("RecoVertex.PrimaryVertexProducer.OfflinePrimaryVertices_cfi")
                process.load("RecoParticleFlow.PFClusterProducer.particleFlowRecHitECAL_cfi")
                process.load("RecoParticleFlow.PFClusterProducer.particleFlowRecHitPS_cfi")
                process.load("RecoParticleFlow.PFClusterProducer.particleFlowClusterECAL_cfi")
                process.load("RecoParticleFlow.PFClusterProducer.particleFlowClusterPS_cfi")
                process.load("RecoEcal.EgammaClusterProducers.particleFlowSuperClusterECAL_cfi")
                from EventFilter.ESRawToDigi.esRawToDigi_cfi import esRawToDigi
                process.ecalPreshowerDigis = esRawToDigi.clone()
                process.load("RecoLocalCalo.EcalRecProducers.ecalPreshowerRecHit_cfi")
            else:
                process.load("RecoEcal.EgammaClusterProducers.ecalClusteringSequence_cff")

            process.load("SimCalorimetry.EcalTrigPrimProducers.ecalTriggerPrimitiveDigis_cfi")
            process.simEcalTriggerPrimitiveDigis.Label = "ecalDigis"
            process.simEcalTriggerPrimitiveDigis.InstanceEB = "ebDigis"
            process.simEcalTriggerPrimitiveDigis.InstanceEE = "eeDigis"

            process.load("L1Trigger.Configuration.L1RawToDigi_cff")

            if not live: # for RecoSummaryTask and ClusterExtraTask
                process.load("RecoEcal.EgammaCoreTools.EcalNextToDeadChannelESProducer_cff")
                process.load("RecoEcal.EgammaClusterProducers.reducedRecHitsSequence_cff")
                if options.useGEDClusters:
                    process.reducedEcalRecHitsEB.interestingDetIdCollections = [cms.InputTag("interestingEcalDetIdPFEB")]
                    process.reducedEcalRecHitsEE.interestingDetIdCollections = [cms.InputTag("interestingEcalDetIdPFEE")]
                else:
                    process.reducedEcalRecHitsEB.interestingDetIdCollections = [cms.InputTag("interestingEcalDetIdEB")]
                    process.reducedEcalRecHitsEE.interestingDetIdCollections = [cms.InputTag("interestingEcalDetIdEE")]
    
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

    if physics:
        if isSource:
            process.load("DQM.EcalMonitorTasks.EcalMonitorTask_cfi")
            if live:
                process.ecalMonitorTask.workers = ["ClusterTask", "EnergyTask", "IntegrityTask", "OccupancyTask", "RawDataTask", "TimingTask", "TrigPrimTask", "PresampleTask", "SelectiveReadoutTask"]
                process.ecalMonitorTask.workerParameters.TrigPrimTask.params.runOnEmul = True
                process.ecalMonitorTask.collectionTags.Source = options.rawDataCollection
                process.ecalMonitorTask.verbosity = verbosity
                process.ecalMonitorTask.commonParameters.onlineMode = True
            if not options.useGEDClusters:
                process.ecalMonitorTask.collectionTags.EBBasicCluster = "hybridSuperClusters:hybridBarrelBasicClusters"
                process.ecalMonitorTask.collectionTags.EEBasicCluster = "multi5x5SuperClusters:multi5x5EndcapBasicClusters"
                process.ecalMonitorTask.collectionTags.EBSuperCluster = "correctedHybridSuperClusters"
                process.ecalMonitorTask.collectionTags.EESuperCluster = "multi5x5SuperClusters:multi5x5EndcapSuperClusters"

            process.ecalMonitorTask.collectionTags.TrigPrimEmulDigi = 'simEcalTriggerPrimitiveDigis'

            if options.outputMode != 2:
                process.ecalMonitorTask.commonParameters.willConvertToEDM = False

        if isClient:
            process.load("DQM.EcalMonitorClient.EcalMonitorClient_cfi")
            process.ecalMonitorClient.verbosity = verbosity
            if 'Offline' not in options.environment:
                process.ecalMonitorClient.workers = ["IntegrityClient", "OccupancyClient", "PresampleClient", "RawDataClient", "TimingClient", "SelectiveReadoutClient", "TrigPrimClient", "SummaryClient"]
                process.ecalMonitorClient.workerParameters.SummaryClient.params.activeSources = ["Integrity", "RawData", "Presample", "TriggerPrimitives", "Timing", "HotCell"]
                if live:
                    process.ecalMonitorClient.commonParameters.onlineMode = True

    elif calib:
        from DQM.EcalCommon.CommonParams_cfi import ecaldqmLaserWavelengths, ecaldqmMGPAGains, ecaldqmMGPAGainsPN
        ecaldqmLaserWavelengths = options.laserWavelengths
        ecaldqmMGPAGains = options.MGPAGains
        ecaldqmMGPAGainsPN = options.MGPAGainsPN

        if isSource:
            process.load("DQM.EcalMonitorTasks.EcalCalibMonitorTasks_cfi")
            process.ecalLaserLedMonitorTask.verbosity = verbosity
            process.ecalPedestalMonitorTask.verbosity = verbosity
            process.ecalTestPulseMonitorTask.verbosity = verbosity
            process.ecalPNDiodeMonitorTask.verbosity = verbosity

            if options.cfgType == 'Calibration':
                process.load("DQM.EcalMonitorTasks.EcalMonitorTask_cfi")
                process.ecalMonitorTask.workers = ["IntegrityTask", "RawDataTask"]
                process.ecalMonitorTask.collectionTags.Source = options.rawDataCollection

        if isClient:
            process.load("DQM.EcalMonitorClient.EcalCalibMonitorClient_cfi")
            process.ecalCalibMonitorClient.verbosity = verbosity
            if options.cfgType == 'Calibration':
                process.ecalCalibMonitorClient.workerParameters.SummaryClient.params.activeSources = ["Integrity", "RawData"]
                if options.calibType == 'PEDESTAL':
                    process.ecalCalibMonitorClient.workers = ["IntegrityClient", "RawDataClient", "PedestalClient", "PNIntegrityClient", "SummaryClient", "CalibrationSummaryClient"]
                elif options.calibType == 'TEST_PULSE':
                    process.ecalCalibMonitorClient.workers = ["IntegrityClient", "RawDataClient", "TestPulseClient", "PNIntegrityClient", "SummaryClient", "CalibrationSummaryClient"]
                else:
                    process.ecalCalibMonitorClient.workers = ["IntegrityClient", "RawDataClient", "PedestalClient", "TestPulseClient", "LaserClient", "LedClient", "PNIntegrityClient", "SummaryClient", "CalibrationSummaryClient"]

            
        #Need to configure the source for calib summary!!
    elif laser:
        # load laser monitor client
#        process.ecalLaserMonitorClient.clientParameters.LightChecker.matacqPlotsDir = "/data/ecalod-disk01/dqm-data/laser"
        pass

    if options.outputMode == 1:
        process.load("DQM.EcalCommon.EcalMEFormatter_cfi")
    
    ### DQM COMMON MODULES ###

    if live:
        process.load('DQM.Integration.test.environment_cfi')
    else:
        process.load("DQMServices.Core.DQM_cfg")
        process.load("DQMServices.Components.DQMEnvironment_cfi")
    
    if physics:
        if isSource:
            process.dqmEnv.subSystemFolder = cms.untracked.string("Ecal")
#        if isClient:
#            process.dqmQTest = cms.EDAnalyzer("QualityTester",
#                reportThreshold = cms.untracked.string("red"),
#                prescaleFactor = cms.untracked.int32(1),
#                qtList = cms.untracked.FileInPath("DQM/EcalCommon/data/EcalQualityTests.xml"),
#                getQualityTestsFromFile = cms.untracked.bool(True),
#                qtestOnEndLumi = cms.untracked.bool(True),
#                qtestOnEndRun = cms.untracked.bool(True)
#            )
    else:
        if isSource:
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

    if options.outputMode == 1:
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

        if options.outputFile:
            process.dqmSaver.dirName = options.outputFile

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
    process.preScaler.prescaleFactor = options.prescaleFactor

    if live:
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

    if isSource:
        process.maxEvents = cms.untracked.PSet(
          input = cms.untracked.int32(options.maxEvents)
        )
    else:
        process.maxEvents = cms.untracked.PSet(
          input = cms.untracked.int32(1)
        )

    frontier = options.frontier

    if p5:
        process.load('DQM.Integration.test.FrontierCondition_GT_cfi')
        if not frontier:
            frontier = str(process.GlobalTag.pfnPrefix)
    else:
        process.load("Configuration.StandardSequences.FrontierConditions_GlobalTag_cff")
        if options.globalTag.startswith('auto:'):
            from Configuration.AlCa.GlobalTag import GlobalTag
            process.GlobalTag = GlobalTag(process.GlobalTag, options.globalTag, '')
        else:
            process.GlobalTag.globaltag = options.globalTag

        if not frontier:
            frontier = 'frontier://FrontierProd'
            
        process.GlobalTag.connect = frontier + "/CMS_COND_31X_GLOBALTAG"            

        process.globalTagPrefer = cms.ESPrefer('PoolDBESSource', 'GlobalTag')
   
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
        cerr = cms.untracked.PSet(
            threshold = cms.untracked.string("WARNING"),
            noLineBreaks = cms.untracked.bool(True),
            noTimeStamps = cms.untracked.bool(True),
            default = cms.untracked.PSet(
                limit = cms.untracked.int32(-1)
            )
        ),
        destinations = cms.untracked.vstring("cerr")
    )

    
    ### SEQUENCES AND PATHS ###

    schedule = []

    if isSource:
        if not laser:
            process.ecalPreRecoSequence = cms.Sequence(
                process.ecalDigis
            )

            process.ecalRecoSequence = cms.Sequence(
                process.ecalGlobalUncalibRecHit +
                process.ecalDetIdToBeRecovered +
                process.ecalRecHit
            )
        
        if physics:
            process.ecalRecoSequence.insert(process.ecalRecoSequence.index(process.ecalRecHit), process.simEcalTriggerPrimitiveDigis)
            process.ecalRecoSequence.insert(process.ecalRecoSequence.index(process.ecalRecHit), process.gtDigis)
            
            if options.useGEDClusters:
                process.ecalClusterSequence = cms.Sequence(
                    process.ecalPreshowerDigis +
                    process.ecalPreshowerRecHit +
                    process.particleFlowRecHitECAL +
                    process.particleFlowRecHitPS +
                    process.particleFlowClusterPS +
                    process.particleFlowClusterECAL +
                    process.offlineBeamSpot +
                    process.siPixelDigis +
                    process.siStripDigis +
                    process.trackerlocalreco +
                    process.MeasurementTrackerEvent +
                    process.recopixelvertexing +
                    process.trackingGlobalReco +
                    process.offlinePrimaryVertices +
                    process.particleFlowSuperClusterECAL
                )
            else:
                process.ecalClusterSequence = cms.Sequence(
                    process.hybridClusteringSequence +
                    process.multi5x5ClusteringSequence
                )
                process.ecalClusterSequence.remove(process.multi5x5SuperClustersWithPreshower)

            process.ecalMonitorPath = cms.Path(
                process.preScaler +
                process.ecalPreRecoSequence +
                process.ecalRecoSequence +
                process.ecalClusterSequence +
                process.ecalMonitorTask
            )

            if live:
                process.ecalMonitorPath.insert(process.ecalMonitorPath.index(process.ecalRecoSequence), process.ecalPhysicsFilter)
            else:
                if options.useGEDClusters:
                    process.additionalRecoSequence = cms.Sequence(
                        process.interestingEcalDetIdPFEB +
                        process.interestingEcalDetIdPFEE +
                        process.reducedEcalRecHitsEB +
                        process.reducedEcalRecHitsEE
                    )
                else:
                    process.additionalRecoSequence = cms.Sequence(
                        process.interestingEcalDetIdEB +
                        process.interestingEcalDetIdEE +
                        process.reducedEcalRecHitsEB +
                        process.reducedEcalRecHitsEE
                    )
   
                process.ecalMonitorPath.insert(process.ecalMonitorPath.index(process.ecalMonitorTask), process.additionalRecoSequence)

            schedule.append(process.ecalMonitorPath)

        elif calib:
            process.ecalLaserLedPath = cms.Path(
                process.ecalPreRecoSequence +
                process.ecalRecoSequence +
                process.ecalLaserLedUncalibRecHit +
                process.ecalLaserLedMonitorTask +
                process.ecalPNDiodeMonitorTask
            )

            if live:
                process.ecalLaserLedPath.insert(1, process.ecalLaserLedFilter)
                process.ecalLaserLedPath.insert(0, process.preScaler)

            schedule.append(process.ecalLaserLedPath)
    
            process.ecalTestPulsePath = cms.Path(
                process.ecalPreRecoSequence +
                process.ecalRecoSequence +
                process.ecalTestPulseUncalibRecHit +
                process.ecalTestPulseMonitorTask +
                process.ecalPNDiodeMonitorTask
            )

            if live:
                process.ecalTestPulsePath.insert(1, process.ecalTestPulseFilter)
                process.ecalTestPulsePath.insert(0, process.preScaler)

            schedule.append(process.ecalTestPulsePath)
    
            process.ecalPedestalPath = cms.Path(
                process.ecalPreRecoSequence +
                process.ecalPedestalFilter +    
                process.ecalRecoSequence +
                process.ecalPedestalMonitorTask +
                process.ecalPNDiodeMonitorTask
            )

            if live:
                process.ecalPedestalPath.insert(1, process.ecalPedestalFilter)
                process.ecalPedestalPath.insert(0, process.preScaler)

            schedule.append(process.ecalPedestalPath)

            if options.cfgType == 'Calibration':
                process.ecalMonitorPath = cms.Path(
                    process.ecalPreRecoSequence +
                    process.ecalMonitorTask
                )

                schedule.append(process.ecalMonitorPath)

        process.dqmEndPath = cms.EndPath(process.dqmEnv)

        schedule.append(process.dqmEndPath)
            
    if isClient:
        if physics:
            process.ecalClientPath = cms.Path(
                process.ecalPreRecoSequence +
                process.ecalMonitorClient
            )

            if live:
                process.ecalClientPath.insert(1, process.ecalPhysicsFilter)
                process.ecalClientPath.insert(0, process.preScaler)

            schedule.append(process.ecalClientPath)
#            process.dqmEndPath.insert(1, process.dqmQTest)
        elif calib:    
            process.ecalClientPath = cms.Path(
                process.ecalPreRecoSequence +
                process.ecalCalibrationFilter +
                process.ecalCalibMonitorClient
            )

            if live:
                process.ecalClientPath.insert(1, process.ecalCalibrationFilter)
                process.ecalClientPath.insert(0, process.preScaler)

            schedule.append(process.ecalClientPath)
        elif laser:
#            process.ecalMonitorPath = cms.Path(
#                process.ecalLaserMonitorClient
#            )
            pass

    if options.outputMode == 1:
        process.dqmOutputPath = cms.EndPath(process.ecalMEFormatter + process.dqmSaver)
        schedule.append(process.dqmOutputPath)
    elif options.outputMode == 2:
        process.load("DQMServices.Components.MEtoEDMConverter_cfi")
        process.dqmEndPath.insert(1, process.MEtoEDMConverter)

        process.DQMoutput = cms.OutputModule("PoolOutputModule",
            splitLevel = cms.untracked.int32(0),
            outputCommands = cms.untracked.vstring("drop *", "keep *_MEtoEDMConverter_*_*"),
            fileName = cms.untracked.string(options.outputFile),
            dataset = cms.untracked.PSet(
                filterName = cms.untracked.string(''),
                dataTier = cms.untracked.string('')
            )
        )

        process.dqmOutputPath = cms.EndPath(process.DQMoutput)

        schedule.append(process.dqmOutputPath)

    process.schedule = cms.Schedule(*schedule)
    
    ### SOURCE ###
    
    if live:
        #process.load("DQM.Integration.test.inputsource_cfi")
        # input source uses VarParsing (Jul 2 2014)
        # options.inputFiles must be [inputDir, runNumber] in this case
        process.source = cms.Source("DQMStreamerReader",
            runNumber = cms.untracked.uint32(int(options.inputFiles[1])),
            runInputDir = cms.untracked.string(options.inputFiles[0]),
            streamLabel = cms.untracked.string(''),
            minEventsPerLumi = cms.untracked.int32(1),
            delayMillis = cms.untracked.uint32(500),
            skipFirstLumis = cms.untracked.bool(False),
            deleteDatFiles = cms.untracked.bool(False),
            endOfRunKills  = cms.untracked.bool(True),
        )

#        if options.rawDataCollection == 'hltEcalCalibrationRaw':
#            process.source.SelectHLTOutput = 'hltOutputCalibration'
#            process.source.SelectEvents = cms.untracked.PSet(SelectEvents = cms.vstring("HLT_EcalCalibration_v*"))
#        else:
#            process.source.SelectHLTOutput = 'hltOutputA'

    else:
        if '.dat' in options.inputFiles[0]:
            process.source = cms.Source("NewEventStreamFileReader")
        else:
            process.source = cms.Source("PoolSource")

        process.source.fileNames = cms.untracked.vstring(options.inputFiles)


    process.prune()

#def buildEcalDQMProcess
    
if __name__ == '__main__':
    if 'FWCore.ParameterSet.Config' not in sys.modules: #standalone python
        # cmsRun executes import FWCore.ParameterSet.Config as cms before running the configuration file via execfile()
        # __name__ == '__main__' cannot distinguish the standalone python and cmsRun usages, because any file executed through execfile is a main
        
        import FWCore.ParameterSet.Config as cms
        from optparse import OptionParser, OptionGroup
        
        # get options
        optparser = OptionParser()
    
        commonOpts = OptionGroup(optparser, "Common job configuration options", "Options passed to the job configuration")
        commonOpts.add_option("-e", "--env", dest = "environment", default = "LocalOffline", help = "ENV=(CMSLive|PrivLive|PrivOffline|LocalLive|LocalOffline)", metavar = "ENV")
        commonOpts.add_option("-c", "--cfg-type", dest = "cfgType", default = "Physics", help = "CONFIG=(Physics|Calibration|Laser)", metavar = "CFGTYPE")
        commonOpts.add_option("-s", "--steps", dest = "steps", default = "sourceclient", help = "STEPS=[source][client]", metavar = "STEPS")
        commonOpts.add_option("-i", "--input-files", dest = "inputFiles", default = "", help = "source file name (comma separated) or URL", metavar = "SOURCE")
        commonOpts.add_option("-r", "--rawdata", dest = "rawDataCollection", default = "rawDataCollector", help = "collection name", metavar = "RAWDATA")
        commonOpts.add_option("-F", "--frontier-URL", dest = "frontier", default = "", help = "frontier URL", metavar = "URL")
        commonOpts.add_option("-g", "--global-tag", dest = "globalTag", default = "auto:com10", help = "global tag", metavar = "TAG")
        commonOpts.add_option("-O", "--output-mode", type = "int", dest = "outputMode", default = 1, help = "0: no output, 1: DQM output, 2: EDM output", metavar = "MODE")
        commonOpts.add_option("-w", "--workflow", dest = "workflow", default = "", help = "offline workflow", metavar = "WORKFLOW")
        commonOpts.add_option("-G", "--use-GED", dest = "useGEDClusters", action = "store_true", default = False, help = "switch for GED clusters")
        commonOpts.add_option("-C", "--collector", dest = "collector", default = "", help = "Collector configuration", metavar = "HOST:PORT")
        commonOpts.add_option("-o", "--output-path", dest = "outputFile", default = "", help = "DQMFileSaver output directory / PoolOutputModule output path", metavar = "DIR")
        commonOpts.add_option("-x", "--max-events", type = "int", dest = "maxEvents", default = -1, help = "Maximum events to process", metavar = "VAL")
        commonOpts.add_option("-P", "--prescale", dest = "prescaleFactor", default = 1, help = "Prescale factor", metavar = "FACTOR")
        commonOpts.add_option("-v", "--verbosity", type = "int", dest = "verbosity", default = 0, help = "ECAL DQM verbosity", metavar = "VAL")
        optparser.add_option_group(commonOpts)

        calibOnlyOpts = OptionGroup(optparser, "Calibration configuration options", "Options, passed to the job configuration")
        calibOnlyOpts.add_option("-t", "--type", dest = "calibType", default = "", help = "ECAL run type", metavar = "CALIBTYPE")
        calibOnlyOpts.add_option("-l", "--laser", dest = "laserWavelengths", default = "1,2,3,4", help = "Laser wavelengths", metavar = "WAVELENGTHS")
        calibOnlyOpts.add_option("-m", "--mgpa", dest = "MGPAGains", default = "1,6,12", help = "MGPA gains", metavar = "GAINS")
        calibOnlyOpts.add_option("-p", "--pn", dest = "MGPAGainsPN", default = "1,16", help = "PN MGPA gains", metavar = "GAINS")
        optparser.add_option_group(calibOnlyOpts)
    
        writeOpts = OptionGroup(optparser, "Write options", "Options for writing the job configuration file")
        writeOpts.add_option("-f", "--file", dest = "file", default = "", help = "write to FILE", metavar = "FILE")
        optparser.add_option_group(writeOpts)
        
        (options, args) = optparser.parse_args()
    
        options.inputFiles = options.inputFiles.split(',')
        if not options.inputFiles[0]:
            options.inputFiles = []

        if 'Live' in options.environment:
            if len(options.inputFiles) < 2:
                options.inputFiles = ['', 0]
    
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
        
            fileName = c + '_dqm_' + options.steps + '-' + e + '_cfg.py'
        
        cfgfile = file(fileName, "w")
    
        cfgfile.write("### AUTO-GENERATED CMSRUN CONFIGURATION FOR ECAL DQM ###")
    
        # VarParsing and OptionParser cannot coexist
        # copied from DQM.Integration.test.environment_cfi
        cfgfile.write("""
from FWCore.ParameterSet.VarParsing import VarParsing

options = VarParsing('analysis')
options.register('runkey', default = 'pp_run', mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = 'Run Keys of CMS')
options.register('runNumber', default = 194533, mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.int, info = "Run number.")
options.register('runInputDir', default = '/fff/BU0/test', mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "Directory where the DQM files will appear.")
options.register('skipFirstLumis', default = False, mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.bool, info = "Skip (and ignore the minEventsPerLumi parameter) for the files which have been available at the begining of the processing.")

options.parseArguments()

""")
        if 'Live' in options.environment:
            cfgfile.write("""
from DQM.Integration.test.dqmPythonTypes import *
runType = RunType(['pp_run','cosmic_run','hi_run','hpu_run'])
if not options.runkey.strip():
    options.runkey = 'pp_run'

runType.setRunType(options.runkey.strip())

""")
            
        cfgfile.write(process.dumpPython())
    
        if 'Live' in options.environment:
            cfgfile.write("""
### Setup source ###
process.source.runNumber = options.runNumber
process.source.runInputDir = options.runInputDir
process.source.skipFirstLumis = options.skipFirstLumis
""")
            if options.cfgType == 'Physics':
                cfgfile.write("""
### Run type specific ###

referenceFileName = process.DQMStore.referenceFileName.pythonValue()
if runType.getRunType() == runType.pp_run:
    process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_pp.root')
elif runType.getRunType() == runType.cosmic_run:
    process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_cosmic.root')
#    process.dqmEndPath.remove(process.dqmQTest)
    process.ecalMonitorTask.workers = ['EnergyTask', 'IntegrityTask', 'OccupancyTask', 'RawDataTask', 'TrigPrimTask', 'PresampleTask', 'SelectiveReadoutTask']
    process.ecalMonitorClient.workers = ['IntegrityClient', 'OccupancyClient', 'PresampleClient', 'RawDataClient', 'SelectiveReadoutClient', 'TrigPrimClient', 'SummaryClient']
    process.ecalMonitorClient.workerParameters.SummaryClient.params.activeSources = ['Integrity', 'RawData', 'Presample', 'TriggerPrimitives', 'HotCell']
elif runType.getRunType() == runType.hi_run:
    process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_hi.root')
elif runType.getRunType() == runType.hpu_run:
    process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_hpu.root')
    process.source.SelectEvents = cms.untracked.PSet(SelectEvents = cms.vstring('*'))
""")
        else:
            cfgfile.write("""
### Setup source ###

if options.inputFiles:
    process.source.fileNames = options.inputFiles
if options.maxEvents != -1:
    process.maxEvents.input = options.maxEvents
""")

        if options.outputMode == 2:
            cfgfile.write("""
### Output name ###

if options.outputFile:
    process.DQMoutput.fileName = options.outputFile
""")
        
        cfgfile.close()
    
    else:
        from FWCore.ParameterSet.VarParsing import VarParsing
    
        options = VarParsing("analysis")
        options._tags.pop('numEvent%d')
        options._tagOrder.remove('numEvent%d')
        
        options.register("environment", default = "LocalOffline", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "ENV=(CMSLive|PrivLive|PrivOffline|LocalLive|LocalOffline)")
        options.register("cfgType", default = "Physics", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "CONFIG=(Physics|Calibration|Laser)")
        options.register("steps", default = "sourceclient", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "DQM steps to perform")
        options.register("rawDataCollection", default = "rawDataCollector", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "collection name")
        options.register("frontier", default = "", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "Frontier URL")
        options.register("globalTag", default = 'auto:com10', mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "GlobalTag")
        options.register("outputMode", default = 1, mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.int, info = "0: no output, 1: DQM output, 2: EDM output")
        options.register("workflow", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "offline workflow")
        options.register("useGEDClusters", default = False, mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.bool, info = 'switch for GED clusters')
        options.register("calibType", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "ECAL run type")
        options.register("laserWavelengths", default = '1, 2, 3, 4', mult = VarParsing.multiplicity.list, mytype = VarParsing.varType.int, info = "Laser wavelengths")
        options.register("MGPAGains", default = '1, 6, 12', mult = VarParsing.multiplicity.list, mytype = VarParsing.varType.int, info = "MGPA gains")
        options.register("MGPAGainsPN", default = '1, 16', mult = VarParsing.multiplicity.list, mytype = VarParsing.varType.int, info = "PN MGPA gains")
        options.register('prescaleFactor', default = 1, mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.int, info = 'Prescale factor')
        options.register('runkey', default = 'pp_run', mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = 'Run Keys of CMS')
        options.register('collector', mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = 'Collector configuration (host:port)')
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
    
        if options.cfgType == 'Physics' and 'Live' in options.environment:
            from DQM.Integration.test.dqmPythonTypes import *
            runType = RunType(['pp_run','cosmic_run','hi_run','hpu_run'])
            if not options.runkey.strip():
                options.runkey = 'pp_run'
    
            runType.setRunType(options.runkey.strip())
        
            referenceFileName = process.DQMStore.referenceFileName.pythonValue()
            if runType.getRunType() == runType.pp_run:
                process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_pp.root')
            elif runType.getRunType() == runType.cosmic_run:
                process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_cosmic.root')
#                process.dqmEndPath.remove(process.dqmQTest)
                process.ecalMonitorTask.workers = ['EnergyTask', 'IntegrityTask', 'OccupancyTask', 'RawDataTask', 'TrigPrimTask', 'PresampleTask', 'SelectiveReadoutTask']
                process.ecalMonitorClient.workers = ['IntegrityClient', 'OccupancyClient', 'PresampleClient', 'RawDataClient', 'SelectiveReadoutClient', 'TrigPrimClient', 'SummaryClient']
                process.ecalMonitorClient.workerParameters.SummaryClient.params.activeSources = ['Integrity', 'RawData', 'Presample', 'TriggerPrimitives', 'HotCell']
            elif runType.getRunType() == runType.hi_run:
                process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_hi.root')
            elif runType.getRunType() == runType.hpu_run:
                process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_hpu.root')
                process.source.SelectEvents = cms.untracked.PSet(SelectEvents = cms.vstring('*'))
