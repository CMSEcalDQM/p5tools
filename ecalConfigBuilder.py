def buildEcalDQMModules(process, options):
    """
    Build a standalone cms.Process that can be used for (quasi-)online DQM and local testing
    """

    ### SET OPTION FLAGS ###
    
    if options.environment not in ['CMSLive', 'PrivLive', 'PrivOffline', 'LocalLive', 'LocalOffline']:
        raise RuntimeError("environment value " + options.environment + " not correct")
    
    if options.cfgType not in ['Physics', 'Calibration', 'CalibrationStandalone', 'Laser']:
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

    p5 = privEcal or central
           
    physics = (options.cfgType == 'Physics')
    calib = (options.cfgType == 'Calibration' or options.cfgType == 'CalibrationStandalone')
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
            process.ecalRecHit.EBuncalibRecHitCollection = "ecalGlobalUncalibRecHit:EcalUncalibRecHitsEB"
            process.ecalRecHit.EEuncalibRecHitCollection = "ecalGlobalUncalibRecHit:EcalUncalibRecHitsEE"
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

            process.load("RecoLuminosity.LumiProducer.bunchSpacingProducer_cfi")
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
                algoPSet= cms.PSet(
                    MinAmplBarrel = cms.double(12.),
                    MinAmplEndcap = cms.double(16.)
                )
            )
         #   process.ecalLaserLedUncalibRecHit.algoPSet.MinAmplBarrel = 12.
         #   process.ecalLaserLedUncalibRecHit.algoPSet.MinAmplEndcap = 16.
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

            process.ecalMonitorTask.collectionTags.EBUncalibRecHit = "ecalGlobalUncalibRecHit:EcalUncalibRecHitsEB"
            process.ecalMonitorTask.collectionTags.EEUncalibRecHit = "ecalGlobalUncalibRecHit:EcalUncalibRecHitsEE"
            process.ecalMonitorTask.collectionTags.TrigPrimEmulDigi = 'simEcalTriggerPrimitiveDigis'

            if options.outputMode != 2:
                process.ecalMonitorTask.commonParameters.willConvertToEDM = False

        if isClient:
            process.load("DQM.EcalMonitorClient.EcalMonitorClient_cfi")
            process.ecalMonitorClient.verbosity = verbosity
            if live:
                process.ecalMonitorClient.workers = ["IntegrityClient", "OccupancyClient", "PresampleClient", "RawDataClient", "TimingClient", "SelectiveReadoutClient", "TrigPrimClient", "SummaryClient"]
#                process.ecalMonitorClient.workerParameters.SummaryClient.params.activeSources = ["Integrity", "RawData", "Presample", "TriggerPrimitives", "Timing", "HotCell"]
# removing Timing from the list of summary client sources during commissioning & until DQM can do run-typing
                process.ecalMonitorClient.workerParameters.SummaryClient.params.activeSources = ["Integrity", "RawData", "Presample", "TriggerPrimitives", "HotCell"]

                process.ecalMonitorClient.commonParameters.onlineMode = True

    elif calib:
        from DQM.EcalCommon.CommonParams_cfi import ecaldqmLaserWavelengths, ecaldqmMGPAGains, ecaldqmMGPAGainsPN
        for wl in options.laserWavelengths:
            if wl not in ecaldqmLaserWavelengths:
                ecaldqmLaserWavelengths.append(wl)
        for gain in options.MGPAGains:
            if gain not in ecaldqmMGPAGains:
                ecaldqmMGPAGains.append(gain)
        for gain in options.MGPAGainsPN:
            if gain not in ecaldqmMGPAGainsPN:
                ecaldqmMGPAGainsPN.append(gain)

        if isSource:
            process.load("DQM.EcalMonitorTasks.EcalCalibMonitorTasks_cfi")
            process.ecalLaserLedMonitorTask.verbosity = verbosity
            process.ecalPedestalMonitorTask.verbosity = verbosity
            process.ecalTestPulseMonitorTask.verbosity = verbosity
            process.ecalPNDiodeMonitorTask.verbosity = verbosity

            if live:
                process.ecalLaserLedMonitorTask.commonParameters.onlineMode = True
                process.ecalPedestalMonitorTask.commonParameters.onlineMode = True
                process.ecalTestPulseMonitorTask.commonParameters.onlineMode = True
                process.ecalPNDiodeMonitorTask.commonParameters.onlineMode = True

            if options.cfgType == 'CalibrationStandalone':
                process.load("DQM.EcalMonitorTasks.EcalMonitorTask_cfi")
                process.ecalMonitorTask.workers = ["IntegrityTask", "RawDataTask","PresampleTask"]
                process.ecalMonitorTask.collectionTags.Source = options.rawDataCollection

            process.ecalLaserLedMonitorTask.collectionTags.EBLaserLedUncalibRecHit = 'ecalLaserLedUncalibRecHit:EcalUncalibRecHitsEB'
            process.ecalLaserLedMonitorTask.collectionTags.EELaserLedUncalibRecHit = 'ecalLaserLedUncalibRecHit:EcalUncalibRecHitsEE'

        if isClient:
            #add the 1st line to enable noise plots
            process.load("DQM.EcalMonitorClient.EcalCalibMonitorClient_cfi")
            process.ecalCalibMonitorClient.verbosity = verbosity

            if live:
                process.ecalCalibMonitorClient.commonParameters.onlineMode = True
            
            if options.cfgType == 'CalibrationStandalone':
                process.ecalCalibMonitorClient.workerParameters.SummaryClient.params.activeSources = ["Integrity", "RawData","Presample"]
                if options.calibType == 'PEDESTAL':
                    process.ecalCalibMonitorClient.workers = ["IntegrityClient", "RawDataClient", "PedestalClient", "PNIntegrityClient", "SummaryClient", "CalibrationSummaryClient"]
                elif options.calibType == 'TEST_PULSE':
                    process.ecalCalibMonitorClient.workers = ["IntegrityClient", "RawDataClient", "TestPulseClient", "PNIntegrityClient", "SummaryClient", "CalibrationSummaryClient"]
                else:
                    process.ecalCalibMonitorClient.workers = ["IntegrityClient", "RawDataClient","PedestalClient", "TestPulseClient", "LaserClient", "LedClient", "PNIntegrityClient", "SummaryClient", "CalibrationSummaryClient", "PresampleClient"]



        #Need to configure the source for calib summary!!
    elif laser:
        # load laser monitor client
#        process.ecalLaserMonitorClient.clientParameters.LightChecker.matacqPlotsDir = "/data/dqm-data/laser"
        pass

    if options.outputMode == 1 and not isSource and isClient:
        process.load("DQM.EcalCommon.EcalMEFormatter_cfi")


    ### DQM COMMON MODULES ###

    if live:
        process.load('DQM.Integration.config.environment_cfi')
        process.load("DQMServices.Components.DQMFileSaver_cfi")
    else:
        process.load("DQMServices.Core.DQM_cfg")
        process.load("DQMServices.Components.DQMEnvironment_cfi")
        process.DQM = cms.Service("DQM",                                                                                                                                                                           
            debug = cms.untracked.bool(False),
            publishFrequency = cms.untracked.double(5.0),
            collectorPort = cms.untracked.int32(0),
            collectorHost = cms.untracked.string(''),
            filter = cms.untracked.string('')
        )      
    
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
        referencePath = '/data/dqm-data/online-DQM/'
    else:
        referencePath = ''

    if referencePath:
        if physics:
            process.DQMStore.referenceFileName = referencePath + 'ecal_reference.root'
        else:
            process.DQMStore.referenceFileName = referencePath + 'ecalcalib_reference.root'

    if options.outputMode == 1:
        if privEcal:
            if not options.workflow:
                raise RuntimeError('No workflow parameter')

            process.dqmSaver.convention = "Offline"
            process.dqmSaver.referenceHandling = "skip"
            process.dqmSaver.workflow = options.workflow
            process.dqmSaver.dirName = "/data/dqm-data/tmp" 
            process.dqmSaver.producer = 'DQM'

        elif not central:
            process.dqmSaver.referenceHandling = "skip"
            process.dqmSaver.workflow = options.workflow

            if live:
                process.dqmSaver.convention = "Online"
            else:
                process.dqmSaver.convention = "Offline"

        if process.dqmSaver.convention == 'Offline':
            if physics:
                process.dqmSaver.version = 1
            else:
                process.dqmSaver.version = 2

        if options.outputPath:
            process.dqmSaver.dirName = options.outputPath

    if live and privEcal:
        process.DQM.collectorHost = "ecalod-web01.cms"
        process.DQM.collectorPort = 9190
    elif live and local:
        process.DQM.collectorHost = "localhost"
        process.DQM.collectorPort = 8061
    elif not central:
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
                physics = cms.untracked.uint32(1),
                cosmics = cms.untracked.uint32(1)
            )
        elif calib:
            process.ecalLaserLedFilter = cms.EDFilter("EcalMonitorPrescaler",
                EcalRawDataCollection = cms.InputTag("ecalDigis"),
                laser = cms.untracked.uint32(1),
                led = cms.untracked.uint32(1)
            )
            process.ecalTestPulseFilter = cms.EDFilter("EcalMonitorPrescaler",
                EcalRawDataCollection = cms.InputTag("ecalDigis"),
                testPulse = cms.untracked.uint32(1)
            )
            process.ecalPedestalFilter = cms.EDFilter("EcalMonitorPrescaler",
                EcalRawDataCollection = cms.InputTag("ecalDigis"),
                pedestal = cms.untracked.uint32(1)
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

    if p5:
        process.load('DQM.Integration.config.FrontierCondition_GT_cfi')
    else:
        process.load("Configuration.StandardSequences.FrontierConditions_GlobalTag_cff")
        if options.globalTag.startswith('auto:'):
            from Configuration.AlCa.GlobalTag import GlobalTag
            process.GlobalTag = GlobalTag(process.GlobalTag._cmsObject, options.globalTag, '')
        else:
            process.GlobalTag.globaltag = options.globalTag

    connect = process.GlobalTag.connect.value()
   
    process.GlobalTag.toGet = cms.VPSet(
        cms.PSet(
            record = cms.string("EcalDQMChannelStatusRcd"),
            tag = cms.string("EcalDQMChannelStatus_v1_hlt"),
            connect = cms.string(connect.replace('CMS_COND_31X_GLOBALTAG', 'CMS_COND_34X_ECAL'))
        ),
        cms.PSet(
            record = cms.string("EcalDQMTowerStatusRcd"),
            tag = cms.string("EcalDQMTowerStatus_v1_hlt"),
            connect = cms.string(connect.replace('CMS_COND_31X_GLOBALTAG', 'CMS_COND_34X_ECAL'))
        )
    )

    process.MessageLogger = cms.Service("MessageLogger",
        destinations = cms.untracked.vstring('cerr', 'cout'),
        categories = cms.untracked.vstring('EcalDQM', 'EcalLaserDbService'),
        cerr = cms.untracked.PSet(
            threshold = cms.untracked.string("WARNING"),
            noLineBreaks = cms.untracked.bool(True),
            noTimeStamps = cms.untracked.bool(True),
            default = cms.untracked.PSet(
                limit = cms.untracked.int32(0)
            )
        ),
        cout = cms.untracked.PSet(
            threshold = cms.untracked.string('INFO'),
            EcalDQM = cms.untracked.PSet(
                limit = cms.untracked.int32(-1)
            ),
            EcalLaserDbService = cms.untracked.PSet(
                limit = cms.untracked.int32(10)
            ),
            default = cms.untracked.PSet(
                limit = cms.untracked.int32(0)
            )
        )
    )

    ### SOURCE ###
    
    if live:
        process.load("DQM.Integration.config.inputsource_cfi")  # input source uses VarParsing (Jul 2 2014)
        if physics:
            process.source.streamLabel = 'streamLookArea'
        if not central:
            pass
#            process.source.endOfRunKills = False
        if calib and options.cfgType != 'CalibrationStandalone':
#            process.source.streamLabel = 'streamDQMCalibration'
            process.source.streamLabel = 'streamLookArea'

    else:
        if '.dat' in options.inputFiles[0]:
            process.source = cms.Source("NewEventStreamFileReader")
        else:
            process.source = cms.Source("PoolSource")

        if options.inputList:
            inputFiles = []
            with open(options.inputList) as sourceList:
                for line in sourceList:
                    inputFiles.append(line.strip())

            process.source.fileNames = cms.untracked.vstring(inputFiles)

        elif options.inputFiles:
            process.source.fileNames = cms.untracked.vstring(options.inputFiles)

#def buildEcalDQMProcess

def buildEcalDQMSequences(process, options):

    isSource = ('source' in options.steps)
    isClient = ('client' in options.steps)

    central = (options.environment == 'CMSLive')
    privEcal = ('Priv' in options.environment)
    local = ('Local' in options.environment)
    live = ('Live' in options.environment)

    p5 = privEcal or central
           
    physics = (options.cfgType == 'Physics')
    calib = (options.cfgType == 'Calibration' or options.cfgType == 'CalibrationStandalone')
    laser = (options.cfgType == 'Laser')

    verbosity = options.verbosity
    if verbosity < 0:
        if local: verbosity = 2
        else: verbosity = 0

    ### SEQUENCES ###

    process.ecalPreRecoSequence = cms.Sequence(process.ecalDigis)

    if isSource:
        process.ecalRecoSequence = cms.Sequence()

        if not laser:
            process.ecalRecoSequence += cms.Sequence(
                process.ecalGlobalUncalibRecHit +
                process.ecalDetIdToBeRecovered +
                process.ecalRecHit
            )
        
        if physics:
            process.ecalPreRecoSequence = cms.Sequence(process.bunchSpacingProducer+process.ecalDigis)
            process.ecalRecoSequence += cms.Sequence(
                process.simEcalTriggerPrimitiveDigis +
                process.gtDigis
            )
            
            if options.useGEDClusters:
                process.iterTracking.remove(process.earlyMuons)
                process.iterTracking.remove(process.muonSeededSeedsInOut)
                process.iterTracking.remove(process.muonSeededTrackCandidatesInOut)
                process.iterTracking.remove(process.muonSeededSeedsOutIn)
                process.iterTracking.remove(process.muonSeededTrackCandidatesOutIn)
                process.ecalRecoSequence += cms.Sequence(
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
                process.multi5x5ClusteringSequence.remove(process.multi5x5SuperClustersWithPreshower)
                process.ecalRecoSequence += cms.Sequence(
                    process.hybridClusteringSequence +
                    process.multi5x5ClusteringSequence
                )

            if not live:
                if options.useGEDClusters:
                    process.ecalRecoSequence += cms.Sequence(
                        process.interestingEcalDetIdPFEB +
                        process.interestingEcalDetIdPFEE +
                        process.reducedEcalRecHitsEB +
                        process.reducedEcalRecHitsEE
                    )
                else:
                    process.ecalRecoSequence += cms.Sequence(
                        process.interestingEcalDetIdEB +
                        process.interestingEcalDetIdEE +
                        process.reducedEcalRecHitsEB +
                        process.reducedEcalRecHitsEE
                    )
                    
    ### PATHS ###

        paths = []
        endpaths = []

        if physics:
            process.ecalMonitorPath = cms.Path(
                process.preScaler +
                process.ecalPreRecoSequence +
                process.ecalRecoSequence +
                process.ecalMonitorTask
            )

            if live:
                process.ecalMonitorPath.insert(process.ecalMonitorPath.index(process.ecalRecoSequence), process.ecalPhysicsFilter)

            paths.append(process.ecalMonitorPath)

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

            paths.append(process.ecalLaserLedPath)
    
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

            paths.append(process.ecalTestPulsePath)
    
            process.ecalPedestalPath = cms.Path(
                process.ecalPreRecoSequence +
                process.ecalRecoSequence +
                process.ecalPedestalMonitorTask +
                process.ecalPNDiodeMonitorTask
            )

            if live:
                process.ecalPedestalPath.insert(1, process.ecalPedestalFilter)
                process.ecalPedestalPath.insert(0, process.preScaler)

            paths.append(process.ecalPedestalPath)

            if options.cfgType == 'CalibrationStandalone':
                process.ecalMonitorPath = cms.Path(
                    process.ecalPreRecoSequence +
                    process.ecalMonitorTask
                )

                paths.append(process.ecalMonitorPath)

        process.dqmEndPath = cms.EndPath(process.dqmEnv)

        endpaths.append(process.dqmEndPath)
            
    if isClient:
        if physics:
            process.ecalClientPath = cms.Path(
                process.ecalPreRecoSequence +
                process.ecalMonitorClient
            )

            if live:
                process.ecalClientPath.insert(1, process.ecalPhysicsFilter)
                process.ecalClientPath.insert(0, process.preScaler)

            paths.append(process.ecalClientPath)

#            process.dqmQTestPath = cms.EndPath(process.dqmQTest)

#            endpaths.append(process.dqmQTestPath)

        elif calib:    
            process.ecalClientPath = cms.Path(
                process.ecalCalibMonitorClient
            )

            paths.append(process.ecalClientPath)

        elif laser:
#            process.ecalMonitorPath = cms.Path(
#                process.ecalLaserMonitorClient
#            )
            pass

    if options.outputMode == 1:
        if not isSource and isClient:
            process.dqmOutputPath = cms.EndPath(process.ecalMEFormatter + process.dqmSaver)
        else:
            process.dqmOutputPath = cms.EndPath(process.dqmSaver)
            
        endpaths.append(process.dqmOutputPath)

    elif options.outputMode == 2:
        process.load("DQMServices.Components.MEtoEDMConverter_cfi")
        process.dqmEndPath.insert(1, process.MEtoEDMConverter)

        process.DQMoutput = cms.OutputModule("PoolOutputModule",
            splitLevel = cms.untracked.int32(0),
            outputCommands = cms.untracked.vstring("drop *", "keep *_MEtoEDMConverter_*_*"),
            fileName = cms.untracked.string(options.outputPath),
            dataset = cms.untracked.PSet(
                filterName = cms.untracked.string(''),
                dataTier = cms.untracked.string('')
            )
        )

        process.dqmOutputPath = cms.EndPath(process.DQMoutput)

        endpaths.append(process.dqmOutputPath)

    process.schedule = cms.Schedule(*(paths + endpaths))

    process.prune()
    
#def buildEcalDQMSequences

    
if __name__ == '__main__':
    import sys
    if 'FWCore.ParameterSet.Config' not in sys.modules: #standalone python
        # cmsRun executes import FWCore.ParameterSet.Config as cms before running the configuration file via execfile()
        # __name__ == '__main__' cannot distinguish the standalone python and cmsRun usages, because any file executed through execfile is a main

        class CustomDefAttr(object):
            def __init__(self, name, cmsObject):
                self._name = name
                self._cmsObject = cmsObject
                self._subattr = {}
                self._explicit = {} # objects set by actual assignment

            def generateAttr(self):
                output = []
                for name, value in self._explicit.items():
                    output.append([name, value])

                for value in self._subattr.values():
                    output += value.generateAttr()

                if len(self._explicit) == 0 and len(self._subattr) == 0:
                    output.append([self._cmsObject])

                for line in output:
                    line.insert(0, self._name)
                    
                return output

            def __getattr__(self, name):
                if name in self._explicit:
                    return self._explicit[name]

                if name not in self._subattr:
                    self._subattr[name] = CustomDefAttr(name, getattr(self._cmsObject, name))

                return self._subattr[name]

            def __setattr__(self, name, value):
                if name.startswith('_'):
                    object.__setattr__(self, name, value)
                else:
                    setattr(self._cmsObject, name, value)
                    self._explicit[name] = value

            def __call__(self, *args):
                return self._cmsObject(*args)
                

        class CfgGenerator(CustomDefAttr):
            def __init__(self, name):
                CustomDefAttr.__init__(self, name, cms.Process('DQM'))
                self._loadPaths = []
                self._lines = []

            def load(self, path):
                self._cmsObject.load(path)
                self._loadPaths.append(path)

            def generate(self):
                output = 'process = cms.Process("' + self._name + '")\n\n'

                output += '### Load cfis ###\n\n'

                for path in self._loadPaths:
                    output += 'process.load("' + path + '")\n'

                output += '\n'

                output += '### Individual module setups ###\n'

                topComponent = ''

                for line in self.generateAttr():
                    if callable(line[-1]):
                        continue

                    if topComponent != line[1]:
                        output += '\n'
                        topComponent = line[1]

                    output += '.'.join(line[0:-1]) + ' = '
        
                    if isinstance(line[-1], cms._ParameterTypeBase) or isinstance(line[-1], cms._ConfigureComponent):
                        dump = line[-1].dumpPython(cms.PrintOptions())
                        output += dump
                        if dump[-1] != '\n': output += '\n'
                    elif type(line[-1]) == str:
                        output += '"' + line[-1] + '"\n'
                    else:
                        output += str(line[-1]) + '\n'

                output += '\n'

                output += '### Sequences ###\n\n'

                for name, seq in self._cmsObject.sequences.items():
                    output += 'process.' + name + ' = ' + seq.dumpPython(cms.PrintOptions())

                output += '\n'

                if len(self._lines):
                    output += '### Customizations ###\n\n'
                    for line in self._lines:
                        output += line + '\n'

                output += '### Paths ###\n\n'

                paths = []

                for name, path in self._cmsObject.paths.items():
                    output += 'process.' + name + ' = ' + path.dumpPython(cms.PrintOptions())
                    paths.append('process.' + name)

                output += '\n'

                for name, path in self._cmsObject.endpaths.items():
                    output += 'process.' + name + ' = ' + path.dumpPython(cms.PrintOptions())
                    paths.append('process.' + name)

                output += '\n'

                output += '### Schedule ###\n\n'

                output += 'process.schedule = cms.Schedule(' + ','.join(paths) + ')\n'

                return output
                
        
        import FWCore.ParameterSet.Config as cms
        from optparse import OptionParser, OptionGroup
        
        # get options
        optparser = OptionParser()
    
        commonOpts = OptionGroup(optparser, "Common job configuration options", "Options passed to the job configuration")
        commonOpts.add_option("-e", "--env", dest = "environment", default = "LocalOffline", help = "ENV=(CMSLive|PrivLive|PrivOffline|LocalLive|LocalOffline)", metavar = "ENV")
        commonOpts.add_option("-c", "--cfg-type", dest = "cfgType", default = "Physics", help = "CONFIG=(Physics|Calibration|Laser)", metavar = "CFGTYPE")
        commonOpts.add_option("-s", "--steps", dest = "steps", default = "sourceclient", help = "STEPS=[source][client]", metavar = "STEPS")
        commonOpts.add_option("-i", "--input-files", dest = "inputFiles", default = "", help = "source file name (comma separated) or URL", metavar = "SOURCE")
        commonOpts.add_option("-I", "--input-list", dest = "inputList", default = "", help = "file containing list of sources", metavar = "FILE")
        commonOpts.add_option("-r", "--rawdata", dest = "rawDataCollection", default = "", help = "collection name", metavar = "RAWDATA")
        commonOpts.add_option("-g", "--global-tag", dest = "globalTag", default = "auto:com10", help = "global tag", metavar = "TAG")
        commonOpts.add_option("-O", "--output-mode", type = "int", dest = "outputMode", default = 1, help = "0: no output, 1: DQM output, 2: EDM output", metavar = "MODE")
        commonOpts.add_option("-w", "--workflow", dest = "workflow", default = "", help = "offline workflow", metavar = "WORKFLOW")
        commonOpts.add_option("-G", "--use-GED", dest = "useGEDClusters", action = "store_true", default = False, help = "switch for GED clusters")
        commonOpts.add_option("-C", "--collector", dest = "collector", default = "", help = "Collector configuration", metavar = "HOST:PORT")
        commonOpts.add_option("-o", "--output-path", dest = "outputPath", default = "", help = "DQMFileSaver output directory / PoolOutputModule output path", metavar = "DIR")
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

        sys.argv = sys.argv[:1]
    
        options.inputFiles = options.inputFiles.split(',')
        if not options.inputFiles[0]:
            options.inputFiles = []

        if 'Live' in options.environment:
            if len(options.inputFiles) < 2:
                options.inputFiles = ['', 0]

        if options.environment == 'CMSLive':
            options.laserWavelengths = [1, 2, 3]
            options.MGPAGains = [12]
            options.MGPAGainsPN = [16]
        else:
            options.laserWavelengths = map(int, options.laserWavelengths.split(','))
            options.MGPAGains = map(int, options.MGPAGains.split(','))
            options.MGPAGainsPN = map(int, options.MGPAGainsPN.split(','))

        if not options.rawDataCollection:
            if options.cfgType == 'Calibration':
                options.rawDataCollection = 'hltEcalCalibrationRaw'
            else:
                options.rawDataCollection = 'rawDataCollector'

        generator = CfgGenerator('process')

        buildEcalDQMModules(generator, options)

        buildEcalDQMSequences(generator._cmsObject, options)

        # write cfg file
        fileName = options.file
        if not fileName:
            if options.cfgType == 'Physics':
                c = 'ecal'
            elif options.cfgType == 'Calibration':
                c = 'ecalcalib'
            elif options.cfgType == 'CalibrationStandalone':
                c = 'ecalcalibstandalone'
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
    
        cfgfile.write("### AUTO-GENERATED CMSRUN CONFIGURATION FOR ECAL DQM ###\n")
        cfgfile.write('import FWCore.ParameterSet.Config as cms\n\n')
        if 'Live' not in options.environment:
            cfgfile.write("""
from FWCore.ParameterSet.VarParsing import VarParsing

options = VarParsing('analysis')
options.parseArguments()

""")

        cfgfile.write(generator.generate())
    
        if 'Live' in options.environment:
            if options.cfgType == 'Physics':
                cfgfile.write("""
### Run type specific ###

referenceFileName = process.DQMStore.referenceFileName.pythonValue()
runTypeName = process.runType.getRunTypeName()
if runTypeName == 'pp_run':
    process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_pp.root')
elif runTypeName == 'cosmic_run':
    process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_cosmic.root')
#    process.dqmEndPath.remove(process.dqmQTest)
    process.ecalMonitorTask.workers = ['EnergyTask', 'IntegrityTask', 'OccupancyTask', 'RawDataTask', 'TrigPrimTask', 'PresampleTask', 'SelectiveReadoutTask']
    process.ecalMonitorClient.workers = ['IntegrityClient', 'OccupancyClient', 'PresampleClient', 'RawDataClient', 'SelectiveReadoutClient', 'TrigPrimClient', 'SummaryClient']
    process.ecalMonitorClient.workerParameters.SummaryClient.params.activeSources = ['Integrity', 'RawData', 'Presample', 'TriggerPrimitives', 'HotCell']
elif runTypeName == 'hi_run':
    process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_hi.root')
    process.ecalMonitorTask.collectionTags.Source = "rawDataRepacker"
    process.ecalDigis.InputLabel = cms.InputTag('rawDataRepacker')
elif runTypeName == 'hpu_run':
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
        onlineSourceArgs = []
        for arg in sys.argv[2:]:
            if 'runNumber=' in arg or 'runInputDir' in arg or 'skipFirstLumi' in arg or 'runtype' in arg or 'runkey' in arg:
                onlineSourceArgs.append(arg)

        for arg in onlineSourceArgs:
            sys.argv.remove(arg)

        from FWCore.ParameterSet.VarParsing import VarParsing
    
        options = VarParsing("analysis")
        options._tags.pop('numEvent%d')
        options._tagOrder.remove('numEvent%d')
        
        options.register("environment", default = "LocalOffline", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "ENV=(CMSLive|PrivLive|PrivOffline|LocalLive|LocalOffline)")
        options.register("cfgType", default = "Physics", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "CONFIG=(Physics|Calibration|CalibrationStandalone|Laser)")
        options.register("steps", default = "sourceclient", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "DQM steps to perform")
        options.register('inputList', default = '', mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = 'File containing list of input files')
        options.register("rawDataCollection", default = "", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "collection name")
        options.register("globalTag", default = 'auto:com10', mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "GlobalTag")
        options.register("outputMode", default = 1, mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.int, info = "0: no output, 1: DQM output, 2: EDM output")
        options.register("outputPath", default = "", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "DQMFileSaver output directory / PoolOutputModule output path")
        options.register("workflow", default = "", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "offline workflow")
        options.register("useGEDClusters", default = False, mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.bool, info = 'switch for GED clusters')
        options.register("calibType", default = "", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = "ECAL run type")
        options.register("laserWavelengths", mult = VarParsing.multiplicity.list, mytype = VarParsing.varType.int, info = "Laser wavelengths")
        options.register("MGPAGains", mult = VarParsing.multiplicity.list, mytype = VarParsing.varType.int, info = "MGPA gains")
        options.register("MGPAGainsPN", mult = VarParsing.multiplicity.list, mytype = VarParsing.varType.int, info = "PN MGPA gains")
        options.register('prescaleFactor', default = 1, mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.int, info = 'Prescale factor')
        options.register('runkey', default = 'pp_run', mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = 'Run Keys of CMS')
        options.register('collector', default = "", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string, info = 'Collector configuration (host:port)')
        options.register('verbosity', default = 0, mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.int, info = 'ECAL DQM verbosity')

        options.parseArguments()

        if options.environment == 'CMSLive':
            for wl in [1, 2, 3]: options.laserWavelengths.append(wl)
            options.MGPAGains.append(12)
            options.MGPAGainsPN.append(16)
        else:
            for wl in [1, 2, 3, 4]: options.laserWavelengths.append(wl)
            for gain in [1, 6, 12]: options.MGPAGains.append(gain)
            for gain in [1, 16]: options.MGPAGainsPN.append(gain)

        sys.argv = sys.argv[:2]
        sys.argv += onlineSourceArgs

        print sys.argv

        if not options.rawDataCollection:
            if (options.environment == 'CMSLive' or options.environment == 'PrivLive') and options.cfgType == 'Calibration':
                options.rawDataCollection = 'hltEcalCalibrationRaw'
            else:
                options.rawDataCollection = 'rawDataCollector'

        process = cms.Process("DQM")

        buildEcalDQMModules(process, options)
        buildEcalDQMSequences(process, options)

        process.prune()

        if options.cfgType == 'Physics' and 'Live' in options.environment:
            from DQM.Integration.config.dqmPythonTypes import *
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
            elif runType.getRunType() == 'hi_run':
                process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_hi.root')
                process.ecalMonitorTask.collectionTags.Source = "rawDataRepacker"
                process.ecalDigis.InputLabel = cms.InputTag('rawDataRepacker')
            elif runType.getRunType() == 'hpu_run':
                process.DQMStore.referenceFileName = referenceFileName.replace('.root', '_hpu.root')
                process.source.SelectEvents = cms.untracked.PSet(SelectEvents = cms.vstring('*'))

            process.ecalMonitorTask.workerParameters.TimingTask.params.energyThresholdEB = 0.
            process.ecalMonitorTask.workerParameters.TimingTask.params.energyThresholdEE = 0.
            process.ecalMonitorTask.workerParameters.OccupancyTask.params.tpThreshold = 0.

        process.source.minEventsPerLumi = 100
        process.source.nextLumiTimeoutMillis = 3000

