import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing
import os

if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ecaldqmconfig import config

options = VarParsing('analysis')
options.parseArguments()

os.environ["TNS_ADMIN"] = "/etc"

process =  cms.Process("DQMDB")

process.source = cms.Source("EmptySource")

process.load("DQM.EcalCommon.EcalCondDBWriter_cfi")
process.ecalCondDBWriter.DBName = cond.dbwrite.name
process.ecalCondDBWriter.userName = cond.dbwrite.user
process.ecalCondDBWriter.password = cond.dbwrite.password
process.ecalCondDBWriter.hostName = 'cms_tstore.cern.ch'
process.ecalCondDBWriter.hostPort = 10121
process.ecalCondDBWriter.location = 'P5_Co'
process.ecalCondDBWriter.runType = "PHYSICS"
process.ecalCondDBWriter.runGeneralTag = "GLOBAL"
process.ecalCondDBWriter.monRunGeneralTag = 'CMSSW-offline-private'
process.ecalCondDBWriter.inputRootFiles = cms.untracked.vstring(options.inputFiles)
process.ecalCondDBWriter.workerParams.laserWavelengths = [1, 2, 3, 4]
process.ecalCondDBWriter.workerParams.MGPAGains = [1, 6, 12]
process.ecalCondDBWriter.workerParams.MGPAGainsPN = [1, 16]
process.ecalCondDBWriter.verbosity = 2

process.load("DQM.EcalCommon.EcalDQMBinningService_cfi")

process.load("DQMServices.Core.DQM_cfg")

process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(1)
)

process.load("Geometry.CaloEventSetup.CaloGeometry_cfi")

process.load("Geometry.CaloEventSetup.CaloTopology_cfi")

process.load("Geometry.CaloEventSetup.EcalTrigTowerConstituents_cfi")

process.load("Geometry.CMSCommonData.cmsIdealGeometryXML_cfi")

process.load("Geometry.EcalMapping.EcalMapping_cfi")

process.load("Geometry.EcalMapping.EcalMappingRecord_cfi")

process.load("DQM.Integration.test.FrontierCondition_GT_cfi")

process.p = cms.Path(process.ecalCondDBWriter)
