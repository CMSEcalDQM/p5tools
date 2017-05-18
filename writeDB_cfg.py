import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing

import os

from ecaldqmconfig import config

options = VarParsing('analysis')
options.register('generalTag', default = 'GLOBAL', mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string)
options.register("laserWavelengths", mult = VarParsing.multiplicity.list, mytype = VarParsing.varType.int, info = "Laser wavelengths")
options.register("ledWavelengths", mult = VarParsing.multiplicity.list, mytype = VarParsing.varType.int, info = "LED wavelengths")
options.register("MGPAGains", mult = VarParsing.multiplicity.list, mytype = VarParsing.varType.int, info = "MGPA gains")
options.register("MGPAGainsPN", mult = VarParsing.multiplicity.list, mytype = VarParsing.varType.int, info = "PN MGPA gains")
options.register("runType",default = "COSMIC", mult = VarParsing.multiplicity.singleton, mytype = VarParsing.varType.string)
options.parseArguments()

os.environ["TNS_ADMIN"] = "/etc"

process =  cms.Process("DQMDB")

process.source = cms.Source("EmptySource")

process.load("DQM.EcalMonitorDbModule.EcalCondDBWriter_cfi")
process.ecalCondDBWriter.DBName = config.dbwrite.dbName
process.ecalCondDBWriter.userName = config.dbwrite.dbUserName
process.ecalCondDBWriter.password = config.dbwrite.dbPassword
process.ecalCondDBWriter.hostName = config.dbwrite.dbHostName
process.ecalCondDBWriter.hostPort = int(config.dbwrite.dbHostPort)
process.ecalCondDBWriter.location = 'P5_Co'
process.ecalCondDBWriter.runType = options.runType 
#process.ecalCondDBWriter.runType = "COSMIC"
#process.ecalCondDBWriter.runType = "LASER"
#process.ecalCondDBWriter.runType = "TEST_PULSE"
#process.ecalCondDBWriter.runType = "PHYSICS"
#process.ecalCondDBWriter.runType = "PEDESTAL"
process.ecalCondDBWriter.runGeneralTag = options.generalTag
process.ecalCondDBWriter.monRunGeneralTag = 'CMSSW-offline-private'
process.ecalCondDBWriter.inputRootFiles = cms.untracked.vstring(options.inputFiles)
if len(options.laserWavelengths):
    process.ecalCondDBWriter.workerParams.laserWavelengths = options.laserWavelengths
if len(options.ledWavelengths):
    process.ecalCondDBWriter.workerParams.ledWavelengths = options.ledWavelegths
if len(options.MGPAGains):
    process.ecalCondDBWriter.workerParams.MGPAGains = options.MGPAGains
if len(options.MGPAGainsPN):
    process.ecalCondDBWriter.workerParams.MGPAGainsPN = options.MGPAGainsPN
process.ecalCondDBWriter.verbosity = 2

process.load("DQMServices.Core.DQM_cfg")

process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(1)
)

process.load("Configuration.StandardSequences.GeometryRecoDB_cff")
#process.load("Geometry.CaloEventSetup.CaloGeometry_cfi")
#process.load("Geometry.CaloEventSetup.CaloTopology_cfi")
#process.load("Geometry.CaloEventSetup.EcalTrigTowerConstituents_cfi")
#process.load("Geometry.CMSCommonData.cmsIdealGeometryXML_cfi")
#process.load("Geometry.EcalMapping.EcalMapping_cfi")
#process.load("Geometry.EcalMapping.EcalMappingRecord_cfi")

process.load("DQM.Integration.config.FrontierCondition_GT_cfi")
process.GlobalTag.connect = cms.string("frontier://(proxyurl=http://localhost:3128)(serverurl=http://localhost:8000/FrontierProd)(serverurl=http://localhost:8000/FrontierProd)(retrieve-ziplevel=0)(failovertoserver=no)/CMS_CONDITIONS")

process.MessageLogger = cms.Service('MessageLogger',
    destinations = cms.untracked.vstring('cout'),
    categories = cms.untracked.vstring('EcalDQM'),
    cout = cms.untracked.PSet(
        threshold = cms.untracked.string('INFO'),
        EcalDQM = cms.untracked.PSet(
            limit = cms.untracked.int32(-1)
        ),
        default = cms.untracked.PSet(
            limit = cms.untracked.int32(0)
        )
    )
)
process.p = cms.Path(process.ecalCondDBWriter)
