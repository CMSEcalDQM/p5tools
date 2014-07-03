import FWCore.ParameterSet.Config as cms
import sys
from FWCore.ParameterSet.VarParsing import VarParsing

options = VarParsing('analysis')
options.register('mode',
                 'detect',
                 VarParsing.multiplicity.singleton,
                 VarParsing.varType.string,
                 "Run mode")
options.register('source',
                 '',
                 VarParsing.multiplicity.singleton,
                 VarParsing.varType.string,
                 "Source to listen to"
                 )

options.parseArguments()

if not options.source:
  sys.exit(1)

detectMode = True
waitMode = False
if options.mode.strip() == 'wait' :
    detectMode = False
    waitMode = True

process = cms.Process("DQM")

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

process.runDetector = cms.EDAnalyzer("RunDetector",
    FEDRawDataCollectionTag = cms.untracked.InputTag("hltEcalCalibrationRaw")
)

process.load("DQM.Integration.test.inputsource_cfi")
process.source.consumerName = cms.untracked.string("Ecal DQM Probe")
process.source.SelectHLTOutput = cms.untracked.string("hltOutputCalibration")
if options.source == 'central':
  process.source.sourceURL = cms.string("http://dqm-c2d07-30.cms:22100/urn:xdaq-application:lid=30")
elif options.source == 'minidaq':
  process.source.sourceURL = cms.string('http://cmsdisk1.cms:15100/urn:xdaq-application:lid=50')
elif options.source == 'playback':
  process.source.sourceURL = cms.string('http://dqm-c2d07-30.cms:50082/urn:xdaq-application:lid=29')

process.preScaler = cms.EDFilter("Prescaler",
    prescaleFactor = cms.int32(1000),
    prescaleOffset = cms.int32(1)
)

process.p = cms.Path(
    process.preScaler +
    process.runDetector
)

# The following block must come after inputsource_cfi!!
if detectMode :
    process.maxEvents.input = cms.untracked.int32(1)
