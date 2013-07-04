from storm.locals import *

#db store connection

def DbStore(login="llbb", password="ijvIg]Em0geqME", database="localhost/llbb"):
  """create a database object and returns the db store from STORM"""
  database = create_database("mysql://"+login+":"+password+"@"+database)
  return Store(database)

#definition of the DB interface classes 

class Dataset(Storm):
  """Table to represent one sample from DAS
     on which we run the analysis"""
  __storm_table__ = "dataset"
  dataset_id = Int(primary=True)
  name = Unicode()
  nevents = Int()
  dsize = Int()
  process = Unicode()
  xsection = Float()
  cmssw_release = Unicode()
  globaltag = Unicode()
  datatype = Unicode()
  user_comment = Unicode()
  energy = Float()
  creation_time = DateTime()
  samples = ReferenceSet(dataset_id,"Sample.source_dataset")
  
  def __init__(self, name, datatype):
    """Initialize a dataset by name and datatype.
       Other attributes may be null and should be set separately"""
    self.name = name
    if datatype==u"mc" or datatype==u"data":
      self.datatype = datatype
    else:
      raise ValueError('data type must be mc or data')

  def replaceBy(self, dataset):
    """Replace one entry, but keep the same key"""
    self.name = dataset.name
    self.nevents = dataset.nevents
    self.dsize = dataset.dsize
    self.process = dataset.process
    self.xsection = dataset.xsection
    self.cmssw_release = dataset.cmssw_release
    self.globaltag = dataset.globaltag
    self.datatype = dataset.datatype
    self.user_comment = dataset.user_comment
    self.energy = dataset.energy
    self.creation_time = dataset.creation_time
  
  def __str__(self):
    result  = "Dataset #%s:\n"%str(self.dataset_id)
    result += "  name: %s\n"%str(self.name)
    result += "  process: %s\n"%str(self.process)
    result += "  cross-section: %s\n"%str(self.xsection)
    result += "  number of events: %s\n"%str(self.nevents)
    result += "  size on disk: %s\n"%str(self.dsize)
    result += "  CMSSW release: %s\n"%str(self.cmssw_release)
    result += "  global tag: %s\n"%str(self.globaltag)
    result += "  type (data or mc): %s\n"%str(self.datatype)
    result += "  center-of-mass energy: %s\n"%str(self.energy)
    result += "  creation time (on DAS): %s\n"%str(self.creation_time)
    result += "  comment: %s\n"%str(self.user_comment)
    return result

class Sample(Storm):
  """Table to represent one processed sample,
     typically a PATtupe, skim, RDS, CP, etc."""
  __storm_table__ = "sample"
  sample_id = Int(primary=True)
  name = Unicode()
  path = Unicode()
  sampletype = Unicode()
  nevents_processed = Int()
  nevents = Int()
  normalization = Float()
  luminosity = Float()
  code_version = Unicode()
  user_comment = Unicode()
  source_dataset_id = Int()
  source_sample_id  = Int()
  source_dataset = Reference(source_dataset_id, "Dataset.dataset_id")
  source_sample = Reference(source_sample_id, "Sample.sample_id")
  derived_samples = ReferenceSet(sample_id,"Sample.source_sample") 
  results = ReferenceSet(sample_id,"SampleResult.sample_id","SampleResult.result_id","Result.result_id")

  SampleTypes = [ "PAT", "SKIM", "RDS", "LHCO", "NTUPLES", "HISTOS", "OTHER" ]
  
  def __init__(self, name, path, sampletype, nevents_processed):
    """Initialize a dataset by name and datatype.
       Other attributes may be null and should be set separately"""
    self.name = name
    self.path = path
    self.nevents_processed = nevents_processed
    if sampletype in self.SampleTypes:
      self.sampletype = sampletype
    else:
      raise ValueError('sample type %s is unkwown'%sampletype)

  def replaceBy(self, sample):
    """Replace one entry, but keep the same key"""
    self.name = sample.name
    self.path = sample.path
    self.sampletype = sample.sampletype
    self.nevents_processed = sample.nevents_processed
    self.nevents = sample.nevents
    self.normalization = sample.normalization
    self.luminosity = sample.luminosity
    self.code_version = sample.code_version
    self.user_comment = sample.user_comment
    self.source_dataset_id = sample.source_dataset_id
    self.source_sample_id = sample.source_sample_id

  def getLuminosity(self):
    """Computes the sample (effective) luminosity"""
    if self.luminosity is not None:
      return self.luminosity
    else:
      if self.source_dataset is not None:
        if self.source_dataset.datatype=="mc":
          # for MC, it can be computed as Nevt/xsection
          if self.nevents_processed is not None and self.source_dataset.xsection is not None:
            return self.nevents_processed/self.source_dataset.xsection
        else:
          # for DATA, it can only be obtained from the parent sample
          if self.source_sample is not None:
            return self.source_sample.luminosity()
    # in all other cases, it is impossible to compute a number.
    return None

  def __str__(self):
    result  = "Sample #%s:\n"%str(self.sample_id)
    result += "  name: %s\n"%str(self.name)
    result += "  path: %s\n"%str(self.path)
    result += "  type: %s\n"%str(self.sampletype)
    result += "  number of processed events: %s\n"%str(self.nevents_processed)
    result += "  number of events: %s\n"%str(self.nevents)
    result += "  normalization: %s\n"%str(self.normalization)
    result += "  (effective) luminosity: %s\n"%str(self.luminosity)
    result += "  code version: %s\n"%str(self.code_version)
    result += "  comment: %s\n"%str(self.user_comment)
    result += "  source dataset: %s\n"%str(self.source_dataset_id)
    result += "  source sample: %s\n"%str(self.source_sample_id)
    return result

class Result(Storm):
  """Table to represent one physics result,
     combining several samples."""
  __storm_table__ = "result"
  result_id = Int(primary=True)
  path = Unicode()
  description = Unicode()
  samples = ReferenceSet(result_id,"SampleResult.result_id","SampleResult.sample_id","Sample.sample_id")

  def __init__(self,path):
    self.path = path

  def replaceBy(self, result):
    """Replace one entry, but keep the same key"""
    self.path = result.path
    self.description = result.description

  def __str__(self):
    result  = "Result in %s\n"%str(self.path)
    result += str(self.description)
    return result

class SampleResult(Storm):
  """Many to many relationship between samples and results."""
  __storm_table__ = "sampleresult"
  __storm_primary__ = "sample_id", "result_id"
  sample_id = Int()
  result_id = Int()

class Event(Storm):
  """Class to represent one unique event"""
  __storm_table__ = "event"
  event_id = Int(primary=True)
  event_number = Int()
  run_number = Int()
  dataset_id = Int()
  dataset = Reference(dataset_id, "Dataset.dataset_id")
  weights = ReferenceSet(event_id,"Weight.event_id")

  def __init__(self,event,run,dataset):
    self.event_number = event
    self.run_number = run
    self.dataset_id = dataset

  def __str__(self):
    return "Event %d, Run %d, Dataset %d\n"%(self.event_number,self.run_number,self.dataset_id)

class MadWeight(Storm):
  """Description of one MadWeight setup,
     including the process but also the various options like ISR, 
     width of a resonance, etc."""
  __storm_table__ = "madweight"
  process_id = Int(primary=True)
  name = Unicode()
  diagram = Unicode()
  isr = Int()
  systematics = Unicode()
  ident_mw_card = Unicode()
  ident_card = Unicode()
  info_card = Unicode()
  MadWeight_card = Unicode()
  mapping_card = Unicode()
  param_card = Unicode()
  param_card_1 = Unicode()
  proc_card_mg5 = Unicode()
  run_card = Unicode()
  transfer_card = Unicode()

  def __init__(self,name):
    self.name = name
  
  def __str__(self):
    result  = "MadWeight configuration #%s\n"%str(self.process_id)
    result += "  name: %s\n"%str(self.name)
    result += "  diagram: %s\n"%str(self.diagram)
    result += "  ISR: %s\n"%str(self.isr)
    result += "  systematics: %s\n"%str(self.systematics)
    return result

  def replaceBy(self, config):
    """Replace one entry, but keep the same key"""
    self.name = config.name
    self.diagram = config.diagram
    self.isr = config.isr
    self.systematics = config.systematics
    self.ident_mw_card = config.ident_mw_card
    self.ident_card = config.ident_card
    self.info_card = config.info_card
    self.MadWeight_card = config.MadWeight_card
    self.mapping_card = config.mapping_card
    self.param_card = config.param_card
    self.param_card_1 = config.param_card_1
    self.proc_card_mg5 = config.proc_card_mg5
    self.run_card = config.run_card
    self.transfer_card = config.transfer_card

class Weight(Storm):
  """One weight. It relates one event and one MadWeight setup
     to one value + uncertainty"""
  __storm_table__ = "weight"
  weight_id = Int(primary=True)
  event_id = Int()
  madweight_process = Int()
  value = Float()
  uncertainty = Float()
  version = Int()
  event = Reference(event_id,"Event.event_id")
  process = Reference(madweight_process,"MadWeight.process_id")

