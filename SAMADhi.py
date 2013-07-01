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

  SampleTypes = [ "PAT", "SKIM", "RDS", "NTUPLES", "HISTOS", "OTHER" ]
  
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

class Result:
  """Table to represent one physics result,
     combining several samples."""
  __storm_table__ = "result"
  result_id = Int(primary=True)
  path = Unicode()
  description = Unicode()
  samples = ReferenceSet(result_id,"SampleResult.result_id","SampleResult.sample_id","Sample.sample_id")
  def __init__(self,path,description):
    self.path = path
    self.description = description
  def __str__(self):
    result  = "Result in %s\n"%path
    result += description
    return result

class SampleResult:
  """Many to many relationship between samples and results."""
  __storm_table__ = "sampleresult"
  __storm_primary__ = "sample_id", "result_id"
  sample_id = Int()
  result_id = Int()

class Event:
  """Class to represent one unique event"""
  __storm_table__ = "event"
  event_id = Int(primary=True)
  event_number = Int()
  run_number = Int()
  dataset_id = Int()
  dataset = Reference(dataset_id, "Dataset.dataset_id")
  weights = ReferenceSet(event_id,"Weight.weight_id")
  def __init__(self,event,run,dataset):
    self.event_number = event
    self.run_number = run
    self.dataset_id = dataset

class MadWeight:
  """Description of one MadWeight setup,
     including the process but also the various options like ISR, 
     width of a resonance, etc."""
  __storm_table__ = "madweight"
  process_id = Int(primary=True)
  name = Unicode()
  diagram = Unicode()
  isr = Int()
  systematics = Unicode()
  card = Unicode()

class Weight:
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

