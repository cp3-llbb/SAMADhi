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
    return 0.


class Event:
  pass

class Weight:
  pass

class MadWeightProcess:
  pass


