from storm.locals import *

#definition of the DB interface classes 

class Dataset(Storm):
  """Table to represent one sample from DAS
     on which we run the analysis"""
  __storm_table__ = "dataset"
  name = Unicode(primary=True)
  nevents = Int()
  dsize = Int()
  process = Unicode()
  xsection = Float()
  cmssw_release = Unicode()
  globaltag = Unicode()
  datatype = Unicode()
  user_comment = Unicode()
  energy = Float()
  
  def __init__(self, name, datatype):
    """Initialize a dataset by name and datatype.
       Other attributes may be null and should be set separately"""
    self.name = name
    if datatype==u"mc" or datatype==u"data":
      self.datatype = datatype
    else:
      raise ValueError('data type must be mc or data')
   

class ProcessedSample:
  pass

class DerivedSample:
  pass

class Event:
  pass

class Weight:
  pass

class MadWeightProcess:
  pass


