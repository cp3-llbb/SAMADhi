# Storms package
import sys
sys.path.append('/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/lib/python2.7/site-packages/storm-0.20-py2.7-linux-x86_64.egg')
sys.path.append('/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/lib/python2.7/site-packages/MySQL_python-1.2.3-py2.7-linux-x86_64.egg')

from storm.locals import *

#db store connection

def DbStore(credentials='~/.samadhi'):
    """create a database object and returns the db store from STORM"""

    import json, os, stat
    credentials = os.path.expanduser(credentials)
    if not os.path.exists(credentials):
        raise IOError('Credentials file %r not found.' % credentials)

    # Check permission
    mode = stat.S_IMODE(os.stat(credentials).st_mode)
    if mode != int('400', 8):
        raise IOError('Credentials file has wrong permission. Please execute \'chmod 400 %s\'' % credentials)

    with open(credentials, 'r') as f:
        data = json.load(f)

        login = data['login']
        password = data['password']
        hostname = data['hostname'] if 'hostname' in data else 'localhost'
        database = data['database']

        db_connection_string = "mysql://%s:%s@%s/%s" % (login, password, hostname, database)
        return Store(create_database(db_connection_string))

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
  samples = ReferenceSet(dataset_id,"Sample.source_dataset_id")
  
  def __init__(self, name, datatype):
    """Initialize a dataset by name and datatype.
       Other attributes may be null and should be set separately"""
    self.name = name
    if datatype==u"mc" or datatype==u"data":
      self.datatype = datatype
    else:
      raise ValueError('dataset type must be mc or data')

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
    result += "  center-of-mass energy: %s TeV\n"%str(self.energy)
    result += "  creation time (on DAS): %s\n"%str(self.creation_time)
    result += "  comment: %s"%str(self.user_comment)
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
  event_weight_sum = Float()
  extras_event_weight_sum = Unicode() #  MEDIUMTEXT in MySQL
  luminosity = Float()
  processed_lumi = Unicode() #  MEDIUMTEXT in MySQL
  code_version = Unicode()
  user_comment = Unicode()
  author = Unicode()
  creation_time = DateTime()
  source_dataset_id = Int()
  source_sample_id  = Int()
  source_dataset = Reference(source_dataset_id, "Dataset.dataset_id")
  source_sample = Reference(source_sample_id, "Sample.sample_id")
  derived_samples = ReferenceSet(sample_id,"Sample.source_sample_id") 
  results = ReferenceSet(sample_id,"SampleResult.sample_id","SampleResult.result_id","Result.result_id")
  files = ReferenceSet(sample_id, "File.sample_id")

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
    self.event_weight_sum = sample.event_weight_sum
    self.extras_event_weight_sum = sample.extras_event_weight_sum
    self.luminosity = sample.luminosity
    self.code_version = sample.code_version
    self.user_comment = sample.user_comment
    self.source_dataset_id = sample.source_dataset_id
    self.source_sample_id = sample.source_sample_id
    self.author = sample.author
    self.creation_time = sample.creation_time

  def removeFiles(self, store):
    store.find(File, File.sample_id == self.sample_id).remove()
    self.files.clear()


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
    result  = "Sample #%s (created on %s by %s):\n"%(str(self.sample_id),str(self.creation_time),str(self.author))
    result += "  name: %s\n"%str(self.name)
    result += "  path: %s\n"%str(self.path)
    result += "  type: %s\n"%str(self.sampletype)
    result += "  number of processed events: %s\n"%str(self.nevents_processed)
    result += "  number of events: %s\n"%str(self.nevents)
    result += "  normalization: %s\n"%str(self.normalization)
    result += "  sum of event weight: %s\n"%str(self.event_weight_sum)
    if self.extras_event_weight_sum:
        result += "  has extras sum of event weight\n"
    result += "  (effective) luminosity: %s\n"%str(self.luminosity)
    if self.processed_lumi:
        result += "  has processed luminosity sections information\n"
    else:
        result += "  does not have processed luminosity sections information\n"
    result += "  code version: %s\n"%str(self.code_version)
    result += "  comment: %s\n"%str(self.user_comment)
    result += "  source dataset: %s\n"%str(self.source_dataset_id)
    result += "  source sample: %s\n"%str(self.source_sample_id)
    if self.sample_id:
        result += "  %d files: \n" % (self.files.count())
        front_files = []
        last_file = None
        if self.files.count() > 5:
            c = 0
            for f in self.files:
                if c < 3:
                    front_files.append(f)

                if c == self.files.count() - 1:
                    last_file = f
                c += 1
        else:
            front_files = self.files

        for f in front_files:
            result += "    - %s (%d entries)\n" % (str(f.lfn), f.nevents)
        if last_file:
            result += "    - ...\n"
            result += "    - %s (%d entries)\n" % (str(last_file.lfn), last_file.nevents)
    else:
        # No way to know if some files are here
        result += "  no files"

    return result

class Result(Storm):
  """Table to represent one physics result,
     combining several samples."""
  __storm_table__ = "result"
  result_id = Int(primary=True)
  path = Unicode()
  description = Unicode()
  author = Unicode()
  creation_time = DateTime()
  analysis_id = Int()
  analysis = Reference(analysis_id, "Analysis.analysis_id")
  elog = Unicode()
  samples = ReferenceSet(result_id,"SampleResult.result_id","SampleResult.sample_id","Sample.sample_id")

  def __init__(self,path):
    self.path = path

  def replaceBy(self, result):
    """Replace one entry, but keep the same key"""
    self.path = result.path
    self.description = result.description
    self.author = result.author
    self.analysis_id = result.analysis_id
    self.elog = result.elog

  def __str__(self):
    result  = "Result in %s \n  created on %s by %s\n  "%(str(self.path),str(self.creation_time),str(self.author))
    result += "%s"%str(self.description)
    if self.analysis is not None:
        result += "\n  part of analysis %s"%str(self.analysis.description)
    if self.elog is not None:
        result += "\n  more details in %s"%str(self.elog)
    return result

class SampleResult(Storm):
  """Many to many relationship between samples and results."""
  __storm_table__ = "sampleresult"
  __storm_primary__ = "sample_id", "result_id"
  sample_id = Int()
  result_id = Int()

class File(Storm):
    __storm_table__ = "file"
    id = Int(primary=True)
    sample_id = Int()
    lfn = Unicode()  # Local file name: /store/
    pfn = Unicode()  # Physical file name: srm:// or root://
    event_weight_sum = Float()
    extras_event_weight_sum = Unicode() #  MEDIUMTEXT in MySQL
    nevents = Int()

    sample = Reference(sample_id, "Sample.sample_id")

    def __init__(self, lfn, pfn, event_weight_sum, extras_event_weight_sum, nevents):
        self.lfn = lfn
        self.pfn = pfn
        self.event_weight_sum = event_weight_sum
        self.extras_event_weight_sum = extras_event_weight_sum
        self.nevents = nevents

    def __str__(self):
        return "%s"%(self.lfn)

class Analysis(Storm):
    __storm_table__ = "analysis"
    analysis_id = Int(primary=True)
    description = Unicode()
    cadiline = Unicode()
    contact = Unicode()
    results = ReferenceSet(analysis_id, "Result.analysis_id")

    def __init__(self,description):
        self.description = description

    def replaceBy(self, analysis):
        self.description = analysis.description
        self.cadiline = analysis.cadiline
        self.contact = analysis.contact
    
    def __str__(self):
        result = "%s\n"%self.description
        if self.cadiline is not None:
            result += "  CADI line: %s\n"%self.cadiline
        if self.contact is not None:
            result += "  Contact/Promotor: %s\n"%self.contact
        result += "  Number of associated results: %d"%self.results.count()
        return result

