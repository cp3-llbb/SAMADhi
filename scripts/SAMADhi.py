#!/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/bin/python
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
  samples = ReferenceSet(result_id,"SampleResult.result_id","SampleResult.sample_id","Sample.sample_id")

  def __init__(self,path):
    self.path = path

  def replaceBy(self, result):
    """Replace one entry, but keep the same key"""
    self.path = result.path
    self.description = result.description

  def __str__(self):
    result  = "Result in %s \n  created on %s by %s\n  "%(str(self.path),str(self.creation_time),str(self.author))
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
    return "Event %d, Run %d, Dataset %d"%(self.event_number,self.run_number,self.dataset_id)

class MadWeight(Storm):
  """Description of one MadWeight setup,
     including the process but also the various options like ISR, 
     width of a resonance, etc."""
  __storm_table__ = "madweight"
  process_id = Int(primary=True)
  name = Unicode()
  diagram = Unicode()
  isr = Int()
  nwa = Int()
  cm_energy = Float()
  higgs_width = Float()
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
  transfer_fctVersion = Unicode()
  transfer_function = Unicode()

  def __init__(self,name):
    self.name = name
  
  def __str__(self):
    result  = "MadWeight configuration #%s\n"%str(self.process_id)
    result += "  name: %s\n"%str(self.name)
    result += "  diagram: %s\n"%str(self.diagram)
    result += "  ISR: %s\n"%str(self.isr)
    result += "  NWA: %s\n"%str(self.nwa)
    result += "  Center of mass energy: %s\n"%str(self.cm_energy)
    result += "  Transfert functions: %s\n"%str(self.transfer_fctVersion)
    result += "  Higgs Width: %s\n"%str(self.higgs_width)
    return result

  def replaceBy(self, config):
    """Replace one entry, but keep the same key"""
    self.name = config.name
    self.diagram = config.diagram
    self.isr = config.isr
    self.nwa = config.nwa
    self.higgs_width = config.higgs_width
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
    self.cm_energy = config.cm_energy
    self.transfer_fctVersion = config.transfer_fctVersion
    self.transfer_function = config.transfer_function

class MadWeightRun(Storm):
  """One run of MadWeight. It relates a MW setup to a LHCO file
     and may contain a systematics flag + comment."""
  __storm_table__ = "madweightrun"
  mwrun_id = Int(primary=True)
  madweight_process = Int()
  lhco_sample_id = Int()
  creation_time = DateTime()
  systematics = Unicode()
  user_comment = Unicode()
  version = Int()
  process = Reference(madweight_process,"MadWeight.process_id")
  lhco_sample = Reference(lhco_sample_id, "Sample.sample_id")

  def __init__(self,madweight_process,lhco_sample_id):
    self.madweight_process = madweight_process
    self.lhco_sample_id = lhco_sample_id

  def __str__(self):
    result  = "MadWeight run #%s performed on %s\n"%(str(self.mwrun_id),str(self.creation_time))
    result += "  MadWeight process: %s (id %s)\n"%(str(self.process.name),str(self.madweight_process))
    result += "  LHCO sample: %s (id %s)\n"%(str(self.lhco_sample.name),str(self.lhco_sample_id))
    result += "  Systematics: %s\n"%str(self.systematics)
    result += "  Comment: %s\n"%str(self.user_comment)
    result += "  Version: %s\n"%str(self.version)
    return result

class Weight(Storm):
  """One weight. It relates one event and one MadWeight setup
     to one value + uncertainty"""
  __storm_table__ = "weight"
  weight_id = Int(primary=True)
  event_id = Int()
  madweight_run = Int()
  value = Float()
  uncertainty = Float()
  event = Reference(event_id,"Event.event_id")
  mw_run = Reference(madweight_run,"MadWeightRun.mwrun_id")

  def __str__(self):
    return "%f +/- %f"%(self.value,self.uncertainty)

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
