#!/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/bin/python

# Script to add a sample to the database

import os
from optparse import OptionParser
from cp3_llbb.SAMADhi.SAMADhi import Dataset, Sample, Event, MadWeight, MadWeightRun, Weight, DbStore
from cp3_llbb.SAMADhi.userPrompt import confirm

class MyOptionParser: 
    """
    Client option parser
    """
    def __init__(self):
        #TODO: we could allow to guess the process and lhco by name or path as well
        usage  = "Usage: %prog lhco_id process file [options]\n"
        usage += "  where lhco_id is the sample id of the LHCO,\n"
        usage += "        process is the id of the MadWeight process,\n"
        usage += "        and file is the output file containing the weights."
        self.parser = OptionParser(usage=usage)
        self.parser.add_option("-v", "--version", action="store", type="int", 
                               default=None, dest="version",
             help="version of that particular weight")
        self.parser.add_option("-s", "--syst", action="store", type="string",
                               default="", dest="syst",
             help="string identifying the systematics variation of the weight")
        self.parser.add_option("-c", "--comment", action="store", type="string",
                               default="", dest="comment",
             help="user comment")

    def get_opt(self):
        """
        Returns parse list of options
        """
        opts, args = self.parser.parse_args()
        if len(args) < 3:
          self.parser.error("lhco, process and file are mandatory")
        opts.lhco_id = int(args[0])
        opts.process = int(args[1])
        opts.filepath = args[2]
        if not os.path.exists(opts.filepath) or not os.path.isfile(opts.filepath):
          self.parser.error("%s is not an existing file"%opts.filepath)
        return opts

def findDataset(sample):
  if sample.source_dataset_id is not None:
    return sample.source_dataset_id
  elif sample.source_sample_id is not None and sample.source_sample_id != sample.sample_id:
    return findDataset(sample.source_sample)
  else:
    return None

def main():
    """Main function"""
    # get the options
    optmgr = MyOptionParser()
    opts   = optmgr.get_opt()
    # connect to the MySQL database using default credentials
    dbstore = DbStore()
    # check that the LHCO exists and obtain the dataset id
    check = dbstore.find(Sample,Sample.sample_id==opts.lhco_id)
    if check.is_empty() or check.one().sampletype != "LHCO":
      raise IndexError("No LHCO with such index: %d"%opts.lhco_id)
    opts.dataset = findDataset(check.one())
    if opts.dataset is None:
      raise RuntimeError("Impossible to get the dataset id.")
    # check that the process exists
    check = dbstore.find(MadWeight,MadWeight.process_id==opts.process)
    if check.is_empty():
      raise IndexError("No process with such index: %d"%opts.process)
    # create the MW run object
    mw_run = MadWeightRun(opts.process,opts.lhco_id)
    mw_run.systematics = unicode(opts.syst)
    mw_run.user_comment = unicode(opts.comment)
    mw_run.version = opts.version
    if mw_run.version is None:
      check = dbstore.find(MadWeightRun,(MadWeightRun.madweight_process==mw_run.madweight_process) & (MadWeightRun.lhco_sample_id==mw_run.lhco_sample_id))
      if not check.is_empty():
        mw_run.version = check.order_by(MadWeightRun.version).last().version + 1
      else:
        mw_run.version = 1
    else:
      check = dbstore.find(MadWeightRun,(MadWeightRun.madweight_process==mw_run.madweight_process) & (MadWeightRun.lhco_sample_id==mw_run.lhco_sample_id) & (MadWeightRun.version==mw_run.version))
      if not check.is_empty():
        raise RuntimeError("There is already one such MadWeight run with the same version number:\n%s\n"%str(check.one()))
    # read the file
    inputfile = open(opts.filepath)
    count = 0
    for line in inputfile:
      data = line.rstrip('\n').split('\t')
      # get the event
      run_number = int(data[0].split('.')[0])
      event_number = int(data[0].split('.')[1])
      event_query = dbstore.find(Event, (Event.event_number==event_number) & (Event.run_number==run_number) & (Event.dataset_id==opts.dataset))
      if event_query.is_empty():
        event = Event(event_number,run_number,opts.dataset)
      else:
        event = event_query.one()
      # create the weight
      weight = Weight()
      weight.event = event
      weight.mw_run = mw_run
      weight.value = float(data[1])
      weight.uncertainty = float(data[2])
      dbstore.add(weight)
      count += 1
    # confirm and commit
    print mw_run
    print "Adding weights to %d events."%count
    if confirm(prompt="Insert into the database?", resp=True):
      dbstore.commit()

#
# main
#
if __name__ == '__main__':
    main()
