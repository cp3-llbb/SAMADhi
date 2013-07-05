#!/usr/bin/env python

# Script to add a sample to the database

import os
from optparse import OptionParser
from SAMADhi import Dataset, Event, MadWeight, Weight, DbStore
from userPrompt import confirm

class MyOptionParser: 
    """
    Client option parser
    """
    def __init__(self):
        usage  = "Usage: %prog dataset process file [options]\n"
        usage += "  where dataset is the id of the dataset containing the events,\n"
        usage += "        process is the id of the MadWeight process,\n"
        usage += "        and file is the output file containing the weights."
        self.parser = OptionParser(usage=usage)
        self.parser.add_option("-v", "--version", action="store", type="int", 
                               default=None, dest="version",
             help="version of that particular weight")

    def get_opt(self):
        """
        Returns parse list of options
        """
        opts, args = self.parser.parse_args()
        if len(args) < 3:
          self.parser.error("sample process and file are mandatory")
        opts.dataset = int(args[0])
        opts.process = int(args[1])
        opts.filepath = args[2]
        if not os.path.exists(opts.filepath) or not os.path.isfile(opts.filepath):
          self.parser.error("%s is not an existing file"%opts.filepath)
        return opts

def main():
    """Main function"""
    # get the options
    optmgr = MyOptionParser()
    opts   = optmgr.get_opt()
    # connect to the MySQL database using default credentials
    dbstore = DbStore()
    # check that the dataset exists
    check = dbstore.find(Dataset,Dataset.dataset_id==opts.dataset)
    if check.is_empty():
      raise IndexError("No dataset with such index: %d"%opts.dataset)
    # check that the process exists
    check = dbstore.find(MadWeight,MadWeight.process_id==opts.process)
    if check.is_empty():
      raise IndexError("No process with such index: %d"%opts.process)
    # read the file
    inputfile = open(opts.filepath)
    versions = set()
    count = 0
    for line in inputfile:
      data = line.rstrip('\n').split('\t')
      # get the event
      run_number = int(data[0].split('.')[0])
      event_number = int(data[0].split('.')[1])
      event_query = dbstore.find(Event, (Event.event_number==event_number) & (Event.run_number==run_number) & (Event.dataset_id==opts.dataset))
      if event_query.is_empty():
        event = Event(event_number,run_number,opts.dataset)
        if opts.version is None: opts.version = 1
      else:
        event = event_query.one()
        # in that case, make sure there is no similar (process + version) weight already
        if opts.version is None:
          check = event.weights.find(Weight.madweight_process==opts.process).order_by(Weight.version)
          if check.is_empty():
            opts.version = 1
          else:
            lastver = check.last().version
            opts.version = lastver+1
        else:
          check = event.weights.find((Weight.madweight_process==opts.process) & (Weight.version==opts.version))
          if not check.is_empty():
            raise ValueError("There is already a weight for process %d with version %d"%(opts.process,opts.version))
      # create the weight
      weight = Weight()
      weight.event = event
      weight.madweight_process = opts.process
      weight.value = float(data[1])
      weight.uncertainty = float(data[2])
      weight.version = opts.version
      dbstore.add(weight)
      versions.add(opts.version)
      count += 1
    # confirm and commit
    print "Adding weights to %d events with the following version(s) (should be unique):"%count
    print versions
    if confirm(prompt="Insert into the database?", resp=True):
      dbstore.commit()

#
# main
#
if __name__ == '__main__':
    main()
