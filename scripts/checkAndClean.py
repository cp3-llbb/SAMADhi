#!/usr/bin/env python
import json
import os
import sys
from cp3_llbb.SAMADhi.SAMADhi import Analysis, Dataset, Sample, Result, DbStore
from cp3_llbb.SAMADhi.SAMADhi import File as SFile
from optparse import OptionParser, OptionGroup

class MyOptionParser:
    """
    Client option parser
    """
    def __init__(self):
        usage  = "Usage: %prog [options]\n"
        self.parser = OptionParser(usage=usage)
        self.parser.add_option("-p","--path", action="store", type="string",
                               dest="path", default="./",
             help="Path to the json files with db analysis results.")
        self.parser.add_option("-o","--output", action="store", type="string",
                               dest="output", default="-",
             help="Name of the output file.")
        self.parser.add_option("-M","--cleanupMissing", action="store_true",
                               dest="cleanupMissing", default=False,
             help="Clean samples with missing path from the database.")
        self.parser.add_option("-U","--cleanupUnreachable", action="store_true",
                               dest="cleanupUnreachable", default=False,
             help="Clean samples with unreachable path from the database.")
        self.parser.add_option("-D","--cleanupDatasets", action="store_true",
                               dest="cleanupDatasets", default=False,
             help="Clean orphan datasets from the database.")
        self.parser.add_option("-w","--whitelist", action="store", type="string",
                               dest="whitelist", default=None,
             help="JSON file with sample whitelists per analysis.")
        self.parser.add_option("-d","--dry-run", action="store_true",
                               dest="dryrun", default=False,
             help="Dry run: do not write to file and/or touch the database.")

    def get_opt(self):
        """
        Returns parse list of options
        """
        opts, args = self.parser.parse_args()
        if opts.path is not None:
          opts.path = os.path.abspath(os.path.expandvars(os.path.expanduser(opts.path)))
        if opts.output == "-":
          opts.output = sys.__stdout__
        else:
          filepath = os.path.dirname(os.path.realpath(os.path.expanduser(opts.output)))
          if not os.access(filepath,os.W_OK):
            self.parser.error("Cannot write to %s"%filepath)
          if os.path.isfile(opts.output):
            self.parser.error("File already exists: %s"%opts.output) 
          if not opts.dryrun:
            try: 
              opts.output = open(opts.output,"w")
            except:
              self.parser.error("Cannot write to %s"%opts.output)
          else:
            opts.output = sys.__stdout__
        try:
          opts.whitelist = open(opts.whitelist)
        except:
          self.parser.error("Cannot open whitelist.")
        return opts

class StoreCleaner():
  """
  handle to the db store, with basic facilities to cleanup entries
  """

  def __init__(self):
    self.dbstore = DbStore()

  def deleteSample(self,sample_id):
     store = self.dbstore
     # first remove the files associated with the sample
     files = store.find(SFile,SFile.sample_id==sample_id)
     for sampleFile in files:
       store.remove(sampleFile)
     # then remove the sample
     sample = store.find(Sample,Sample.sample_id==sample_id).one()
     print("deleting sample %d"%sample_id)
     store.remove(sample)

  def deleteDataset(self,dataset_id):
     store = self.dbstore
     # simply delete the dataset
     dataset = store.find(Dataset,Dataset.dataset_id==dataset_id).one()
     print("deleting dataset %d"%dataset_id)
     store.remove(dataset)

  def commit(self):
     self.dbstore.commit()

  def rollback(self):
     self.dbstore.rollback()

 
# Script to check samples for deletion

def main():
  """Main function"""
  # get the options
  optmgr = MyOptionParser()
  opts = optmgr.get_opt()

  # set stdout
  sys.stdout = opts.output

  # whitelist with samples that we should not touch ever
  if opts.whitelist is not None:
    whitelist = json.load(opts.whitelist)
  else: 
    whitelist = {}

  # utility class to clean the db
  myCleaner = StoreCleaner()

  # open the sample analysis report and classify bad samples
  samplesAnalysisReport = os.path.join(opts.path, "SamplesAnalysisReport.json")
  with open(samplesAnalysisReport) as jfile:
    data = json.load(jfile)
  samples = data["MissingDirSamples"]
  investigate = []
  delete = []
  empty = []
  empty_delete = []
  for sample in samples:
    whitelisted = False
    for v in whitelist.values():
      for label in v:
        if label in sample["name"]:
          whitelisted = True
    if whitelisted:
      if sample["path"]=="":
           empty.append(sample)
      else:
         investigate.append(sample)
    else:
      if sample["path"]=="":
           empty_delete.append(sample)
      else:
         delete.append(sample)
  print("\n\nWhitelisted sample with missing path. Investigate:")
  for sample in empty:
    print(sample["name"])
  print("\n\nWhitelisted sample with unreachable path. Investigate:")
  for sample in investigate:
    print(sample["name"])
  print("\n\nSamples to be deleted because of missing path:")
  for sample in empty_delete:
    print(sample["name"])
    if opts.cleanupMissing : myCleaner.deleteSample(sample["sample_id"])
  print("\n\nSamples to be deleted because of unreachable path:")
  for sample in delete:
    print(sample["name"])
    if opts.cleanupUnreachable : myCleaner.deleteSample(sample["sample_id"])

  # now clean orphan datasets
  datasetsAnalysisReport = os.path.join(opts.path, "DatasetsAnalysisReport.json")
  with open(datasetsAnalysisReport) as jfile:
    data = json.load(jfile)
  datasets = data["Orphans"]
  for dataset in datasets:
    if opts.cleanupDatasets : myCleaner.deleteDataset(dataset["dataset_id"])

  # and commit
  if not opts.dryrun:
    myCleaner.commit()
  else:
    myCleaner.rollback()

#
# main
#
if __name__ == '__main__':
    main()

