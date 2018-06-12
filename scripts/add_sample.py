#!/usr/bin/env python

# Script to add a sample to the database

import os
import glob
from pwd import getpwuid
from optparse import OptionParser
from datetime import datetime
from cp3_llbb.SAMADhi.SAMADhi import Dataset, Sample, File, DbStore
from cp3_llbb.SAMADhi.userPrompt import confirm, prompt_dataset, prompt_sample

def get_file_data_(f_):
    import ROOT

    f = ROOT.TFile.Open(f_)
    if not f:
        return (None, None)

    weight_sum = f.Get("event_weight_sum")
    if weight_sum:
        weight_sum = weight_sum.GetVal()
    else:
        weight_sum = None

    entries = None
    tree = f.Get("t")
    if tree:
        entries = tree.GetEntriesFast()

    return (weight_sum, entries)


class MyOptionParser: 
    """
    Client option parser
    """
    def __init__(self):
        usage  = "Usage: %prog type path [options]\n"
        usage += "where type is one of PAT, SKIM, RDS, NTUPLES, HISTOS, ...\n"
        usage += "      and path is the location of the sample on disk"  
        self.parser = OptionParser(usage=usage)
        self.parser.add_option("--name", action="store", type="string", 
                               default=None, dest="name",
             help="specify sample name")
        self.parser.add_option("--processed", action="store", type="int", 
                               default=None, dest="nevents_processed",
             help="number of processed events (from the input)")
        self.parser.add_option("--nevents", action="store", type="int", 
                               default=None, dest="nevents",
             help="number of events (in the sample)")
        self.parser.add_option("--norm", action="store", type="float", 
                               default=1.0, dest="normalization",
             help="additional normalization factor")
        self.parser.add_option("--weight-sum", action="store", type="float", 
                               default=1.0, dest="weight_sum",
             help="additional normalization factor")
        self.parser.add_option("--lumi", action="store", type="float", 
                               default=None, dest="luminosity",
             help="sample (effective) luminosity")
        self.parser.add_option("--code_version", action="store", type="string",
                               default="", dest="code_version",
             help="version of the code used to process that sample (e.g. git tag or commit)")
        self.parser.add_option("--comment", action="store", type="string",
                               default="", dest="user_comment",
             help="comment about the dataset")
        self.parser.add_option("--source_dataset", action="store", type="int", 
                               default=None, dest="source_dataset_id",
             help="reference to the source dataset")
        self.parser.add_option("--source_sample", action="store", type="int", 
                               default=None, dest="source_sample_id",
             help="reference to the source sample, if any")
        self.parser.add_option("-a", "--author", action="store", type="string",
                               default=None, dest="author",
             help="author of the result. If not specified, is taken from the path.")
        self.parser.add_option("--files", action="store", type="string",
                               default="", dest="files",
             help="list of files (full path, comma-separated values)")
        self.parser.add_option("-t", "--time", action="store", type="string",
                               default=None, dest="time",
             help="result timestamp. If set to \"path\", timestamp will be taken from the path. Otherwise, it must be formated like YYYY-MM-DD HH:MM:SS. Default is current time.")

    def get_opt(self):
        """
        Returns parse list of options
        """
        opts, args = self.parser.parse_args()
        # mandatory arguments
        if len(args) < 2:
          self.parser.error("type and path are mandatory")
        opts.sampletype = args[0]
        opts.path = os.path.abspath(os.path.expandvars(os.path.expanduser(args[1])))
        # check path
        if not os.path.exists(opts.path) or not ( os.path.isdir(opts.path) or os.path.isfile(opts.path)) :
          self.parser.error("%s is not an existing directory"%opts.path)
        # set author
        if opts.author is None:
          opts.author = getpwuid(os.stat(opts.path).st_uid).pw_name
        # set timestamp
        if not opts.time is None:
          if opts.time=="path":
            opts.datetime = datetime.fromtimestamp(os.path.getctime(opts.path))
          else:
            opts.datetime = datetime.strptime(opts.time,'%Y-%m-%d %H:%M:%S')
        else:
          opts.datetime = datetime.now()
        # set name
        if opts.name is None:
          if opts.path[-1]=='/':
            opts.name = opts.path.split('/')[-2]
          else:
            opts.name = opts.path.split('/')[-1]
        return opts

def main():
    """Main function"""
    # get the options
    optmgr = MyOptionParser()
    opts   = optmgr.get_opt()
    # build the sample from user input
    sample  = Sample(unicode(opts.name), unicode(opts.path), unicode(opts.sampletype), opts.nevents_processed)
    sample.nevents = opts.nevents
    sample.normalization = opts.normalization
    sample.event_weight_sum = opts.weight_sum
    sample.luminosity = opts.luminosity
    sample.code_version = unicode(opts.code_version)
    sample.user_comment = unicode(opts.user_comment)
    sample.source_dataset_id = opts.source_dataset_id
    sample.source_sample_id = opts.source_sample_id
    sample.author = unicode(opts.author)
    sample.creation_time = opts.datetime
    # connect to the MySQL database using default credentials
    dbstore = DbStore()
    # unless the source is set, prompt the user and present a list to make a choice
    if sample.source_dataset_id is None:
      prompt_dataset(sample,dbstore)
    if sample.source_sample_id is None:
      prompt_sample(sample,dbstore)
    # check that source sample and dataset exist
    if sample.source_dataset_id is not None:
      checkExisting = dbstore.find(Dataset,Dataset.dataset_id==sample.source_dataset_id)
      if checkExisting.is_empty():
        raise IndexError("No dataset with such index: %d"%sample.source_dataset_id)
    if sample.source_sample_id is not None:
      checkExisting = dbstore.find(Sample,Sample.sample_id==sample.source_sample_id)
      if checkExisting.is_empty():
        raise IndexError("No sample with such index: %d"%sample.source_sample_id)
    # if opts.nevents is not set, take #events from source sample (if set) or from source dataset (if set) in that order
    if sample.nevents_processed is None and sample.source_sample_id is not None:
      sample.nevents_processed = dbstore.find(Sample,Sample.sample_id==sample.source_sample_id).one().nevents_processed
    if sample.nevents_processed is None and sample.source_dataset_id is not None:
      sample.nevents_processed = dbstore.find(Dataset,Dataset.dataset_id==sample.source_dataset_id).one().nevents
    if sample.nevents_processed is None:
      print "Warning: Number of processed events not given, and no way to guess it."

    # List input files
    files = []
    if opts.files == "":
        files = glob.glob(os.path.join(sample.path, '*.root'))
    else:
        files = unicode(opts.files).split(",")
    if len(files) == 0:
      print "Warning: no root files found in %r" % sample.path

    # Try to guess the number of events stored into the file, as well as the weight sum
    for f in files:
        (weight_sum, entries) = get_file_data_(f)
        sample.files.add(File(f, f, weight_sum, entries))

    # check that there is no existing entry
    checkExisting = dbstore.find(Sample,Sample.name==sample.name)
    if checkExisting.is_empty():
      print sample
      if confirm(prompt="Insert into the database?", resp=True):
        dbstore.add(sample)
        # compute the luminosity, if possible
        if sample.luminosity is None:
          dbstore.flush()
          sample.luminosity = sample.getLuminosity()
    else:
      existing = checkExisting.one()
      prompt  = "Replace existing "
      prompt += str(existing)
      prompt += "\nby new "
      prompt += str(sample)
      prompt += "\n?"
      if confirm(prompt, resp=False):
        existing.replaceBy(sample)
        if existing.luminosity is None:
          dbstore.flush()
          existing.luminosity = existing.getLuminosity()
    # commit
    dbstore.commit()

#
# main
#
if __name__ == '__main__':
    main()
