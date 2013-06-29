#!/usr/bin/env python

# Script to add a sample to the database

import os
from optparse import OptionParser
from SAMADhi import Dataset, Sample, DbStore

class OptionParser: 
    """
    Client option parser
    """
    def __init__(self):
        usage  = "Usage: %prog [options]\n"
        self.parser = OptionParser(usage=usage)
        self.parser.add_option("--sample", action="store", type="string", 
                               default=None, dest="name",
             help="specify sample name")
        self.parser.add_option("--path", action="store", type="string", 
                               default=None, dest="path",
             help="specify path to sample on disk")
        self.parser.add_option("--type", action="store", type="string", 
                               default=None, dest="sampletype",
             help="specify the type of sample (PAT, SKIM, RDS, NTUPLES, HISTOS, ...")
        self.parser.add_option("--processed", action="store", type="int", 
                               default=None, dest="nevents_processed",
             help="number of processed events (from the input)")
        self.parser.add_option("--nevents", action="store", type="int", 
                               default=None, dest="nevents_processed",
             help="number of events (in the sample)")
        self.parser.add_option("--norm", action="store", type="float", 
                               default=1.0, dest="normalization",
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
        self.parser.add_option("--source_dataset", action="store", type="string", 
                               default=None, dest="source_dataset_id",
             help="reference to the source dataset")
        self.parser.add_option("--source_sample", action="store", type="string", 
                               default=None, dest="source_sample_id",
             help="reference to the source sample, if any")

    def get_opt(self):
        """
        Returns parse list of options
        """
        opts, _ = self.parser.parse_args()
        # mandatory arguments
        if opts.name is None:
          self.parser.error("sample name is mandatory")
        if opts.path is None:
          self.parser.error("sample path is mandatory")
        if opts.sampletype is None:
          self.parser.error("sample type is mandatory")
        if opts.nevents_processed is None:
          self.parser.error("number of processed events is mandatory")
        return opts

def confirm(prompt=None, resp=False):
    """prompts for yes or no response from the user. Returns True for yes and
    False for no. 'resp' should be set to the default value assumed by the caller when
    user simply types ENTER.
    >>> confirm(prompt='Create Directory?', resp=True)
    Create Directory? [y]|n: 
    True
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y: 
    False
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y: y
    True
    """
    if prompt is None:
        prompt = 'Confirm'
    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')
    while True:
        ans = raw_input(prompt)
        if not ans:
            return resp
        if ans not in ['y', 'Y', 'n', 'N']:
            print 'please enter y or n.'
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False

# TODO: prompt the user and present a list to make a choice
def prompt_dataset(sample,store):
  pass

# TODO: prompt the user and present a list to make a choice
def prompt_sample(sample,store):
  pass

def main():
    """Main function"""
    # get the options
    optmgr = OptionParser()
    opts   = optmgr.get_opt()
    # build the sample from user input
    sample  = Sample(opts.name, opts.path, opts.sampletype, opts.nevents_processed)
    sample.nevents = opts.nevents
    sample.normalization = opts.normalization
    sample.luminosity = opts.luminosity
    sample.code_version = opts.code_version
    sample.user_comment = opts.user_comment
    sample.source_dataset_id = opts.source_dataset_id
    sample.source_sample_id = opts.source_sample_id
    # connect to the MySQL database using default credentials
    dbstore = DbStore()
    # unless the source is set, prompt the user and present a list to make a choice
    if sample.source_dataset_id is None:
      prompt_dataset(sample,dbstore)
    if sample.sample.source_sample_id is None:
      prompt_sample(sample,dbstore)
    # check that there is no existing entry
    checkExisting = dbstore.find(Sample,Sample.name==sample.name)
    if checkExisting.is_empty():
      print sample
      if confirm(prompt="Insert into the database?", resp=True):
        dbstore.add(sample)
    else:
      prompt  = "Replace existing entry:\n"
      prompt += str(checkExisting.one())
      prompt += "\nby new entry:\n"
      prompt += str(sample)
      prompt += "\n?"
      if confirm(prompt, resp=False):
        checkExisting.remove()
        dbstore.add(sample)
    # compute the luminosity, if possible
    if sample.luminosity is None:
      store.flush()
      sample.luminosity = sample.getLuminosity()
    # commit
    dbstore.commit()


#
# main
#
if __name__ == '__main__':
    main()
