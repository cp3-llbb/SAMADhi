#!/usr/bin/env python

# Script to add a sample to the database

import os
from optparse import OptionParser
from SAMADhi import Dataset, Sample, DbStore

class OptionParser: 
    """
    Client option parser
    """
    #TODO: update for this case
    def __init__(self):
        usage  = "Usage: %prog [options]\n"
        self.parser = OptionParser(usage=usage)
        self.parser.add_option("--sample", action="store", type="string", 
                               default=False, dest="sample",
             help="specify sample for your request.")
        self.parser.add_option("--process", action="store", type="string",
                               default=None, dest="process",
             help="specify process name. TLatex synthax may be used.")
        self.parser.add_option("--xsection", action="store", type="float",
                               default=0.0, dest="xsection",
             help="specify the cross-section.")
        self.parser.add_option("--energy", action="store", type="float",
                               default=0.0, dest="energy",
             help="specify the centre of mass energy.")
        self.parser.add_option("--comment", action="store", type="string",
                               default="", dest="comment",
             help="comment about the dataset")
    def get_opt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()

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

def main():
    """Main function"""
    # get the options
    optmgr  = OptionParser()
    opts, _ = optmgr.get_opt()
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
    #TODO: unless the source is set, prompt the user and present a list to make a choice
    # check that there is no existing entry
    checkExisting = dbstore.find(Sample,Sample.name==sample.name)
    if checkExisting.is_empty():
      #TODO: print the sample
      if confirm(prompt="Insert into the database?", resp=True):
        dbstore.add(dataset)
    else:
      prompt  = "Replace existing entry:\n"
      #TODO: print checkExisting.one() in prompt
      prompt += "\nby new entry:\n"
      #TODO: print the sample
      prompt += "\n?"
      if confirm(prompt, resp=False):
        checkExisting.remove()
        dbstore.add(sample)
    #TODO: the luminosity should be computed here, if possible, and stored
    # it probably implies to flush, call sample.luminosity() and then set it to the argument
    # commit
    dbstore.commit()


#
# main
#
if __name__ == '__main__':
    main()
