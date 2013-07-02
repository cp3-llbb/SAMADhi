#!/usr/bin/env python

# Script to add a sample to the database

import os
from optparse import OptionParser
from SAMADhi import Sample, Result, DbStore

class MyOptionParser: 
    """
    Client option parser
    """
    def __init__(self):
        usage  = "Usage: %prog path [options]\n"
        self.parser = OptionParser(usage=usage)
        self.parser.add_option("-s", "--sample", action="store", type="string", 
                               default=None, dest="inputSamples",
             help="comma separated list of samples used as input to produce that result")
        self.parser.add_option("-d", "--description", action="store", type="string", 
                               default=None, dest="desc",
             help="description of the result")

    def get_opt(self):
        """
        Returns parse list of options
        """
        opts, path = self.parser.parse_args()
        opts.path = path
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

def parse_sample(inputString):
  return [ int(x) for x in inputString.split(',') ]

def prompt_sample(store):
  """prompts for the source sample among the existing ones"""
  print "No source sample defined."
  print "Please select the samples associated with this result."
  # full list of samples
  print "Sample\t\tName"
  check = store.find(Sample)
  all_samples = check.values(Sample.sample_id,Sample.name)
  for dset in all_samples:
    print "%i\t\t%s"%(dset[0], dset[1])
  # prompt
  while True:
    try:
      return parse_sample(raw_input("Comma-separated list of sample id [None]?"))
    except:
      continue

def main():
    """Main function"""
    # get the options
    optmgr = MyOptionParser()
    opts   = optmgr.get_opt()
    # build the result from user input
    result = Result(unicode(opts.path))
    result.description = unicode(opts.desc)
    # connect to the MySQL database using default credentials
    dbstore = DbStore()
    # unless the source is set, prompt the user and present a list to make a choice
    if opts.inputSamples is None:
      inputSamples = prompt_sample(dbstore)
    else:
      inputSamples = parse_sample(opts.inputSamples)
    # create and store the relations
    samples = dbstore.find(Sample,Sample.name.is_in(inputSamples))
    if samples.is_empty():
      dbstore.add(result)
    else:
      for sample in samples:
        sample.results.add(result)
    print result
    if confirm(prompt="Insert into the database?", resp=True):
      dbstore.commit()

#
# main
#
if __name__ == '__main__':
    main()
