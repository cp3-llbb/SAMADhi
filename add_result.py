#!/usr/bin/env python

# Script to add a sample to the database

import os
from pwd import getpwuid
from datetime import datetime
from optparse import OptionParser
from SAMADhi import Sample, Result, DbStore
from userPrompt import confirm, prompt_samples, parse_samples

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
        self.parser.add_option("-a", "--author", action="store", type="string", 
                               default=None, dest="author",
             help="author of the result. If not specified, is taken from the path.")
        self.parser.add_option("-t", "--time", action="store", type="string", 
                               default=None, dest="time",
             help="result timestamp. If set to \"path\", timestamp will be taken from the path. Otherwise, it must be formated like YYYY-MM-DD HH:MM:SS")

    def get_opt(self):
        """
        Returns parse list of options
        """
        opts, args = self.parser.parse_args()
        # check that the path exists
        if len(args) < 1:
          self.parser.error("path is mandatory")
        opts.path = args[0]
        if not os.path.exists(opts.path) or not os.path.isdir(opts.path):
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
        return opts

def main():
    """Main function"""
    # get the options
    optmgr = MyOptionParser()
    opts   = optmgr.get_opt()
    # build the result from user input
    result = Result(unicode(opts.path))
    result.description = unicode(opts.desc)
    result.author = unicode(opts.author)
    result.creation_time = opts.datetime
    # connect to the MySQL database using default credentials
    dbstore = DbStore()
    # unless the source is set, prompt the user and present a list to make a choice
    if opts.inputSamples is None:
      inputSamples = prompt_samples(dbstore)
    else:
      inputSamples = parse_samples(opts.inputSamples)
    # create and store the relations
    samples = dbstore.find(Sample,Sample.sample_id.is_in(inputSamples))
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
