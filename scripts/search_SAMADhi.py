#!/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/bin/python

# Script to add a sample to the database

import os
from optparse import OptionParser
from SAMADhi import Dataset, Sample, Result, MadWeight, DbStore

class MyOptionParser: 
    """
    Client option parser
    """
    def __init__(self):
        usage  = "Usage: %prog type [options]\n"
        usage += "Where type is one of dataset, sample, result, madweight"
        self.parser = OptionParser(usage=usage)
        self.parser.add_option("-l","--long", action="store_true", 
                               dest="longOutput", default=False,
             help="detailed output")
        self.parser.add_option("-n","--name", action="store", type="string",
                               dest="name", default=None,
             help="filter on name")
        self.parser.add_option("-p","--path", action="store", type="string",
                               dest="path", default=None,
             help="filter on path")
        self.parser.add_option("-i","--id", action="store", type="int",
                               dest="objid", default=None,
             help="filter on id")

    def get_opt(self):
        """
        Returns parse list of options
        """
        opts, args = self.parser.parse_args()
        if len(args) == 0:
          self.parser.error("must specify the type of item to search for")
        if args[0] not in ["dataset","sample","result","madweight"]:
          self.parser.error("type must be one of dataset, sample, result")
        cnt = 0
        if opts.path is not None: 
          cnt +=1
          opts.path = os.path.abspath(os.path.expandvars(os.path.expanduser(opts.path)))
        if opts.name is not None: cnt +=1
        if opts.objid is not None: cnt +=1
        if cnt>1:
          self.parser.error("only one selection criteria may be applied")
        if args[0]=="dataset" and opts.path is not None:
          self.parser.error("cannot search dataset by path")
        if args[0]=="result" and opts.name is not None:
          self.parser.error("cannot search a result by name")
        if args[0]=="madweight" and opts.path is not None:
          self.parser.error("cannot search MadWeight setup by path")
        opts.objtype = args[0]
        return opts

def main():
    """Main function"""
    # get the options
    optmgr = MyOptionParser()
    opts = optmgr.get_opt()
    # connect to the MySQL database using default credentials
    dbstore = DbStore()
    # build the query
    if opts.objtype == "dataset":
      objectClass = Dataset
      objectId = Dataset.dataset_id
    elif opts.objtype == "sample":
      objectClass = Sample
      objectId = Sample.sample_id
    elif opts.objtype == "madweight":
      objectClass = MadWeight
      objectId = MadWeight.process_id
    else:
      objectClass = Result
      objectId = Result.result_id

    if opts.objid is not None:
      result = dbstore.find(objectClass, objectId==opts.objid)
    elif opts.path is not None:
      result = dbstore.find(objectClass, objectClass.path.like(unicode(opts.path.replace('*', '%').replace('?', '_'))))
    elif opts.name is not None:
      result = dbstore.find(objectClass, objectClass.name.like(unicode(opts.name.replace('*', '%').replace('?', '_'))))
    else: 
      result = dbstore.find(objectClass)

    result = result.order_by(objectId)
    # loop and print
    if opts.longOutput:
      for entry in result:
        print entry
        print "--------------------------------------------------------------------------------------"
    else:
      if opts.objtype != "result":
        data = result.values(objectId, objectClass.name)
      else:
        data = result.values(objectId, objectClass.description)
      for dset in data:
        print "%i\t%s"%(dset[0], dset[1])

#
# main
#
if __name__ == '__main__':
    main()
