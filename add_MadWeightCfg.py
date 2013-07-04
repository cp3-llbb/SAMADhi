#!/usr/bin/env python

# Script to add a sample to the database

import os
from optparse import OptionParser
from SAMADhi import MadWeight, DbStore

cards = [ "ident_card",
          "ident_mw_card",
          "info_card",
          "MadWeight_card",
          "mapping_card",
          "param_card_1",
          "param_card",
          "proc_card_mg5",
          "run_card",
          "transfer_card" ]

class MyOptionParser: 
    """
    Client option parser
    """
    def __init__(self):
        usage  = "Usage: %prog name path [options]\n"
        usage += "  where name is the configuration name\n"
        usage += "  and where path points to the MadWeight Cards directory"
        self.parser = OptionParser(usage=usage)
        self.parser.add_option("-s", "--syst", action="store", type="string",
                               default="", dest="syst",
             help="string identifying the systematics variation of the weight")

    def get_opt(self):
        """
        Returns parse list of options
        """
        opts, args = self.parser.parse_args()
        if len(args) < 2:
          self.parser.error("config name and path are mandatory")
        opts.name = str(args[0])
        opts.path = str(args[1])
        if not os.path.exists(opts.path) or not os.path.isdir(opts.path):
          self.parser.error("%s is not an existing directory"%opts.path)
        for card in cards:
          if not os.path.exists("%s/%s.dat"%(opts.path,card)):
            self.parser.error("%s doesn't look like a MadWeigh Card directory"%opts.path)
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

def main():
    """Main function"""
    # get the options
    optmgr = MyOptionParser()
    opts   = optmgr.get_opt()
    # build the configuration from user input
    madweightCfg = MadWeight(unicode(opts.name))
    for card in cards:
      setattr(madweightCfg, card, unicode(open(opts.path+"/"+card+".dat","r").read()))
    madweightCfg.systematics = unicode(opts.syst)
    # find the generate line(s)
    theCfg = filter(lambda x:x.startswith("generate"),map(lambda x:x.lstrip(' \t'),madweightCfg.proc_card_mg5.splitlines()))
    if len(theCfg)!=1:
      raise RuntimeError("Could not find a unique generate statement in proc_card_mg5.dat")
    madweightCfg.diagram = theCfg[0][8:].lstrip(' \t')
    # find the ISR correction parameter
    theCfg = filter(lambda x:x.startswith("isr"),map(lambda x:x.lstrip(' \t'),madweightCfg.MadWeight_card.splitlines()))
    if len(theCfg)!=1:
      raise RuntimeError("Could not find a unique isr statement in MadWeight_card.dat")
    madweightCfg.isr=int(theCfg[0].split(None,2)[1])
    
    # connect to the MySQL database using default credentials
    dbstore = DbStore()
    # check that there is no existing entry
    checkExisting = dbstore.find(MadWeight,MadWeight.name==madweightCfg.name)
    if checkExisting.is_empty():
      print madweightCfg
      if confirm(prompt="Insert into the database?", resp=True):
        dbstore.add(madweightCfg)
    else:
      existing = checkExisting.one()
      prompt  = "Replace existing "
      prompt += str(existing)
      prompt += "by new "
      prompt += str(madweightCfg)
      prompt += "?"
      if confirm(prompt, resp=False):
        existing.replaceBy(madweightCfg)
    # commit
    dbstore.commit()

#
# main
#
if __name__ == '__main__':
    main()
