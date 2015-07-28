#!/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/bin/python

# Script to add a sample to the database

import os
from optparse import OptionParser
from SAMADhi import MadWeight, DbStore
from userPrompt import confirm

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
        usage += "  and where path points to the MadWeight directory"
        self.parser = OptionParser(usage=usage)

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
          if not os.path.exists("%s/Cards/%s.dat"%(opts.path,card)):
            self.parser.error("%s doesn't look like a MadWeigh directory"%opts.path)
        if not os.path.exists("%s/Source/MadWeight/transfer_function/Transfer_FctVersion.txt"%opts.path):
            self.parser.error("%s/Source/MadWeight/transfer_function/Transfer_FctVersion.txt does not exist!"%opts.path)
        return opts

def main():
    """Main function"""
    # get the options
    optmgr = MyOptionParser()
    opts   = optmgr.get_opt()
    # build the configuration from user input
    madweightCfg = MadWeight(unicode(opts.name))
    for card in cards:
      setattr(madweightCfg, card, unicode(open(opts.path+"/Cards/"+card+".dat","r").read()))
    # get the transfert functions
    madweightCfg.transfer_fctVersion = unicode(open('%s/Source/MadWeight/transfer_function/Transfer_FctVersion.txt'%opts.path,"r").read().strip('\n'))
    theCfg = madweightCfg.transfer_fctVersion.split(':')[0]
    if not os.path.exists("%s/Source/MadWeight/transfer_function/data/TF_%s.dat"%(opts.path,theCfg)):
      raise RuntimeError("Could not find the transfert functions TF_%s.dat"%theCfg)
    madweightCfg.transfer_function = unicode(open("%s/Source/MadWeight/transfer_function/data/TF_%s.dat"%(opts.path,theCfg),"r").read())
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
    # find the NWA configuration parameter
    theCfg = filter(lambda x:x.startswith("nwa"),map(lambda x:x.lstrip(' \t'),madweightCfg.MadWeight_card.splitlines()))
    if len(theCfg)!=1:
      raise RuntimeError("Could not find a unique nwa statement in MadWeight_card.dat")
    nwa = theCfg[0].split(None,2)[1]
    if nwa=='F':
      madweightCfg.nwa=False
    elif nwa=='T':
      madweightCfg.nwa=True
    else:
      raise RuntimeError("Unrecognized value for the nwa parameter in MadWeight_card.dat: %s"%nwa)
    # find the beam energy and store cm energy in TeV
    theCfg = filter(lambda x:"ebeam1" in x,madweightCfg.run_card.splitlines())
    try:
      madweightCfg.cm_energy = float(theCfg[0].split()[0])*0.002
    except:
      print "Cannot find the beam energy in the run card"
      raise
    # find and add the Higgs weight (can be null, so no error if missing)
    theCfg = filter(lambda x:x.startswith("DECAY"),map(lambda x:x.lstrip(' \t'),madweightCfg.param_card_1.splitlines()))
    for cfg in theCfg:
      fields = cfg.split()
      if fields[1]=="25":
        madweightCfg.higgs_width = float(fields[2])
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
      prompt += "\nby new "
      prompt += str(madweightCfg)
      prompt += "\n?"
      if confirm(prompt, resp=False):
        existing.replaceBy(madweightCfg)
    # commit
    dbstore.commit()

#
# main
#
if __name__ == '__main__':
    main()
