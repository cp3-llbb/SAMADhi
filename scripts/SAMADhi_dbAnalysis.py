#!/usr/bin/env python

# Script to do basic checks to the database and output statistics on usage and issues

import os,errno,json
import re
import ROOT
ROOT.gROOT.SetBatch()
from optparse import OptionParser, OptionGroup
from datetime import date
from collections import defaultdict
from cp3_llbb.SAMADhi.SAMADhi import Analysis, Dataset, Sample, Result, DbStore
from cp3_llbb.SAMADhi.SAMADhi import File as SFile
from storm.info import get_cls_info
from datetime import datetime
from collections import defaultdict
from cp3_llbb.SAMADhi.das_import import query_das

class MyOptionParser:
    """
    Client option parser
    """
    def __init__(self):
        usage  = "Usage: %prog [options]\n"
        self.parser = OptionParser(usage=usage)
        self.parser.add_option("-p","--path", action="store", type="string",
                               dest="path", default=datetime.now().strftime("%y%m%d-%H:%M:%S"),
             help="Destination path")
        self.parser.add_option("-b","--basedir", action="store", type="string",
                               dest="basedir", default="",
             help="Directory where the website will be installed")
        self.parser.add_option("-f","--full", action="store_true",
                               dest="DAScrosscheck", default=False,
             help="Full check: compares each Dataset entry to DAS and check for consistency (slow!)")
        self.parser.add_option("-d","--dry", action="store_true",
                               dest="dryRun", default=False,
             help="Dry run: do no write to disk")

    def get_opt(self):
        """
        Returns parse list of options
        """
        opts, args = self.parser.parse_args()
        if opts.path is not None:
          opts.path = os.path.abspath(os.path.expandvars(os.path.expanduser(opts.path)))
        if not opts.dryRun and os.path.exists(opts.path):
           raise OSError(errno.EEXIST,"Existing directory",opts.path);
        return opts

def main():
    """Main function"""
    # get the options
    optmgr = MyOptionParser()
    opts = optmgr.get_opt()
    # connect to the MySQL database using default credentials
    dbstore = DbStore()
    # prepare the output directory
    if not os.path.exists(opts.path) and not opts.dryRun:
      os.makedirs(opts.path)
    # run each of the checks and collect data

    # collect general statistics
    outputDict = collectGeneralStats(dbstore,opts)
    if not opts.dryRun:
      with open(opts.path+'/stats.json', 'w') as outfile:
        json.dump(outputDict, outfile, default=encode_storm_object)
	force_symlink(opts.path+'/stats.json',opts.basedir+'/data/stats.json')
 
    # check datasets
    outputDict = {}
    outputDict["DatabaseInconsistencies"] = checkDatasets(dbstore,opts) if opts.DAScrosscheck else copyInconsistencies(opts.basedir)
    dbstore = DbStore() # reconnect, since the checkDatasets may take very long...
    outputDict["Orphans"] = findOrphanDatasets(dbstore,opts)
    outputDict["IncompleteData"] = checkDatasetsIntegrity(dbstore,opts)
    outputDict["DatasetsStatistics"] = analyzeDatasetsStatistics(dbstore,opts)
    if not opts.dryRun:
      with open(opts.path+'/DatasetsAnalysisReport.json', 'w') as outfile:
        json.dump(outputDict, outfile, default=encode_storm_object)
	force_symlink(opts.path+'/DatasetsAnalysisReport.json',opts.basedir+'/data/DatasetsAnalysisReport.json')

    # check samples
    outputDict = {}
    outputDict["MissingDirSamples"] = checkSamplePath(dbstore,opts)
    outputDict["DatabaseInconsistencies"] = checkSampleConsistency(dbstore,opts)
    outputDict["SampleStatistics"] = analyzeSampleStatistics(dbstore,opts)
    if not opts.dryRun:
      with open(opts.path+'/SamplesAnalysisReport.json', 'w') as outfile:
        json.dump(outputDict, outfile, default=encode_storm_object)
	force_symlink(opts.path+'/SamplesAnalysisReport.json',opts.basedir+'/data/SamplesAnalysisReport.json')

    # now, check results
    outputDict = {}
    outputDict["MissingDirSamples"] = checkResultPath(dbstore,opts)
    outputDict["DatabaseInconsistencies"] = checkResultConsistency(dbstore,opts)
    outputDict["SelectedResults"] = selectResults(dbstore,opts)
    outputDict["ResultsStatistics"] = analyzeResultsStatistics(dbstore,opts)
    if not opts.dryRun:
      with open(opts.path+'/ResultsAnalysisReport.json', 'w') as outfile:
        json.dump(outputDict, outfile, default=encode_storm_object)
	force_symlink(opts.path+'/ResultsAnalysisReport.json',opts.basedir+'/data/ResultsAnalysisReport.json')

    # finally, some stats about Analysis objects
    outputDict = {}
    outputDict["AnalysisStatistics"] = analyzeAnalysisStatistics(dbstore,opts)
    if not opts.dryRun:
      with open(opts.path+'/AnalysisAnalysisReport.json', 'w') as outfile:
        json.dump(outputDict, outfile, default=encode_storm_object)
        force_symlink(opts.path+'/AnalysisAnalysisReport.json',opts.basedir+'/data/AnalysisAnalysisReport.json')

def collectGeneralStats(dbstore,opts):
    # get number of datasets, samples, results, analyses
    result = {}
    results = dbstore.find(Result)
    samples = dbstore.find(Sample)
    datasets = dbstore.find(Dataset)
    analyses = dbstore.find(Analysis)
    result["nDatasets"] = datasets.count()
    result["nSamples"] = samples.count()
    result["nResults"] = results.count()
    result["nAnalyses"] = analyses.count()
    print "\nGeneral statistics:"
    print '======================'
    print datasets.count(), " datasets"
    print samples.count(), " samples"
    print results.count(), " results"
    return result

def checkDatasets(dbstore,opts):
    datasets = dbstore.find(Dataset)
    print "\nDatasets inconsistent with DAS:"
    print '=================================='
    result = []
    for dataset in datasets:
      # query DAS to get the same dataset, by name
      metadata = {}
      try:
        metadata = query_das(dataset.name)
      except:
        result.append([dataset,"Inconsistent with DAS"])
        print "%s (imported on %s) -- Error getting dataset in DAS"%(str(dataset.name),str(dataset.creation_time))
        continue
        
      # perform some checks: 
      try:
        # release name either matches or is unknown in DAS
        test1 = str(metadata[u'release'])=="unknown" or dataset.cmssw_release == str(metadata[u'release'])
        # datatype matches
        test2 = dataset.datatype == metadata[u'datatype']
        # nevents matches
        test3 = dataset.nevents == metadata[u'nevents']
        # size matches
        test4 = dataset.dsize == metadata[u'file_size']
      except:
         result.append([dataset,"Inconsistent with DAS"])
         print "%s (imported on %s)"%(str(dataset.name),str(dataset.creation_time))
      else:
         if not(test1 and test2 and test3 and test4):
             result.append([dataset,"Inconsistent with DAS"])
             print "%s (imported on %s)"%(str(dataset.name),str(dataset.creation_time))
    return result

def findOrphanDatasets(dbstore,opts):
    datasets = dbstore.find(Dataset)
    print "\nOrphan Datasets:"
    print '==================='
    result = []
    for dataset in datasets:
        if dataset.samples.count()==0:
            result.append(dataset)
            print "%s (imported on %s)"%(str(dataset.name),str(dataset.creation_time))
    if len(result)==0:
       print "None"
    return result

def checkDatasetsIntegrity(dbstore,opts):
    datasets = dbstore.find(Dataset)
    print "\nDatasets integrity issues:"
    print '==========================='
    result = []
    for dataset in datasets:
        if dataset.cmssw_release is None:
            result.append([dataset,"missing CMSSW release"])
            print "%s (imported on %s): missing CMSSW release"%(str(dataset.name),str(dataset.creation_time))
        elif dataset.energy is None:
            result.append([dataset,"missing Energy"])
            print "%s (imported on %s): missing Energy"%(str(dataset.name),str(dataset.creation_time))
        elif dataset.globaltag is None:
            result.append([dataset,"missing Globaltag"])
            print "%s (imported on %s): missing Globaltag"%(str(dataset.name),str(dataset.creation_time))
    if len(result)==0:
       print "None"
    return result
    

def analyzeDatasetsStatistics(dbstore,opts):
    # ROOT output
    if not opts.dryRun:
      rootfile = ROOT.TFile(opts.path+"/analysisReport.root","update")
    stats = {}
    # Releases used
    output =  dbstore.execute("select dataset.cmssw_release,COUNT(dataset.dataset_id) as numOfDataset FROM dataset GROUP BY cmssw_release")
    stats["cmssw_release"] = output.get_all()
    if None in stats["cmssw_release"]: 
        stats["cmssw_release"]["Unknown"] = stats["cmssw_release"][None] + stats["cmssw_release"].get("Unknown",0)
        del stats["cmssw_release"][None]
    releasePie = ROOT.TPie("datasetReleasePie","Datasets release",len(stats["cmssw_release"]))
    for index,entry in enumerate(stats["cmssw_release"]):
      releasePie.SetEntryVal(index,entry[1])
      releasePie.SetEntryLabel(index,"None" if entry[0] is None else entry[0])
    releasePie.SetTextAngle(0);
    releasePie.SetRadius(0.3);
    releasePie.SetTextColor(1);
    releasePie.SetTextFont(62);
    releasePie.SetTextSize(0.03);
    canvas = ROOT.TCanvas("datasetRelease","",2)
    releasePie.Draw("r")
    if not opts.dryRun:
      ROOT.gPad.Write()
    # GlobalTag used
    output =  dbstore.execute("select dataset.globaltag,COUNT(dataset.dataset_id) as numOfDataset FROM dataset GROUP BY globaltag")
    stats["globaltag"] = output.get_all()
    if None in stats["globaltag"]: 
        stats["globaltag"]["Unknown"] = stats["globaltag"][None] + stats["globaltag"].get("Unknown",0)
        del stats["globaltag"][None]
    globaltagPie = ROOT.TPie("datasetGTPie","Datasets globaltag",len(stats["globaltag"]))
    for index,entry in enumerate(stats["globaltag"]):
      globaltagPie.SetEntryVal(index,entry[1])
      globaltagPie.SetEntryLabel(index,"None" if entry[0] is None else entry[0])
    globaltagPie.SetTextAngle(0);
    globaltagPie.SetRadius(0.3);
    globaltagPie.SetTextColor(1);
    globaltagPie.SetTextFont(62);
    globaltagPie.SetTextSize(0.03);
    canvas = ROOT.TCanvas("datasetGT","",2)
    globaltagPie.Draw("r")
    if not opts.dryRun:
      ROOT.gPad.Write()
    # Datatype
    output =  dbstore.execute("select dataset.datatype,COUNT(dataset.dataset_id) as numOfDataset FROM dataset GROUP BY datatype")
    stats["datatype"] = output.get_all()
    if None in stats["datatype"]: 
        stats["datatype"]["Unknown"] = stats["datatype"][None] + stats["datatype"].get("Unknown",0)
        del stats["datatype"][None]
    datatypePie = ROOT.TPie("datasetTypePie","Datasets datatype",len(stats["datatype"]))
    for index,entry in enumerate(stats["datatype"]):
      datatypePie.SetEntryVal(index,entry[1])
      datatypePie.SetEntryLabel(index,"None" if entry[0] is None else entry[0])
    datatypePie.SetTextAngle(0);
    datatypePie.SetRadius(0.3);
    datatypePie.SetTextColor(1);
    datatypePie.SetTextFont(62);
    datatypePie.SetTextSize(0.03);
    canvas = ROOT.TCanvas("datasetType","",2)
    datatypePie.Draw("r")
    if not opts.dryRun:
      ROOT.gPad.Write()
    # Energy
    output =  dbstore.execute("select dataset.energy,COUNT(dataset.dataset_id) as numOfDataset FROM dataset GROUP BY energy")
    stats["energy"] = output.get_all()
    energyPie = ROOT.TPie("datasetEnergyPie","Datasets energy",len(stats["energy"]))
    for index,entry in enumerate(stats["energy"]):
      energyPie.SetEntryVal(index,entry[1])
      energyPie.SetEntryLabel(index,"None" if entry[0] is None else str(entry[0]))
    energyPie.SetTextAngle(0);
    energyPie.SetRadius(0.3);
    energyPie.SetTextColor(1);
    energyPie.SetTextFont(62);
    energyPie.SetTextSize(0.03);
    canvas = ROOT.TCanvas("datasetEnergy","",2)
    energyPie.Draw("r")
    if not opts.dryRun:
      ROOT.gPad.Write()
    # get all datasets to loop
    datasets = dbstore.find(Dataset)
    datasets.order_by(Dataset.creation_time)
    # time evolution of # datasets (still in db)
    datasets_time = [[0,0]]
    # various stats (histograms)
    datasets_nsamples = ROOT.TH1I("dataseets_nsamples","datasets_nsamples",10,0,10)
    datasets_nevents  = ROOT.TH1I("dataseets_nevents", "datasets_nevents" ,100,0,-100)
    datasets_dsize    = ROOT.TH1I("dataseets_dsize",   "datasets_dsize"   ,100,0,-100)
    # let's go... loop
    for dataset in datasets:
        # for Highcharts the time format is #seconds since epoch
        time = int(dataset.creation_time.strftime("%s"))*1000
        datasets_time.append([time,datasets_time[-1][1]+1])
        datasets_nsamples.Fill(dataset.samples.count())
        datasets_nevents.Fill(dataset.nevents)
        datasets_dsize.Fill(dataset.dsize)
    # drop this: just to initialize the loop
    datasets_time.pop(0)
    # output
    stats["datasetsTimeprof"] = datasets_time
    datasetsTimeprof_graph = ROOT.TGraph(len(datasets_time))
    for i,s in enumerate(datasets_time):
        datasetsTimeprof_graph.SetPoint(i,s[0]/1000,s[1])
    if not opts.dryRun:
        datasetsTimeprof_graph.Write("datasetsTimeprof_graph")
    data = []
    for bin in range(1,datasets_nsamples.GetNbinsX()+1):
        data.append([datasets_nsamples.GetBinCenter(bin),datasets_nsamples.GetBinContent(bin)])
    stats["datasetsNsamples"] = data
    data = []
    for bin in range(1,datasets_nevents.GetNbinsX()+1):
        data.append([datasets_nevents.GetBinCenter(bin),datasets_nevents.GetBinContent(bin)])
    stats["datasetsNevents"] = data
    data = []
    for bin in range(1,datasets_dsize.GetNbinsX()+1):
        data.append([datasets_dsize.GetBinCenter(bin),datasets_dsize.GetBinContent(bin)])
    stats["datasetsDsize"] = data
    # some printout
    print "\nDatasets Statistics extracted."
    print '================================='
    # ROOT output
    if not opts.dryRun:
      rootfile.Write();
      rootfile.Close();
    # JSON output
    return stats


def checkResultPath(dbstore,opts):
    # get all samples
    result = dbstore.find(Result)
    print "\nResults with missing path:"
    print '==========================='
    array = []
    for res in result:
      # check that the path exists, and keep track of the sample if not the case.
      if not os.path.exists(res.path):
        print "Result #%s (created on %s by %s):"%(str(res.result_id),str(res.creation_time),str(res.author)),
        print " missing path: %s" %res.path
        array.append(res)
    if len(array)==0: print "None"
    return array

    
def checkSamplePath(dbstore,opts):
    # get all samples
    result = dbstore.find(Sample)
    print "\nSamples with missing path:"
    print '==========================='
    array = []
    for sample in result:
      # check that the path exists, and keep track of the sample if not the case.
      vpath = getSamplePath(sample,dbstore)
      for path in vpath:
        if not os.path.exists(path):
          print "Sample #%s (created on %s by %s):"%(str(sample.sample_id),str(sample.creation_time),str(sample.author)),
          print " missing path: %s" %path
          print vpath
          array.append(sample)
          break
    if len(array)==0: print "None"
    return array

def getSamplePath(sample,dbstore):
    vpath=[]
    # the path should be stored in sample.path
    # if it is empty, look for files in that path
    if sample.path=="":
      regex = r".*SFN=(.*)"
      files = dbstore.find(SFile, SFile.sample_id==sample.sample_id)
      for file in files:
        m = re.search(regex,str(file.pfn))
        if m: vpath.append(os.path.dirname(m.group(1)))
      vpath=list(set(vpath))
      return vpath
    else:
      return [sample.path]

def selectResults(dbstore,opts):
    # look for result records pointing to a ROOT file
    # eventually further filter 
    results = dbstore.find(Result)
    print "\nSelected results:"
    print '==========================='
    array = []
    for result in results:
        path = result.path
        if os.path.exists(path) and os.path.isdir(path):
            files = [ f for f in os.listdir(path) if os.path.isfile(path+"/"+f) ]
            if len(files)==1:
                path = path+"/"+f
		result.path = path
	if os.path.exists(path) and os.path.isfile(path) and path.lower().endswith(".root"):
	    symlink = "%s/data/result_%s.root"%(opts.basedir,str(result.result_id))
	    relpath = "../data/result_%s.root"%(str(result.result_id))
	    force_symlink(path,symlink)
	    array.append([result,relpath])
            print "Result #%s (created on %s by %s): "%(str(result.result_id),str(result.creation_time),str(result.author)),
            print symlink

    if len(array)==0: print "None"
    return array

def checkResultConsistency(dbstore,opts):
    # get all samples
    result = dbstore.find(Result)
    print "\nResults with missing source:"
    print '============================='
    array = []
    for res in result:
      # check that the source sample exists in the database.
      # normaly, this should be protected already at the level of sql rules
      for sample in res.samples:
        if sample is None:
          print "Result #%s (created on %s by %s):"%(str(res.result_id),str(res.creation_time),str(res.author)),
          print "inconsistent source sample"
          array.append([res,"inconsistent source sample"])
          print res
          break
    if len(array)==0: print "None"
    return array


def checkSampleConsistency(dbstore,opts):
    # get all samples
    result = dbstore.find(Sample)
    print "\nSamples with missing source:"
    print '============================='
    array = []
    for sample in result:
      # check that either the source dataset or the source sample exists in the database.
      # normaly, this should be protected already at the level of sql rules
      sourceDataset = sample.source_dataset
      sourceSample = sample.source_sample
      if (sample.source_dataset_id is not None) and (sourceDataset is None):
        print "Sample #%s (created on %s by %s):"%(str(sample.sample_id),str(sample.creation_time),str(sample.author)),
        print "inconsistent source dataset"
        array.append([sample,"inconsistent source dataset"])
        print sample
      if (sample.source_sample_id is not None) and (sourceSample is None):
        print "Sample #%s (created on %s by %s):"%(str(sample.sample_id),str(sample.creation_time),str(sample.author)),
        print "inconsistent source sample"
        array.append([sample,"inconsistent source sample"])
    if len(array)==0: print "None"
    return array

def analyzeAnalysisStatistics(dbstore,opts):
    stats = {}
    # ROOT output
    if not opts.dryRun:
      rootfile = ROOT.TFile(opts.path+"/analysisReport.root","update")
    # contact
    output =  dbstore.execute("select analysis.contact,COUNT(analysis.analysis_id) as numOfAnalysis FROM analysis GROUP BY contact")
    stats["analysisContacts"] = output.get_all()
    if None in stats["analysisContacts"]: 
        stats["analysisContacts"]["Unknown"] = stats["analysisContacts"][None] + stats["analysisContacts"].get("Unknown",0)
        del stats["analysisContacts"][None]
    contactPie = ROOT.TPie("AnalysisContactPie","Analysis contacts",len(stats["analysisContacts"]))
    for index,entry in enumerate(stats["analysisContacts"]):
      contactPie.SetEntryVal(index,entry[1])
      contactPie.SetEntryLabel(index,"None" if entry[0] is None else entry[0])
    contactPie.SetTextAngle(0);
    contactPie.SetRadius(0.3);
    contactPie.SetTextColor(1);
    contactPie.SetTextFont(62);
    contactPie.SetTextSize(0.03);
    canvas = ROOT.TCanvas("analysisContact","",2)
    contactPie.Draw("r")
    if not opts.dryRun:
      ROOT.gPad.Write()
    # analysis size in terms of results (pie)
    output =  dbstore.execute("select analysis.description,COUNT(result.result_id) as numOfResults  FROM result INNER JOIN analysis ON result.analysis_id=analysis.analysis_id GROUP BY result.analysis_id;")
    stats["analysisResults"] = output.get_all()
    if None in stats["analysisResults"]: 
        stats["analysisResults"]["Unknown"] = stats["analysisResults"][None] + stats["analysisResults"].get("Unknown",0)
        del stats["analysisResults"][None]
    resultPie = ROOT.TPie("AnalysisResultsPie","Analysis results",len(stats["analysisResults"]))
    for index,entry in enumerate(stats["analysisResults"]):
      resultPie.SetEntryVal(index,entry[1])
      resultPie.SetEntryLabel(index,"None" if entry[0] is None else entry[0])
    resultPie.SetTextAngle(0);
    resultPie.SetRadius(0.3);
    resultPie.SetTextColor(1);
    resultPie.SetTextFont(62);
    resultPie.SetTextSize(0.03);
    canvas = ROOT.TCanvas("analysisResults","",2)
    resultPie.Draw("r")
    if not opts.dryRun:
      ROOT.gPad.Write()
    # stats to collect: group distribution (from CADI line) (pie)
    analyses = dbstore.find(Analysis)
    regex = r".*([A-Z]{3})-\d{2}-\d{3}"
    stats["physicsGroup"] = defaultdict(int)
    for analysis in analyses:
        m = re.search(regex,str(analysis.cadiline))
        physicsGroup = "NONE"
        if m: 
            physicsGroup = m.group(1)
        stats["physicsGroup"][physicsGroup] += 1
    stats["physicsGroup"] = dict(stats["physicsGroup"])
    if None in stats["physicsGroup"]: 
        stats["physicsGroup"]["Unknown"] = stats["physicsGroup"][None] + stats["physicsGroup"].get("Unknown",0)
        del stats["physicsGroup"][None]

    # the end of the loop, we have all what we need to fill a pie chart.
    physicsGroupPie = ROOT.TPie("physicsGroupPie","Physics groups",len(stats["physicsGroup"]))
    for index,(group,count) in enumerate(stats["physicsGroup"].iteritems()):
      physicsGroupPie.SetEntryVal(index,count)
      physicsGroupPie.SetEntryLabel(index,group)
    physicsGroupPie.SetTextAngle(0);
    physicsGroupPie.SetRadius(0.3);
    physicsGroupPie.SetTextColor(1);
    physicsGroupPie.SetTextFont(62);
    physicsGroupPie.SetTextSize(0.03);
    canvas = ROOT.TCanvas("physicsGroup","",2)
    physicsGroupPie.Draw("r")
    if not opts.dryRun:
      ROOT.gPad.Write()
    # some printout
    print "\nAnalysis Statistics extracted."
    print '================================'
    # ROOT output
    if not opts.dryRun:
      rootfile.Write();
      rootfile.Close();
    # JSON output
    stats["physicsGroup"] = [ [a,b] for (a,b) in stats["physicsGroup"].items()]
    return stats

def analyzeResultsStatistics(dbstore,opts):
    stats = {}
    # ROOT output
    if not opts.dryRun:
      rootfile = ROOT.TFile(opts.path+"/analysisReport.root","update")
    #authors statistics
    output =  dbstore.execute("select result.author,COUNT(result.result_id) as numOfResults FROM result GROUP BY author")
    stats["resultsAuthors"] = output.get_all()
    if None in stats["resultsAuthors"]: 
        stats["resultsAuthors"]["Unknown"] = stats["resultsAuthors"][None] + stats["resultsAuthors"].get("Unknown",0)
        del stats["resultsAuthors"][None]
    authorPie = ROOT.TPie("resultsAuthorsPie","Results authors",len(stats["resultsAuthors"]))
    for index,entry in enumerate(stats["resultsAuthors"]):
      authorPie.SetEntryVal(index,entry[1])
      authorPie.SetEntryLabel(index,"None" if entry[0] is None else entry[0])
    authorPie.SetTextAngle(0);
    authorPie.SetRadius(0.3);
    authorPie.SetTextColor(1);
    authorPie.SetTextFont(62);
    authorPie.SetTextSize(0.03);
    canvas = ROOT.TCanvas("resultsAuthor","",2)
    authorPie.Draw("r")
    if not opts.dryRun:
      ROOT.gPad.Write()
    result_nsamples = ROOT.TH1I("result_nsamples","result_nsamples",20,0,20)
    # get all samples to loop
    results = dbstore.find(Result)
    results.order_by(Result.creation_time)
    # time evolution of # results (still in db)
    results_time = [[0,0]]
    # let's go... loop
    for result in results:
        # for Highcharts the time format is #seconds since epoch
        time = int(result.creation_time.strftime("%s"))*1000
        results_time.append([time,results_time[-1][1]+1])
        result_nsamples.Fill(result.samples.count())
    # drop this: just to initialize the loop
    results_time.pop(0)
    # output
    stats["resultsTimeprof"] = results_time
    resultsTimeprof_graph = ROOT.TGraph(len(results_time))
    for i,s in enumerate(results_time):
        resultsTimeprof_graph.SetPoint(i,s[0]/1000,s[1])
    if not opts.dryRun:
        resultsTimeprof_graph.Write("resultsTimeprof_graph")
    data = []
    for bin in range(1,result_nsamples.GetNbinsX()+1):
        data.append([result_nsamples.GetBinCenter(bin),result_nsamples.GetBinContent(bin)])
    stats["resultNsamples"] = data
    # some printout
    print "\nResults Statistics extracted."
    print '================================'
    # ROOT output
    if not opts.dryRun:
      rootfile.Write();
      rootfile.Close();
    # JSON output
    return stats

def analyzeSampleStatistics(dbstore,opts):
    stats = {}
    # ROOT output
    if not opts.dryRun:
      rootfile = ROOT.TFile(opts.path+"/analysisReport.root","update")
    #authors statistics
    output =  dbstore.execute("select sample.author,COUNT(sample.sample_id) as numOfSamples FROM sample GROUP BY author")
    stats["sampleAuthors"] = output.get_all()
    if None in stats["sampleAuthors"]: 
        stats["sampleAuthors"]["Unknown"] = stats["sampleAuthors"][None] + stats["sampleAuthors"].get("Unknown",0)
        del stats["sampleAuthors"][None]
    authorPie = ROOT.TPie("sampleAuthorsPie","Samples authors",len(stats["sampleAuthors"]))
    for index,entry in enumerate(stats["sampleAuthors"]):
      authorPie.SetEntryVal(index,entry[1])
      authorPie.SetEntryLabel(index,"None" if entry[0] is None else entry[0])
    authorPie.SetTextAngle(0);
    authorPie.SetRadius(0.3);
    authorPie.SetTextColor(1);
    authorPie.SetTextFont(62);
    authorPie.SetTextSize(0.03);
    canvas = ROOT.TCanvas("sampleAuthor","",2)
    authorPie.Draw("r")
    if not opts.dryRun:
      ROOT.gPad.Write()
    #sample types statistics
    output =  dbstore.execute("select sample.sampletype,COUNT(sample.sample_id) as numOfSamples FROM sample GROUP BY sampletype")
    stats["sampleTypes"] = output.get_all()
    if None in stats["sampleTypes"]: 
        stats["sampleTypes"]["Unknown"] = stats["sampleTypes"][None] + stats["sampleTypes"].get("Unknown",0)
        del stats["sampleTypes"][None]
    typePie = ROOT.TPie("sampleTypesPie","Samples types",len(stats["sampleTypes"]))
    for index,entry in enumerate(stats["sampleTypes"]):
      typePie.SetEntryVal(index,entry[1])
      typePie.SetEntryLabel(index,"None" if entry[0] is None else entry[0])
    typePie.SetTextAngle(0);
    typePie.SetRadius(0.3);
    typePie.SetTextColor(1);
    typePie.SetTextFont(62);
    typePie.SetTextSize(0.03);
    canvas = ROOT.TCanvas("sampleType","",2)
    typePie.Draw("r")
    if not opts.dryRun:
      ROOT.gPad.Write()
    # get all samples to loop
    result = dbstore.find(Sample)
    result.order_by(Sample.creation_time)
    # events statistics
    sample_nevents_processed = ROOT.TH1I("sample_nevents_processed","sample_nevents_processed",100,0,-100)
    sample_nevents = ROOT.TH1I("sample_nevents","sample_nevents",100,0,-100)
    # time evolution of statistics & # samples (still in db)
    sample_nevents_processed_time = [[0,0]]
    sample_nevents_time = [[0,0]] 
    samples_time = [[0,0]]
    # let's go... loop
    for sample in result:
        # for Highcharts the time format is #seconds since epoch
        time = int(sample.creation_time.strftime("%s"))*1000
        ne = 0 if sample.nevents is None else sample.nevents
        np = 0 if sample.nevents_processed is None else sample.nevents_processed
        sample_nevents_processed.Fill(np)
        sample_nevents.Fill(ne)
        sample_nevents_processed_time.append([time,sample_nevents_processed_time[-1][1]+np])
        sample_nevents_time.append([time,sample_nevents_time[-1][1]+ne])
        samples_time.append([time,samples_time[-1][1]+1])
    # drop this: just to initialize the loop
    sample_nevents_processed_time.pop(0)
    sample_nevents_time.pop(0)
    samples_time.pop(0)
    # output
    stats["sampleNeventsTimeprof"] = sample_nevents_time
    stats["sampleNeventsProcessedTimeprof"] = sample_nevents_processed_time
    stats["samplesTimeprof"] = samples_time
    sampleNeventsTimeprof_graph = ROOT.TGraph(len(sample_nevents_time))
    sampleNeventsProcessedTimeprof_graph = ROOT.TGraph(len(sample_nevents_processed_time))
    samplesTimeprof_graph = ROOT.TGraph(len(samples_time))
    for i,s in enumerate(sample_nevents_time):
        sampleNeventsTimeprof_graph.SetPoint(i,s[0]/1000,s[1])
    for i,s in enumerate(sample_nevents_processed_time):
        sampleNeventsProcessedTimeprof_graph.SetPoint(i,s[0]/1000,s[1])
    for i,s in enumerate(samples_time):
        samplesTimeprof_graph.SetPoint(i,s[0]/1000,s[1])
    if not opts.dryRun:
        sampleNeventsTimeprof_graph.Write("sampleNeventsTimeprof_graph")
        sampleNeventsProcessedTimeprof_graph.Write("sampleNeventsProcessedTimeprof_graph")
        samplesTimeprof_graph.Write("samplesTimeprof_graph")
    # unfortunately, TBufferJSON is not available in CMSSW (no libRHttp) -> no easy way to export to JSON
    # the JSON format for highcharts data is [ [x1,y1], [x2,y2], ... ]
    data = []
    for bin in range(1,sample_nevents.GetNbinsX()+1):
      data.append([sample_nevents.GetBinCenter(bin),sample_nevents.GetBinContent(bin)])
    stats["sampleNevents"] = data
    data = []
    for bin in range(1,sample_nevents_processed.GetNbinsX()+1):
      data.append([sample_nevents_processed.GetBinCenter(bin),sample_nevents_processed.GetBinContent(bin)])
    stats["sampleNeventsProcessed"] = data
    # some printout
    print "\nSamples Statistics extracted."
    print '================================'
    # ROOT output
    if not opts.dryRun:
      rootfile.Write();
      rootfile.Close();
    # JSON output
    return stats

# function to serialize the storm objects,
# from Jamu Kakar and Mario Zito at https://lists.ubuntu.com/archives/storm/2010-May/001286.html
def encode_storm_object(object):
    ''' Serializes to JSON a Storm object
    
    Use:
        from storm.info import get_cls_info
        import json
        ...
        storm_object = get_storm_object()
        print json.dumps(storm_object, default=encode_storm_object)
            
    Warnings:
        Serializes objects containing Int, Date and Unicode data types
        other datatypes are not tested. MUST be improved
    '''
    if not hasattr(object, "__storm_table__"):
        raise TypeError(repr(object) + " is not JSON serializable")
    result = {}
    cls_info = get_cls_info(object.__class__)
    for name in cls_info.attributes.iterkeys():
        value= getattr(object, name)
        if (isinstance(value, date)): 
            value= str(value)
        result[name] = value
    return result

def force_symlink(file1, file2):
    try:
        os.symlink(file1, file2)
    except OSError, e:
        if e.errno == errno.EEXIST:
            os.remove(file2)
            os.symlink(file1, file2)

def copyInconsistencies(basedir):
    # try to read inconsistencies from previous job
    # the file must be there and must contain the relevant data
    try:
        with open(basedir+'/data/DatasetsAnalysisReport.json') as jfile:
            content = json.load(jfile)
            return content["DatabaseInconsistencies"]
    except IOError:
        # no file. Return an empty string.
        # This will happen if basedir is not (properly) set or if it is new.
        print("No previous dataset analysis report found in path. The Database inconsistencies will be empty.")
        return []
    except KeyError:
        # no proper key. Return an empty string.
        # This should not happen, so print a warning.
        print("No DatabaseInconsistencies key in the previous json file ?!")
        return []

#
# main
#
if __name__ == '__main__':
    main()

