#!/usr/bin/env python

# Script to do basic checks to the database and output statistics on usage and issues

import os,errno,json
import ROOT
ROOT.gROOT.SetBatch()
from optparse import OptionParser
from storm.info import get_cls_info
from datetime import date
from SAMADhi import Dataset, Sample, Result, MadWeight, DbStore
from datetime import datetime
from collections import defaultdict

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
    outputDict = {}
    outputDict["MissingDirSamples"] = checkSamplePath(dbstore,opts)
    outputDict["DatabaseInconsistencies"] = checkSampleConsistency(dbstore,opts)
    outputDict["SampleStatistics"] = analyzeSampleStatistics(dbstore,opts)
    if not opts.dryRun:
      with open(opts.path+'/analysisReport.json', 'w') as outfile:
        json.dump(outputDict, outfile, default=encode_storm_object)

    
def checkSamplePath(dbstore,opts):
    # get all samples
    result = dbstore.find(Sample)
    print "\nSamples with missing path:"
    print '==========================='
    array = []
    for sample in result:
      # check that the path exists, and keep track of the sample if not the case.
      if not os.path.exists(sample.path):
        print "Sample #%s (created on %s by %s):"%(str(sample.sample_id),str(sample.creation_time),str(sample.author)),
        print " missing path: %s" %sample.path
        array.append(sample)
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
        array.append(sample)
        print sample
      if (sample.source_sample_id is not None) and (sourceSample is None):
        print "Sample #%s (created on %s by %s):"%(str(sample.sample_id),str(sample.creation_time),str(sample.author)),
        print "inconsistent source sample"
        array.append(sample)
    if len(array)==0: print "None"
    return array


def analyzeSampleStatistics(dbstore,opts):
    stats = {}
    # ROOT output
    if not opts.dryRun:
      rootfile = ROOT.TFile(opts.path+"/analysisReport.root","update")
    #authors statistics
    output =  dbstore.execute("select sample.author,COUNT(sample.sample_id) as numOfSamples FROM sample GROUP BY author")
    stats["sampleAuthors"] = output.get_all()
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
    print "\nStatistics extracted."
    print '========================'
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

#
# main
#
if __name__ == '__main__':
    main()


#        json += json.dumps(sample, default=encode_storm_object)
        # here I should dump a report in the output path if necessary
#        if not opts.dryRun:
          #print json.dumps(sample.__str__())
          #TODO: check the best option for web rendering.
#my plan: 
# panel with general info on # errors
# collapsable accordeon panel with all faulty datasets -> header with sample id + name  and  bulk with the rest of the text

#<?php
#
#$fh = fopen('filename.txt','r');
#while ($line = fgets($fh)) {
#  // <... Do your work with the line ...>
#  // echo($line);
#}
#fclose($fh);
#?>
#<?php
#echo nl2br("foo isn't\n bar");
#?>
#
#<?php
#$myfile = fopen("webdictionary.txt", "r") or die("Unable to open file!");
#// Output one line until end-of-file
#while(!feof($myfile)) {
#  echo fgets($myfile) . "<br>";
#}
#fclose($myfile);
#?>

#test json:
#https://eval.in/426950
# question: do I output a json that reflects the sample class, or the message? or both?
