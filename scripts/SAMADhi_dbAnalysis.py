#!/usr/bin/env python
""" Script to do basic checks to the database and output statistics on usage and issues """

import os,errno,json
import re
import ROOT
ROOT.PyConfig.IgnoreCommandLineOptions = True
ROOT.gROOT.SetBatch()
import argparse
from datetime import datetime
from cp3_llbb.SAMADhi.SAMADhi import Analysis, Dataset, Sample, Result, SAMADhiDB
from cp3_llbb.SAMADhi.SAMADhi import File as SFile
from collections import defaultdict
from cp3_llbb.SAMADhi.das_import import query_das
from playhouse.shortcuts import model_to_dict

from contextlib import contextmanager

@contextmanager
def openRootFile(fileName, noOp=False, mode="update"):
    if noOp:
        yield
    else:
        rootfile = ROOT.TFile.Open(fileName, mode)
        yield
        rootfile.Write();
        rootfile.Close();

def saveReportJSON(jReport, outFileName, outDir=".", symlinkDir=None):
    outFullName = os.path(outDir, outFileName)
    with open(outFullName, "w") as outFile:
        json.dump(jReport, outfile, default=model_to_dict)
    if symlinkDir:
        force_symlink(outFullName, os.path.join(symlinkDir, outFileName))

def main(args=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-p", "--path", type=(lambda p : os.path.abspath(os.path.expandvars(os.path.expanduser(p)))),
        default=datetime.now().strftime("%y%m%d-%H:%M:%S"),
        help="Destination path")
    parser.add_argument("-b", "--basedir", 
        help="Directory where the website will be installed")
    parser.add_argument("-f", "--full", action="store_true", dest="DAScrosscheck",
        help="Full check: compares each Dataset entry to DAS and check for consistency (slow!)")
    parser.add_argument("-d", "--dry", action="store_true", dest="dryRun",
        help="Dry run: do no write to disk")
    parser.add_argument("--database", default="~/.samadhi",
        help="JSON Config file with database connection settings and credentials")
    args = parser.parse_args(args=args)
    if not args.dryRun:
        if os.path.exists(args.path):
            raise OSError(errno.EEXIST, "Existing directory", args.path);
        else:
            os.makedirs(args.path)

    # connect to the MySQL database using default credentials
    with SAMADhiDB(credentials=args.database) as db, openRootFile(os.path.join(args.path, "analysisReport.root"), noOp=dryRun, mode="UPDATE"):
        # run each of the checks and collect data
        # collect general statistics
        general = collectGeneralStats()
        if not args.dryRun:
            saveReportJSON(general, "stats.json", outDir=args.path, symlinkDir=os.path.join(args.basedir, "data"))
        # check datasets
        datasets = {
            "DatabaseInconsistencies" : (checkDatasets() if args.DAScrosscheck else copyInconsistencies(args.basedir)),
            "Orphans" : findOrphanDatasets(),
            "IncompleteData" : checkDatasetsIntegrity()
            "DatasetsStatistics" : analyzeDatasetsStatistics(writeRoot=(not args.dryRun))
            }
        if not args.dryRun:
            saveReportJSON(datasets, "DatasetsAnalysisReport.json", outDir=args.path, symlinkDir=os.path.join(args.basedir, "data"))
        # check samples
        samples = {
            "MissingDirSamples" : checkSamplePath()
            "DatabaseInconsistencies" : checkSampleConsistency()
            "SampleStatistics" : analyzeSampleStatistics(writeRoot=(not args.dryRun))
            }
        if not args.dryRun:
            saveReportJSON(samples, "SamplesAnalysisReport.json", outDir=args.path, symlinkDir=os.path.join(args.basedir, "data"))
        # now, check results
        results = {
            "MissingDirSamples" : checkResultPath()
            "DatabaseInconsistencies" : checkResultConsistency()
            "SelectedResults" : selectResults(os.path.join(args.basedir, "data"))
            "ResultsStatistics" : analyzeResultsStatistics(writeRoot=(not args.dryRun))
            }
        if not args.dryRun:
            saveReportJSON(results, "ResultsAnalysisReport.json", outdir=args.path, symlinkDir=os.path.join(args.basedir, "data"))
        # finally, some stats about Analysis objects
        analyses = {
            "AnalysisStatistics" : analyzeAnalysisStatistics(writeRoot=(not args.dryRun))
            }
        if not args.dryRun
            saveReportJSON(analyses, "AnalysisAnalysisReport.json", outdir=args.path, symlinkDir=os.path.join(args.basedir, "data"))

def collectGeneralStats():
    # get number of datasets, samples, results, analyses
    result = {
        "nDatasets" : Dataset.select().count(),
        "nSamples"  : Sample.select().count(),
        "nResults"  : Result.select().count(),
        "nAnalysis" : Analysis.select().count()
        }
    print("\nGeneral statistics:")
    print("======================")
    for kt,num in result.items():
        print("{0:d} {1}".format(num, kt[1:].lower()))
    return result

def checkDatasets():
    print("\nDatasets inconsistent with DAS:")
    print("==================================")
    result = []
    for dataset in Dataset.select():
        # query DAS to get the same dataset, by name
        try:
            metadata = query_das(dataset.name)
        except:
            result.append([ dataset, "Inconsistent with DAS" ])
            print("{0.name} (imported on {0.creation_time!s}) -- Error getting dataset in DAS".format(dataset))
            continue
          
        # perform some checks: 
        try:
            # release name either matches or is unknown in DAS
            test1 = metadata['release'] == "unknown" or dataset.cmssw_release == metadata['release']
            # datatype matches
            test2 = dataset.datatype == metadata['datatype']
            # nevents matches
            test3 = dataset.nevents == metadata['nevents']
            # size matches
            test4 = dataset.dsize == metadata['file_size']
        except:
            result.append([ dataset, "Inconsistent with DAS" ])
            print("{0.name} (imported on {0.creation_time!s})".format(dataset))
        else:
            if not ( test1 and test2 and test3 and test4 ):
                result.append([ dataset, "Inconsistent with DAS" ])
                print("{0.name} (imported on {0.creation_time!s})".format(dataset))
    return result

def findOrphanDatasets():
    print("\nOrphan Datasets:")
    print("===================")
    result = []
    for dataset in Dataset.select():
        if dataset.samples.count() == 0:
            result.append(dataset)
            print("{0.name} (imported on {0.creation_time!s})".format(dataset))
    if len(result) == 0:
       print("None")
    return result

def checkDatasetsIntegrity():
    print("\nDatasets integrity issues:")
    print("===========================")
    result = []
    for dataset in Dataset.select():
        if dataset.cmssw_release is None:
            result.append([ dataset, "missing CMSSW release" ])
            print("{0.name} (imported on {0.creation_time!s}): missing CMSSW release".format(dataset))
        elif dataset.energy is None:
            result.append([dataset,"missing Energy"])
            print("{0.name} (imported on {0.creation_time!s}): missing Energy".format(dataset))
        elif dataset.globaltag is None:
            result.append([dataset,"missing Globaltag"])
            print("{0.name} (imported on {0.creation_time!s}): missing Globaltag".format(dataset))
    if len(result) == 0:
       print("None")
    return result

def makePie(uName, data, title=None, save=False):
    pie = ROOT.TPie("{0}Pie".format(uName), title if title is not None else uName, len(data))
    for idx, (val, freq) in enumerate(data.items()):
        pie.SetEntryVal(idx, freq)
        pie.SetEntryLabel(idx, val)
    pie.SetTextAngle(0);
    pie.SetRadius(0.3);
    pie.SetTextColor(1);
    pie.SetTextFont(62);
    pie.SetTextSize(0.03);
    canvas = ROOT.TCanvas(uName, "", 2)
    pie.Draw("r")
    if save:
        ROOT.gPad.Write()

def getFreqs(model, attName, addNoneTo=None):
    from peewee import fn
    freqs = dict((getattr(val, attName), val.count) for val in model.select(getattr(model, attName), fn.Count(model.id).alias("count")).group_by(getattr(model, attName)))
    if addNoneTo is not None and None in freqs:
        freqs[addNoneTo] = freqs.get(addNoneTo, 0)+freqs[None]
        del freqs[None]
    return freqs

def th1ToChart(histo):
    return [ [ histo.GetBinCenter(ib), histo.GetBinContent(ib) ] for ib in range(1, histo.GetNbinsX()+1) ]

def toTH1I(name, data, N, xMin, xMax, title=None):
    if title is None:
        title = name
    h = ROOT.TH1I(name, title, N, xMin, xMax)
    for x in data:
        h.Fill(x)
    return h

def toGraph(x, y=None):
    if y is None:
        y = np.array(range(len(x))+1)
    else:
        assert len(x) == len(y)
    gr = TGraph(len(x))
    for i,(x,y) in enumerate(zip(x,y)):
        gr.SetPoint(i, x, y)
    return gr

def analyzeDatasetsStatistics(writeRoot=False):
    stats = {}
    for prop in ("cmssw_release", "globaltag", "datatype", "energy"):
        nDataset_by_prop = getFreqs(Dataset, prop, addNoneTo="Unknown")
        stats[prop] = [ [k,v] for k,v in nDataset_by_prop.items() ]
        makePie("dataset{0}".format(prop.capitalize()), nDataset_by_prop, title="Datasets {0}".format(prop), save=(not dryRun))

    dset_time, dset_nsamples, dset_nevents, dset_dsize = zip(*(
        (int(dset.creation_time.strftime("%s"))*1000, dset.samples.count(), dset.nevents, dset.dsize)
        for dset in Dataset.select(Dataset.creation_time, Dataset.samples, Dataset.nevents, Dataset.dsize).order_by(Dataset.creation_time)
        ))
    stats["datasetsNsamples"] = th1ToChart(toTH1I("dataseets_nsamples", dset_nsamples, 10, 0, 10))
    stats["datasetsNevents"] = th1ToChart(toTH1I("dataseets_nevents", dset_nevents, 100, 0, -100))
    stats["datasetsDsize"] = th1ToChart(toTH1I("dataseets_dsize", dset_dsize, 100, 0, -100))
    stats["datasetsTimeprof"] = [ [tm, i+1] for i, tm in enumerate(dset_time) ]
    if writeRoot:
        toGraph(np.array(dset_time)/1000.).Write("datasetsTimeprof_graph")

    print("\nDatasets Statistics extracted.")
    print('=================================')

    return stats

def checkResultPath():
    # get all samples
    print("\nResults with missing path:")
    print("===========================")
    for res in Result.select():
        # check that the path exists, and keep track of the sample if not the case.
        if not os.path.exists(res.path):
            print("Result #{0.id} (created on {0.creation_time} by {0.author}):".format(res))
            print(" missing path: {0}".format(res.path))
            result.append(res)
    if len(result) == 0:
        print("None")
    return result
    
def checkSamplePath():
    print("\nSamples with missing path:")
    print("===========================")
    result = []
    for sample in Sample.select():
        # check that the path exists, and keep track of the sample if not the case.
        vpath = getSamplePath(sample)
        for path in vpath:
            if not os.path.exists(path):
                print "Sample #%s (created on %s by %s):"%(str(sample.id),str(sample.creation_time),str(sample.author)),
                print " missing path: %s" %path
                print vpath
                result.append(sample)
                break
    if len(result) == 0:
        print("None")
    return result

def getSamplePath(sample):
    # the path should be stored in sample.path
    # if it is empty, look for files in that path
    if sample.path == "":
        vpath = set()
        regex = r".*SFN=(.*)"
        for f in SFile.select().where(SFile.sample.id == sample.id):
            m = re.search(regex, f.pfn)
            if m:
                vpath.add(os.path.dirname(m.group(1)))
        return list(vpath)
    else:
        return [ sample.path ]

def selectResults(symlinkDir):
    # look for result records pointing to a ROOT file
    # eventually further filter 
    print("\nSelected results:")
    print("===========================")
    result = []
    for res in Result.select():
        path = res.path
        if os.path.exists(path) and os.path.isdir(path):
            files = [ f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) ]
            if len(files) == 1:
                path = os.path.join(path, f)
		res.path = path
	if os.path.exists(path) and os.path.isfile(path) and path.lower().endswith(".root"):
	    symlink = os.path.join(symlinkDir, "res_{0}.root".format(res.id))
	    relpath = "../data/res_{0}.root"%(res.id)
	    force_symlink(path, symlink)
	    result.append([ res, relpath ])
            print("res #{0.id} (created on {0.creation_time} by {0.author}): ".format(res))
            print(symlink)
    if len(result) == 0:
        print("None")
    return result

def checkResultConsistency():
    print("\nResults with missing source:")
    print("=============================")
    result = []
    for res in Result.select():
      # check that the source sample exists in the database.
      # normaly, this should be protected already at the level of sql rules
      for sample in res.samples:
          if sample is None:
              print("Result #%s (created on %s by %s):"%(str(res.id),str(res.creation_time),str(res.author)),
              print "inconsistent source sample"
              result.append([res,"inconsistent source sample"])
              print res
              break
    if len(result) == 0:
        print("None")
    return result

def checkSampleConsistency():
    print("\nSamples with missing source:")
    print("=============================")
    result = []
    for sample in Sample.select():
        # check that either the source dataset or the source sample exists in the database.
        # normaly, this should be protected already at the level of sql rules
        sourceDataset = sample.source_dataset
        sourceSample = sample.source_sample
        if sample.source_dataset_id is not None and sample.source_dataset is None:
            print("Sample #{0.id} (created on {0.creation_time} by {0.author}".format(sample))
            print("inconsistent source dataset")
            result.append([ sample, "inconsistent source dataset" ])
            print(sample)
        if sample.source_sample_id is not None and sample.source_sample is None:
            print("Sample #{0.id} (created on {0.creation_time} by {0.author}".format(sample))
            print("inconsistent source sample")
            result.append([ sample, "inconsistent source sample" ])
    if len(array) == 0:
        print("None")
    return array

def analyzeAnalysisStatistics(writeRoot=False):
    stats = {}
    nAnalyses_by_contact = getFreqs(Analysis, "contact", addNoneTo="Unknown")
    stats["analysisContacts"] = [ [k,v] for k,v in nAnalyses_by_contact.items() ]
    makePie("analysisContact", nAnalyses_by_contact, title"Analysis contacts", save=writeRoot)
    nResults_by_analysis = dict((ana.description, len(ana.results)) for ana in Analysis.select(Analysis.description, Analysis.results) if len(ana.results) > 0)
    stats["analysisResults"] = [ [k,v] for k,v in nResults_by_analysis.items() ]
    makePie("analysisResults", nResults_by_analysis, title="Analysis results", save=writeRoot)

    # stats to collect: group distribution (from CADI line) (pie)
    cadiExpr = re.compile(r".*([A-Z]{3})-\d{2}-\d{3}")
    nAnalysis_by_physicsgroup = defaultdict(int)
    for analysis in Analysis.select():
        m = cadiExpr.search(analysis.cadiline)
        nAnalysis_by_physicsgroup[m.group(1) if m else "NONE"] += 1
    stats["physicsGroup"] = [ [k,v] for k,v in nAnalysis_by_physicsgroup.items() ]
    makePie("physicsGroup", nAnalyses_by_physicsgroup, title="Physics groups", save=writeRoot)

    print("\nAnalysis Statistics extracted.")
    print("================================")

    return stats

def analyzeResultsStatistics(writeRoot=False):
    stats = {}
    nResults_by_author = getFreqs(Result, "author", addNoneTo="Unknown")
    stats["resultsAuthors"] = [ [k,v] for k,v in nResults_by_author.items() ]

    res_time, res_nsamples = zip(*(
        (int(res.creation_time.strftime("%s"))*1000, res.samples.count())
        for res in Result.select(Result.creation_time, Result.samples).order_by(Result.creation_time)
        ))
    stats["resultNsamples"] = th1ToChart(toTH1I("result_nsamples", res_nsamples, 20, 0, 20))
    if writeRoot:
        toGraph(np.array(res_time)/1000.).Write("resultsTimeprof_graph")

    print("\nResults Statistics extracted.")
    print("================================")

    return stats

def analyzeSampleStatistics(writeRoot=False):
    stats = {}
    nSamples_by_author = getFreqs(Sample, "author", addNoneTo="Unknown")
    stats["sampleAuthors"] = [ [k,v] for k,v in nSamples_by_author.items() ]
    makePie("sampleAuthors", nSamples_by_author, title="Sample authors", save=writeRoot)
    nSamples_by_type = getFreqs(Sample, "sampletype", addNoneTo="Unknown"])
    stats["sampleTypes"] = [ [k,v] for k,v in nSamples_by_type.items() ]
    makePie("sampleTypes", nSamples_by_type, title="Sample types", save=writeRoot)

    samples_time, sample_nevents, sample_nevents_processed = zip(*(
        (int(smp.creation_time.strftime("%s"))*1000, sample.nevents, sample.nevents_processed)
        for smp in Sample.select(Sample.creation_time, Sample.nevents, Sample.nevents_processed).order_by(Sample.creation_time)
        ))
    statsp"sampleNevents"] = th1ToChart(toTH1I("sample_nevents", sample_nevents, 100, 0, -100))
    statsp"sampleNeventsProcessed"] = th1ToChart(toTH1I("sample_nevents_processed", sample_nevents_processed, 100, 0, -100))
    stats["sampleNeventsTimeprof"] = list(list(row) for row in zip(samples_time, np.cumsum(np.array(sample_nevents))))
    stats["sampleNeventsProcessedTimeprof"] = list(list(row) for row in zip(samples_time, np.cumsum(np.array(sample_nevents_processed))))
    stats["samplesTimeprof"] = samples_time
    if writeRoot:
        samples_time_s = np.array(samples_time)/1000.
        toGraph(samples_time_s, np.cumsum(sample_nevents)).Write("sampleNeventsTimeprof_graph")
        toGraph(samples_time_s, np.cumsum(sample_nevents_processed)).Write("sampleNeventsProcessedTimeprof_graph")
        toGraph(samples_time_s).Write("samplesTimeprof_graph")

    print("\nSamples Statistics extracted.")
    print("================================")

    return stats

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
        with open(os.path.join(basedir, "data", "DatasetsAnalysisReport.json")) as jfile:
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

if __name__ == "__main__":
    main()
