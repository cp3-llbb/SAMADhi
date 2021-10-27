import datetime
import json
import re
import subprocess

from .utils import confirm_transaction


def do_das_query(query):
    """
    Execute das_client for the specified query, and return parsed JSON output
    """

    args = ['dasgoclient', '-json', '-query', query]
    result = subprocess.check_output(args)

    return json.loads(result)

def query_das(dataset):
    """
    Do a DAS request for the given dataset and return the metadata collected
    """

    summary_query  = "summary dataset=%s" % dataset
    metadata_query  = "dataset=%s" % dataset
    release_query  = "release dataset=%s" % dataset
    config_query  = "config dataset=%s system=dbs3" % dataset

    summary_results = do_das_query(summary_query)
    metadata_results = do_das_query(metadata_query)
    release_results = do_das_query(release_query)
    config_results = do_das_query(config_query)

    # Grab results from DAS
    metadata = {}
    for d in next(entry for entry in metadata_results if "dbs3:dataset_info" in entry["das"]["services"])["dataset"]:
        for key, value in d.items():
            metadata[key] = value
    for d in summary_results[0]["summary"]:
        for key, value in d.items():
            metadata[key] = value

    # Set release in global tag
    metadata.update({
        'release': release_results[0]["release"][0]["name"][0],
        'globalTag': config_results[0]["config"][0]["global_tag"]
    })

    # Last chance for the global tag
    for d in config_results:
      if metadata['globalTag']=='UNKNOWN':
        metadata['globalTag']=d["config"][0]["global_tag"]
    if metadata['globalTag']=='UNKNOWN':
      del metadata['globalTag']

    return metadata

def import_cms_dataset(dataset, process=None, energy=None, xsection=1.0, comment="", assumeDefault=False, credentials=None):
    """
    Do a DAS request for the given dataset and insert it into SAMAdhi
    """

    if subprocess.call(["voms-proxy-info", "--exists", "--valid", "0:5"]) != 0:
        raise RuntimeError("No valid proxy found (with at least 5 minutes left)")

    # Guess default sane values for unspecifed parameters
    if not process:
        splitString = dataset.split('/', 2)
        if len(splitString) > 1:
            process = splitString[1]

    if not energy:
        energyRe = re.search(r"([\d.]+)TeV", dataset)
        if energyRe:
            energy = float(energyRe.group(1))

    metadata = query_das(dataset)
    metadata.update({
        "process": process,
        "xsection": xsection,
        "energy": energy,
        "comment": comment
    })
    if not all(ky in metadata for ky in ("name", "datatype")):
        raise RuntimeError(f"Could not find all required keys (name and datatype) in {metadata!s}")

    # definition of the conversion key -> column
    column_conversion = {
            "process": 'process',
            "user_comment": 'comment',
            "energy": 'energy',
            "nevents": 'nevents',
            "cmssw_release": 'release',
            "dsize": 'file_size',
            "globaltag": 'globalTag',
            "xsection": 'xsection'
            }
    # columns of the dataset to create (if needed)
    dset_columns = {col: metadata[key] for col, key in column_conversion.items()}
    dset_columns["creation_time"] = datetime.datetime.fromtimestamp(metadata["creation_time"]) if "creation_time" in metadata else None

    from .SAMADhi import SAMADhiDB, Dataset

    with SAMADhiDB(credentials) as db:
        existing = Dataset.get_or_none(Dataset.name == metadata["name"])
        with confirm_transaction(db, "Insert into the database?" if existing is None else "Update this dataset?", assumeDefault=assumeDefault):
            dataset, created = Dataset.get_or_create(
                    name=metadata["name"], datatype=metadata["datatype"],
                    defaults=dset_columns
                    )
            print(dataset)
            return dataset

def main(args=None):
    import argparse
    parser = argparse.ArgumentParser(description='Import CMS datasets into SAMADhi')
    parser.add_argument("dataset", help="CMS dataset")
    parser.add_argument("-p", "--process", help="Process name")
    parser.add_argument("--xsection", type=float, default=1.0, help="Cross-section in pb")
    parser.add_argument("--energy", type=float, dest="energy", help="CoM energy, in TeV")
    parser.add_argument("--comment", default="", help="User defined comment")
    parser.add_argument("--database", default="~/.samadhi", help="JSON Config file with database connection settings and credentials")
    parser.add_argument("-y", "--continue", dest="assumeDefault", action="store_true", help="Insert or replace without prompt for confirmation")
    args = parser.parse_args(args=args)

    import_cms_dataset(args.dataset, args.process, args.energy, args.xsection, args.comment, assumeDefault=args.assumeDefault, credentials=args.database)

def get_nanoFile_data(fileName):
    from cppyy import gbl
    f = gbl.TFile.Open(fileName)
    if not f:
        print(f"Warning: could not open file {fileName}")
        return None, None
    eventsTree = f.Get("Events")
    if ( not eventsTree ) or ( not isinstance(eventsTree, gbl.TTree) ):
        print(f"No tree with name 'Events' found in {fileName}")
        return None, None
    entries = eventsTree.GetEntries()
    runs = f.Get("Runs")
    if ( not runs ) or ( not isinstance(runs, gbl.TTree) ):
        print(f"No tree with name 'Runs' found in {fileName}")
        return entries, None
    sums = dict()
    runs.GetEntry(0)
    for lv in runs.GetListOfLeaves():
        lvn = lv.GetName()
        if lvn != "run":
            if lv.GetLeafCount():
                lvcn = lv.GetLeafCount().GetName()
                if lvcn in sums:
                    del sums[lvcn]
                sums[lvn] = [ lv.GetValue(i) for i in range(lv.GetLeafCount().GetValueLong64()) ]
            else:
                sums[lvn] = lv.GetValue()
    for entry in range(1, runs.GetEntries()):
        runs.GetEntry(entry)
        for cn, vals in sums.items():
            if hasattr(vals, "__iter__"):
                entryvals = getattr(runs, cn)
                ## warning and workaround (these should be consistent for all NanoAODs in a sample)
                if len(vals) != len(entryvals):
                    logger.error(f"Runs tree: array of sums {cn} has a different length in entry {entry:d}: {len(entryvals):d} (expected {len(vals):d})")
                for i in range(min(len(vals), len(entryvals))):
                    vals[i] += entryvals[i]
            else:
                sums[cn] += getattr(runs, cn)
    return entries, sums

def import_nanoAOD_sample(args=None):
    import argparse
    parser = argparse.ArgumentParser("Add a NanoAOD sample based on the DAS path and (optionally) cross-section")
    parser.add_argument("path", help="DAS path")
    parser.add_argument("--xsection", default=1., type=float, help="Cross-section value")
    parser.add_argument("--energy", default=13., type=float, help="CoM energy, in TeV")
    parser.add_argument("-p", "--process", help="Process name")
    parser.add_argument("--comment", default="", help="User defined comment")
    parser.add_argument("--datasetcomment", default="", help="User defined comment")
    parser.add_argument("--store", required=True, help="root path of the local CMS storage (e.g. /storage/data/cms)")
    parser.add_argument("--database", default="~/.samadhi", help="JSON Config file with database connection settings and credentials")
    parser.add_argument("-y", "--continue", dest="assumeDefault", action="store_true", help="Insert or replace without prompt for confirmation")
    args = parser.parse_args(args=args)

    if subprocess.call(["voms-proxy-info", "--exists", "--valid", "0:5"]) != 0:
        raise RuntimeError("No valid proxy found (with at least 5 minutes left)")

    parent_results = do_das_query(f"parent dataset={args.path}")
    if not ( len(parent_results) == 1 and len(parent_results[0]["parent"]) == 1):
        raise RuntimeError("Parent dataset query result has an unexpected format")
    parent_name = parent_results[0]["parent"][0]["name"]
    source_dataset = import_cms_dataset(parent_name, process=args.process, energy=args.energy, xsection=args.xsection, comment=args.datasetcomment, assumeDefault=args.assumeDefault, credentials=args.database)

    files_results = do_das_query(f"file dataset={args.path}")
    nevents = sum(fr["file"][0]["nevents"] for fr in files_results)

    from .SAMADhi import Sample, File, SAMADhiDB
    import os.path

    ## Next: the add_sample part
    with SAMADhiDB(credentials=args.database) as db:
        existing = Sample.get_or_none(Sample.name == args.path)
        with confirm_transaction(db, "Insert into the database?" if existing is None else f"Replace existing {existing!s}?", assumeDefault=args.assumeDefault):
            sample, created = Sample.get_or_create(name=args.path, path=args.path,
                    defaults={
                        "sampletype" : "NTUPLES",
                        "nevents_processed" : nevents
                        })
            sample.nevents = nevents
            sample.normalization = 1.
            sample.source_dataset = source_dataset
            sample.source_sample = None

            sample_weight_sum = 0
            for fRes in files_results:
                if len(fRes["file"]) != 1:
                    raise RuntimeError("File result from DAS query has an unexpected format")
                fileInfo = fRes["file"][0]
                pfn = os.path.join(args.store, fileInfo["name"].lstrip(os.path.sep))
                entries, weight_sums = get_nanoFile_data(pfn)
                #print("For debug: nevents from DAS={0:d}, from file={1:d}".format(fileInfo["nevents"], entries))
                event_weight_sum = weight_sums["genEventSumw"]
                #print("All event weight sums: {0!r}".format(weight_sums))
                sample_weight_sum += event_weight_sum
                File.create(
                    lfn=fileInfo["name"], pfn=pfn,
                    event_weight_sum=event_weight_sum,
                    nevents=(entries if entries is not None else 0),
                    sample=sample
                    ) ## FIXME extras_event_weight_sum

            sample.event_weight_sum = sample_weight_sum
            sample.luminosity = sample.getLuminosity() ## from xsection and sum of weights
            sample.comment = args.comment
            sample.author = "CMS"
            sample.save()

            print(sample)
