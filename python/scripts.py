from __future__ import unicode_literals, print_function
"""
Simple command-line SAMADhi utilities: search, interactive shell etc.
"""
import argparse
import os.path
import glob
from datetime import datetime

def interactive(args=None):
    """ iSAMADhi: Explore (and manipulate) the SAMADhi database in an IPython shell """
    parser = argparse.ArgumentParser(description="Explore (and manipulate) the SAMADhi database in an IPython shell")
    parser.add_argument("--database", default="~/.samadhi", help="JSON Config file with database connection settings and credentials")
    args = parser.parse_args(args=args)

    from .SAMADhi import _models, SAMADhiDB

    import IPython
    for md in _models:
        locals()[md.__name__] = md
    with SAMADhiDB(credentials=args.database) as db:
        IPython.embed(banner1=(
            "Interactively exploring SAMADhi database {database}\n"
            "Available models: {models}\n"
            "WARNING: by default your changes *will* be committed to the database"
            ).format(
                database="{0}({1}){2}".format(db.__class__.__name__, db.database,
                    (" at {0}".format(db.connect_params["host"]) if "host" in db.connect_params else "")),
                models=", ".join(md.__name__ for md in _models)
                ))

def search(args=None):
    """ search_SAMADhi: search for datasets, samples, results, or analyses """
    parser = argparse.ArgumentParser(description="Search for datasets, samples, results or analyses in SAMADhi")
    parser.add_argument("type", help="Object type to search for", choices=["dataset", "sample", "result", "analysis"])
    parser.add_argument("-l", "--long", action="store_true", help="detailed output")
    pquery = parser.add_mutually_exclusive_group(required=True)
    pquery.add_argument("-n", "--name", help="filter on name")
    pquery.add_argument("-p", "--path", help="filter on path", type=(lambda pth : os.path.abspath(os.path.expandvars(os.path.expanduser(pth)))))
    pquery.add_argument("-i", "--id", type=int, help="filter on id")
    parser.add_argument("--database", default="~/.samadhi", help="JSON Config file with database connection settings and credentials")
    args = parser.parse_args(args=args)
    # more validation
    if args.type in ("dataset", "analysis") and args.path:
        parser.error("Cannot search {0} by path".format(args.type))
    elif args.type == "result" and args.name:
        parser.error("Cannot search results by name")

    from . import SAMADhi
    from .SAMADhi import SAMADhiDB
    from .utils import replaceWildcards

    objCls = getattr(SAMADhi, args.type.capitalize())

    with SAMADhiDB(credentials=args.database) as db:
        qry = objCls.select()
        if args.id:
            qry = qry.where(objCls.id == args.id)
        elif args.name:
            qry = qry.where(objCls.name % replaceWildcards(args.name, db=db))
        elif args.path:
            qry = qry.where(objCls.path % replaceWildcards(args.path, db=db))
        results = qry.order_by(objCls.id)

        if args.long:
            for entry in results:
                print(str(entry))
                print(86*"-")
        else:
            fmtStr = "{{0.id}}\t{{0.{0}}}".format(("name" if args.type not in ("result", "analysis") else "description"))
            for res in results:
                print(fmtStr.format(res))

def update_dataset_cross_section(args=None):
    parser = argparse.ArgumentParser(description='Update cross-sections of datasets.')
    parser.add_argument('regex', type=str, help=('Regular expression used to filter *samples*.'
        'Only \'*\' and \'?\' wildcards are supported. Take note that filtering is applied to samples, and not to datasets.'))
    parser.add_argument('-f', '--force', type=float, help='For the cross-section of all datasets matching the regular expression to be this value', metavar='XSEC')
    parser.add_argument('-w', '--write', action='store_true', help='Write changes to the database')
    parser.add_argument("--database", default="~/.samadhi", help="JSON Config file with database connection settings and credentials")
    args = parser.parse_args(args)

    from .SAMADhi import Dataset, Sample, SAMADhiDB
    from .utils import replaceWildcards, maybe_dryrun

    with SAMADhiDB(credentials=args.database) as db:
        samples = Sample.select().where(Sample.name % replaceWildcards(args.regex, db=db))
        if samples.count() == 0:
            print("No sample found.")
        else:
            with maybe_dryrun(db, dryRun=(not args.write),
                    dryMessage="Currently running in dry-run mode. If you are happy with the change, pass the '-w' flag to this script to store the changes into the database."):
                for sample in samples:
                    if sample.source_dataset.datatype == "data":
                        continue
                    # Consider a cross-section of one as a non-updated value
                    if sample.source_dataset.xsection == 1 or sample.source_dataset.xsection is None:
                        # Try to find a similar sample in the database, with the same center of mass energy
                        print("Updating cross-section of {}".format(sample.source_dataset.process))
                        if args.force:
                            print("  Forcing the cross-section to {}".format(args.force))
                            sample.source_dataset.xsection = args.force
                        else:
                            possible_matches = Dataset.select().where(
                                    (Dataset.process % sample.source_dataset.process) &
                                    (Dataset.energy == sample.source_dataset.energy) &
                                    ( Dataset.id != sample.source_dataset.id )
                                    )
                            if possible_matches.count() == 0:
                                print("No match for this dataset found")
                            elif ( possible_matches.count() > 1 ) and not all( p.xsec == possible_matches[0].xsec for p in possible_matches ):
                                print("  Warning: more than one possible match found for this dataset, and they do not have the same cross-section. I do not know what to do...")
                            else:
                                xsec = possible_matches[0].xsec
                                print("  Updating with cross-section = {}".format(xsec))
                                sample.source_dataset.xsection = xsec

def get_file_data(f_):
    from cppyy import gbl

    f = gbl.TFile.Open(f_)
    if not f:
        return (None, None)

    weight_sum = f.Get("event_weight_sum")
    if weight_sum:
        weight_sum = weight_sum.GetVal()
    else:
        weight_sum = None

    entries = None
    tree = f.Get("t")
    if tree:
        entries = tree.GetEntriesFast()

    return (weight_sum, entries)

def add_sample(args=None):
    from .utils import parsePath, userFromPath, timeFromPath, confirm_transaction, prompt_dataset, prompt_sample

    parser = argparse.ArgumentParser(description="Add a sample to the database")
    parser.add_argument("--name", help="specify sample name")
    parser.add_argument("--processed", type=int, dest="nevents_processed", help="number of processed events (from the input)")
    parser.add_argument("--nevents", type=int, help="number of events (in the sample)")
    parser.add_argument("--norm", type=float, default=1.0, help="additional normalization factor")
    parser.add_argument("--weight-sum", type=float, default=1.0, help="additional normalization factor")
    parser.add_argument("--lumi", type=float, help="sample (effective) luminosity")
    parser.add_argument("--code_version", default="", help="version of the code used to process that sample (e.g. git tag or commit)")
    parser.add_argument("--comment", default="", help="comment about the dataset")
    parser.add_argument("--source_dataset", type=int, help="reference to the source dataset")
    parser.add_argument("--source_sample", type=int, help="reference to the source sample, if any")
    parser.add_argument("-a", "--author", help="author of the result. If not specified, is taken from the path.")
    parser.add_argument("--files", help="list of files (full path, comma-separated values)")
    parser.add_argument("-t", "--time", help="result timestamp. If set to \"path\", timestamp will be taken from the path. Otherwise, it must be formated like YYYY-MM-DD HH:MM:SS. Default is current time.")
    parser.add_argument("--database", default="~/.samadhi", help="JSON Config file with database connection settings and credentials")
    parser.add_argument("-y", "--continue", dest="assumeDefault", action="store_true", help="Assume defaults instead of prompt")
    parser.add_argument("type", choices=["PAT", "SKIM", "RDS", "NTUPLES", "HISTOS"], help="Sample type")
    parser.add_argument("path", help="location of the sample on disk", type=parsePath)
    args = parser.parse_args(args=args)

    if args.author is None:
        args.author = userFromPath(args.path)
    if args.time == "path":
        args.time = timeFromPath(args.path)
    elif args.time is not None:
        args.time = datetime.strptime(args.time, '%Y-%m-%d %H:%M:%S')
    else:
        args.time = datetime.now()
    if args.name is None:
        args.name = next(tk for tk in reversed(args.path.split("/")) if len(tk))

    from .SAMADhi import Dataset, Sample, File, SAMADhiDB

    with SAMADhiDB(credentials=args.database) as db:
        existing = Sample.get_or_none(Sample.name == args.name)
        with confirm_transaction(db, "Insert into the database?" if existing is None else "Replace existing {0!s}?".format(existing), assumeDefault=args.assumeDefault):
            sample, created = Sample.get_or_create(name=args.name, path=args.path,
                    defaults={
                        "sampletype" : args.type,
                        "nevents_processed" : args.nevents_processed
                        })
            sample.sampletype = args.type
            sample.nevents_processed = args.nevents_processed
            sample.nevents = args.nevents
            sample.normalization = args.norm
            sample.event_weight_sum = args.weight_sum
            sample.luminosity = args.lumi
            sample.code_version = args.code_version
            sample.user_comment = args.comment
            sample.source_dataset = ( Dataset.get_or_none(Dataset.id  ==  args.source_dataset) if args.source_dataset is not None else None )
            sample.source_sample = ( Sample.get_or_none(Sample.id  ==  args.source_sample) if args.source_sample is not None else None )
            sample.author = args.author
            sample.creation_time = args.time

            if sample.source_dataset is None and not args.assumeDefault:
                prompt_dataset(sample) ## TODO: check existence
            if sample.source_sample is None and not args.assumeDefault:
                prompt_sample(sample) ## TODO: check existence

            if sample.nevents_processed is None:
                if sample.source_sample is not None:
                    sample.nevents_processed = sample.source_sample.nevents_processed
                elif sample.source_dataset is not None:
                    sample.nevents_processed = sample.source_dataset.nevents
                else:
                    print("Warning: Number of processed events not given, and no way to guess it.")

            if args.files is not None:
                files = list(args.files.split(","))
            else:
                files = glob.glob(os.path.join(sample.path, "*.root"))
            if not files:
                print("Warning: no root files found in {0!r}".format(sample.path))
            for fName in files:
                weight_sum, entries = get_file_data(fName)
                File.create(
                    lfn=fName, pfn=fName,
                    event_weight_sum=weight_sum,
                    nevents=(entries if entries is not None else 0),
                    sample=sample
                    ) ## FIXME extras_event_weight_sum

            if sample.luminosity is None:
                sample.luminosity = sample.getLuminosity()
            sample.save()

            print(sample)

def add_result(args=None):
    from .utils import parsePath, userFromPath, timeFromPath, confirm_transaction, prompt_samples

    parser = argparse.ArgumentParser(description="Add a result to the database")
    parser.add_argument("path", type=parsePath)
    parser.add_argument("-s", "--sample", dest="inputSamples", help="comma separated list of samples used as input to produce that result")
    parser.add_argument("-d", "--description", help="description of the result")
    parser.add_argument("-e", "--elog", help="elog with more details")
    parser.add_argument("-A", "--analysis", type=int, help="analysis whose result belong to")
    parser.add_argument("-a", "--author", help="author of the result. If not specified, is taken from the path")
    parser.add_argument("-t", "--time", help="result timestamp. If set to \"path\", timestamp will be taken from the path. Otherwise, it must be formated like YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--database", default="~/.samadhi", help="JSON Config file with database connection settings and credentials")
    parser.add_argument("-y", "--continue", dest="assumeDefault", action="store_true", help="Assume defaults instead of prompt")
    args = parser.parse_args(args=args)

    if args.author is None:
        args.author = userFromPath(args.path)
    if args.time == "path":
        time = timeFromPath(args.path)
    elif args.time is not None:
        time = datetime.strptime(args.time, '%Y-%m-%d %H:%M:%S')
    else:
        time = datetime.now()

    from .SAMADhi import Sample, Result, SampleResult, SAMADhiDB

    with SAMADhiDB(credentials=args.database) as db:
        with confirm_transaction(db, "Insert into the database?", assumeDefault=args.assumeDefault):
            result = Result.create(
                path=args.path,
                description=args.description,
                author=args.author,
                creation_time=time,
                elog=args.elog,
                analysis=args.analysis,
                )
            if args.inputSamples is None:
                inputSampleIDs = prompt_samples()
            else:
                inputSampleIDs = [ int(x) for x in args.inputSamples.split(",") ]
            for smpId in inputSampleIDs:
                smp = Sample.get_or_none(Sample.id == smpId)
                if not smp:
                    print("Could not find sample #{0:d}".format(smpId))
                else:
                    SampleResult.create(sample=smp, result=result)
            print(result)

def splitWith(sequence, predicate):
    trueList, falseList = [], []
    for element in sequence:
        if predicate(element):
            trueList.append(element)
        else:
            falseList.append(element)

def checkAndClean(args=None):
    from .utils import parsePath, redirectOut, arg_loadJSON

    parser = argparse.ArgumentParser(description="Script to check samples for deletion")
    parser.add_argument("-p", "--path", default="./", type=parsePath, help="Path to the json files with db analysis results")
    parser.add_argument("-o", "--output", default="-", help="Name of the output file")
    parser.add_argument("-M", "--cleanupMissing", action="store_true", help="Clean samples with missing path from the database.")
    parser.add_argument("-U", "--cleanupUnreachable", action="store_true", help="Clean samples with unreachable path from the database")
    parser.add_argument("-D", "--cleanupDatasets", action="store_true", help="Clean orphan datasets from the database")
    parser.add_argument("-w", "--whitelist", type=arg_loadJSON, help="JSON file with sample whitelists per analysis.")
    parser.add_argument("-d", "--dry-run", action="store_true", help="Dry run: do not write to file and/or touch the database.")
    parser.add_argument("--database", default="~/.samadhi", help="JSON Config file with database connection settings and credentials")
    args = parser.parse_args(args=args)

    from .SAMADhi import SAMADhiDB

    with redirectOut(args.output if not args.dry_run else "-"):
        # open the sample analysis report and classify bad samples
        samples_missing = arg_loadJSON(os.path.join(args.path, "SamplesAnalysisReport.json")).get("MissingDirSamples", [])
        smp_white, smp_nonWhite = splitWith(samples_missing,
                lambda smp : any(label in smp["name"] for v in args.whitelist.values() for label in v))
        smp_empty, smp_investigate = splitWith(smp_white, lambda smp : smp["path"] == "")
        smp_empty_delete, smp_delete = splitWith(smp_nonwhite, lambda smp : smp["path"] == "")
        # now clean orphan datasets
        ds_orphan = arg_loadJSON(os.path.join(args.path, "DatasetsAnalysisReport.json")).get("Orphans", [])
        ## print a summary now
        print("\n\nWhitelisted sample with missing path. Investigate:\n{0}".format(
            "\n".join(smp["name"] for smp in smp_empty)))
        print("\n\nWhitelisted sample with unreachable path. Investigate:\n{0}".format(
            "\n".join(smp["name"] for smp in smp_investigate)))
        print("\n\nSamples to be deleted because of missing path:\n{0}".format(
            "\n".join(smp["name"] for smp in smp_empty_delete)))
        print("\n\nSamples to be deleted because of unreachable path:\n{0}".format(
            "\n".join(smp["name"] for smp in smp_delete)))
        ## actually perform the cleanup
        with SAMADhiDB(credentials=args.database) as db:
            with maybe_dryrun(db, dryRun=args.dry_run):
                if opts.cleanupMissing:
                    for smp in smp_empty_delete:
                        sample = Sample.get_or_none((Sample.id == smp["id"]) & (Sample.name == smp["name"]))
                        if sample is None:
                            print("Could not find sample #{id} {name}".format(smp["id"], smp["name"]))
                        else:
                            smp.removeFiles()
                            smp.delete_instance()
                if opts.cleanupUnreachable:
                    for smp in smp_delete:
                        sample = Sample.get_or_none((Sample.id == smp["id"]) & (Sample.name == smp["name"]))
                        if sample is None:
                            print("Could not find sample #{id} {name}".format(smp["id"], smp["name"]))
                        else:
                            sample.removeFiles()
                            sample.delete_instance()
                if args.cleanupDatasets:
                    for ids in ds_orphan:
                        dataset = Dataset.get_or_none((Dataset.id == ids["id"]) & (Dataset.name == ids["name"]))
                        if dataset is None:
                            print("Could not find dataset #{id} {name}".format(ids["id"], ids["name"]))
                        else:
                            dataset.delete_instance()
