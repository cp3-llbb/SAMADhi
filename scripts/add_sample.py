#!/usr/bin/env python
""" Add a sample to the database """
import argparse
import os.path
import glob
from pwd import getpwuid
from datetime import datetime
from cp3_llbb.SAMADhi.SAMADhi import Dataset, Sample, File, SAMADhiDB
from cp3_llbb.SAMADhi.utils import parsePath, userFromPath, timeFromPath, confirm_transaction, prompt_dataset, prompt_sample

def get_file_data(f_):
    import ROOT

    f = ROOT.TFile.Open(f_)
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

def main(args=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name",
            help="specify sample name")
    parser.add_argument("--processed", type=int, dest="nevents_processed",
            help="number of processed events (from the input)")
    parser.add_argument("--nevents", type=int,
            help="number of events (in the sample)")
    parser.add_argument("--norm", type=float, default=1.0,
            help="additional normalization factor")
    parser.add_argument("--weight-sum", type=float, default=1.0,
            help="additional normalization factor")
    parser.add_argument("--lumi", type=float,
            help="sample (effective) luminosity")
    parser.add_argument("--code_version", default="",
            help="version of the code used to process that sample (e.g. git tag or commit)")
    parser.add_argument("--comment", default="",
            help="comment about the dataset")
    parser.add_argument("--source_dataset", type=int,
            help="reference to the source dataset")
    parser.add_argument("--source_sample", type=int,
            help="reference to the source sample, if any")
    parser.add_argument("-a", "--author",
            help="author of the result. If not specified, is taken from the path.")
    parser.add_argument("--files",
            help="list of files (full path, comma-separated values)")
    parser.add_argument("-t", "--time",
            help="result timestamp. If set to \"path\", timestamp will be taken from the path. Otherwise, it must be formated like YYYY-MM-DD HH:MM:SS. Default is current time.")
    parser.add_argument("--database", default="~/.samadhi",
            help="JSON Config file with database connection settings and credentials")
    parser.add_argument("-y", "--continue", dest="assumeDefault", action="store_true",
            help="Assume defaults instead of prompt")
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


    with SAMADhiDB(credentials=args.database) as db:
        existing = Sample.get_or_none(Sample.name == args.name)
        with confirm_transaction(db, "Insert into the database?" if existing is None else "Replace existing {0!s}?".format(existing), assumeDefault=args.assumeDefault):
            sample, created = Sample.get_or_create(
                    name=args.name,
                    path=args.path,
                    sampletype=args.type,
                    nevents_processed=args.nevents_processed,
                    nevents=args.nevents,
                    normalization=args.norm,
                    event_weight_sum=args.weight_sum,
                    luminosity=args.lumi,
                    code_version=args.code_version,
                    user_comment=args.comment,
                    source_dataset=( Dataset.get_or_none(Dataset.id == args.source_dataset) if args.source_dataset is not None else None ),
                    source_sample=( Sample.get_or_none(Sample.id == args.source_sample) if args.source_sample is not None else None ),
                    author=args.author,
                    creation_time=args.time,
                    )

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

            print(sample)

if __name__ == '__main__':
    main()
