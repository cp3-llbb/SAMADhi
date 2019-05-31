#!/usr/bin/env python
""" Script to check samples for deletion """
import argparse
from cp3_llbb.SAMADhi.SAMADhi import SAMADhiDB

def parsePath(pth):
    import os.path
    pth = os.path.abspath(os.path.expandvars(os.path.expanduser(pth)))
    if not os.path.exists(pth) or not ( os.path.isdir(pth) or os.path.isfile(pth) ):
        raise argparse.ArgumentError("{0} is not an existing file or directory".format(pth))
    return pth

def checkWriteable(pth):
    import os, os.path
    pth = os.path.abspath(os.path.expandvars(os.path.expanduser(pth)))
    if not os.access(pth, os.W_OK):
        raise argparse.ArgumentError("Cannot write to {0}".format(pth))
    if os.path.isfile(pth):
        raise argparse.ArgumentError("File already exists: {0}".format(pth))
    return pth

@contextmanager
def redirectOut(outArg):
    if outArg == "-"
        yield
    else:
        outPth = checkWriteable(outArg)
        import sys
        with open(outPth, "W") as outF:
            bk_stdout = sys.stdout
            sys.stdout = outF
            yield
            sys.stdout = bk_stdout

def arg_loadJSON(pth):
    if pth:
        import json
        with open(parsePath(pth)) as jsF:
            return json.load(jsF)
    else:
        return dict()

def splitWith(sequence, predicate):
    trueList, falseList = [], []
    for element in sequence:
        if predicate(element):
            trueList.append(element)
        else:
            falseList.append(element)

def main(args=None)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-p", "--path", default="./", type=parsePath,
            help="Path to the json files with db analysis results")
    parser.add_argument("-o", "--output", default="-",
            help="Name of the output file")
    parser.add_argument("-M", "--cleanupMissing", action="store_true",
            help="Clean samples with missing path from the database.")
    parser.add_argument("-U", "--cleanupUnreachable", action="store_true",
            help="Clean samples with unreachable path from the database")
    parser.add_argument("-D", "--cleanupDatasets", action="store_true",
            help="Clean orphan datasets from the database")
    parser.add_argument("-w", "--whitelist", type=arg_loadJSON,
            help="JSON file with sample whitelists per analysis.")
    parser.add_argument("-d", "--dry-run", action="store_true",
            help="Dry run: do not write to file and/or touch the database.")
    args = parser.parse_args(args=args)

    with redirectOut(args.output if not args.dry_run else "-"),
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
        with SAMADhiDB() as db:
            with maybe_dryrun(db, dryRun=args.dry_run):
                if opts.cleanupMissing:
                    for smp in smp_empty_delete:
                        smp.removeFiles()
                        smp.delete_instance()
                if opts.cleanupUnreachable:
                    for smp in smp_delete:
                        smp.removeFiles()
                        smp.delete_instance()
                if args.cleanupDatasets:
                    for ids in ds_orphan:
                        ids.delete_instance()

if __name__ == "__main__":
    main()
