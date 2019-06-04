#!/usr/bin/env python
""" Script to check samples for deletion """
import argparse
from cp3_llbb.SAMADhi.SAMADhi import SAMADhiDB
from cp3_llbb.SAMADhi.utils import parsePath, redirectOut, arg_loadJSON

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
    parser.add_argument("--database", default="~/.samadhi",
            help="JSON Config file with database connection settings and credentials")
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
        with SAMADhiDB(credentials=args.database) as db:
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
