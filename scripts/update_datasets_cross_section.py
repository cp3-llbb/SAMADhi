#!/usr/bin/env python
from __future__ import unicode_literals, print_function
""" Simple script to compute the luminosity of a set of samples """

import subprocess
import argparse
from cp3_llbb.SAMADhi.SAMADhi import Dataset, Sample, SAMADhiDB
from cp3_llbb.SAMADhi.utils import replaceWildcards, maybe_dryrun

def main(args=None):
    parser = argparse.ArgumentParser(description='Update cross-sections of datasets.')
    parser.add_argument('regex', type=str, help=('Regular expression used to filter *samples*.'
        'Only \'*\' and \'?\' wildcards are supported. Take note that filtering is applied to samples, and not to datasets.'))
    parser.add_argument('-f', '--force', type=float, help='For the cross-section of all datasets matching the regular expression to be this value', metavar='XSEC')
    parser.add_argument('-w', '--write', action='store_true', help='Write changes to the database')
    parser.add_argument("--database", default="~/.samadhi", help="JSON Config file with database connection settings and credentials")
    args = parser.parse_args(args)

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

if __name__ == '__main__':
    main()
