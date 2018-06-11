#!/usr/bin/env python
""" Simple script to compute the luminosity of a set of samples """

import subprocess
import argparse
from cp3_llbb.SAMADhi.SAMADhi import Dataset, Sample, DbStore
from storm.locals import Desc

def get_options():
    parser = argparse.ArgumentParser(description='Update cross-sections of datasets.')

    parser.add_argument('regex', type=str, help='Regular expression used to filter *samples*. Only \'*\' and \'?\' wildcards are supported. Take note that filtering is applied to samples, and not to datasets.', metavar='REGEX')

    parser.add_argument('-f', '--force', type=float, dest='force', help='For the cross-section of all datasets matching the regular expression to be this value', metavar='XSEC')

    parser.add_argument('-w', '--write', dest='write', action='store_true', help='Write changes to the database')

    options = parser.parse_args()

    return options


dbstore = DbStore()

def get_samples(name):
    return dbstore.find(Sample, Sample.name.like(unicode(name.replace('*', '%').replace('?', '_'))))

def main():
    options = get_options()
    samples = get_samples(options.regex)

    if samples.count() == 0:
        print("No sample found.")
        return

    for sample in samples:
        if sample.source_dataset.datatype == "data":
            continue

        # Consider a cross-section of one as a non-updated value
        if sample.source_dataset.xsection == 1:
            # Try to find a similar sample in the database, with the same center of mass energy
            print("Updating cross-section of {}".format(sample.source_dataset.process))

            if options.force:
                print("  Forcing the cross-section to {}".format(options.force))
                if options.write:
                    sample.source_dataset.xsection = options.force
            else:
                possible_matches = dbstore.find(Dataset, Dataset.process.like(sample.source_dataset.process),
                        Dataset.energy == sample.source_dataset.energy,
                        Dataset.dataset_id != sample.source_dataset.dataset_id)

                xsec = None
                if possible_matches.count() == 0:
                    print("  No match for this dataset found.")
                else:
                    for p in possible_matches.order_by(Desc(Dataset.dataset_id)):
                        if not xsec:
                            xsec = p.xsection
                        else:
                            if xsec != p.xsection:
                                print("  Warning: more than one possible match found for this dataset, and they do not have the same cross-section. I do not know what to do...")
                                xsec = None
                                break
                    if xsec:
                        print("  Updating with cross-section = {}".format(xsec))
                        if options.write:
                            sample.source_dataset.xsection = xsec


    if options.write:
        dbstore.commit()
    else:
        print("Currently running in dry-run mode. If you are happy with the change, pass the '-w' flag to this script to store the changes into the database.")
        dbstore.rollback()
#
# main
#
if __name__ == '__main__':
    main()
