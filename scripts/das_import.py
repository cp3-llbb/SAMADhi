#!/usr/bin/env python

import argparse
from cp3_llbb.SAMADhi.das_import import import_cms_dataset

def main(args=None):
    parser = argparse.ArgumentParser(description='Import CMS datasets into SAMADhi')
    parser.add_argument("dataset", help="CMS dataset")
    parser.add_argument("-p", "--process", help="Process name")
    parser.add_argument("--xsection", type=float, default=1.0, help="Cross-section in pb")
    parser.add_argument("--energy", type=float, dest="energy", help="CoM energy, in TeV")
    parser.add_argument("--comment", default="", help="User defined comment")
    options = parser.parse_args(args=args)

    import_cms_dataset(options.dataset[0], options.process, options.energy, options.xsection, options.comment, True)

if __name__ == '__main__':
    main()
