#!/usr/bin/env python
from __future__ import unicode_literals, print_function
import argparse
from cp3_llbb.SAMADhi.das_import import import_cms_dataset

def main(args=None):
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

if __name__ == '__main__':
    main()
