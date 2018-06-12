#!/usr/bin/env python

import argparse

from cp3_llbb.SAMADhi.das_import import import_cms_dataset

def get_options():
    parser = argparse.ArgumentParser(description='Import CMS datasets into SAMADhi')

    parser.add_argument("-p", "--process", action="store", type=str, dest="process", help="Process name.")

    parser.add_argument("--xsection", action="store", type=float, default=1.0, dest="xsection", help="Cross-section in pb.")

    parser.add_argument("--energy", action="store", type=float, dest="energy", help="CoM energy, in TeV.")

    parser.add_argument("--comment", action="store", type=str, default="", dest="comment", help="User defined comment")

    parser.add_argument("dataset", action="store", type=str, nargs=1, help="CMS dataset")

    args = parser.parse_args()

    return args

if __name__ == '__main__':
    options = get_options()
    import_cms_dataset(options.dataset[0], options.process, options.energy, options.xsection, options.comment, True)
