#!/usr/bin/env python

# Script to add a sample to the database

import cp3_llbb.SAMADhi.SAMADhi as SAMADhi
from cp3_llbb.SAMADhi.SAMADhi import SAMADhiDB
from cp3_llbb.SAMADhi.utils import replaceWildcards

import os.path
import argparse

def main(args=None):
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

    objCls = getattr(SAMADhi, args.type.capitalize())
    idAttrName = "{0}_id".format(args.type)

    with SAMADhiDB(credentials=args.database):
        qry = objCls.select()
        if args.id:
            qry = qry.where(getattr(objCls, idAttrName) == args.id)
        elif args.name:
            qry = qry.where(objCls.name % replaceWildcards(args.name))
        elif args.path:
            qry = qry.where(objCls.path % replaceWildcards(args.path))
        results = qry.order_by(getattr(objCls, idAttrName))

        if args.long:
            for entry in results:
                print(str(entry))
                print(86*"-")
        else:
            fmtStr = "{{0.{0}}}\t{{0.{1}}}".format(idAttrName,
                    ("name" if args.type not in ("result", "analysis") else "description"))
            for res in results:
                print(fmtStr.format(res))

if __name__ == "__main__":
    main()
