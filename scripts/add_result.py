#!/usr/bin/env python
""" Add a result to the database """
import os.path
from datetime import datetime
import argparse
from cp3_llbb.SAMADhi.SAMADhi import Sample, Result, SAMADhiDB
from cp3_llbb.SAMADhi.userPrompt import confirm_transaction, prompt_samples

def parsePath(pth):
    import os.path
    import argparse
    pth = os.path.abspath(os.path.expandvars(os.path.expanduser(pth)))
    if not os.path.exists(pth) or not ( os.path.isdir(pth) or os.path.isfile(pth) ):
        raise argparse.ArgumentError("{0} is not an existing file or directory".format(pth))
    return pth

def userFromPath(pth):
    import os
    from pwd import getpwuid
    return getpwuid(os.stat(pth).st_uid).pw_name

def main(args=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=parsePath)
    parser.add_argument("-s", "--sample", dest="inputSamples", help="comma separated list of samples used as input to produce that result")
    parser.add_argument("-d", "--description", help="description of the result")
    parser.add_argument("-e", "--elog", help="elog with more details")
    parser.add_argument("-A", "--analysis", type=int, help="analysis whose result belong to")
    parser.add_argument("-a", "--author", help="author of the result. If not specified, is taken from the path")
    parser.add_argument("-t", "--time", help="result timestamp. If set to \"path\", timestamp will be taken from the path. Otherwise, it must be formated like YYYY-MM-DD HH:MM:SS")
    args = parser.parse_args(args=args)

    if args.author is None:
        args.author = userFromPath(args.path)
    if args.time == "path":
        time = datetime.fromtimestamp(os.path.getctime(args.path))
    elif args.time is not None:
        time = datetime.strptime(args.time, '%Y-%m-%d %H:%M:%S')
    else:
        time = datetime.now()

    with SAMADhiDB() as db:
        with confirm_transaction(db, "Insert into the database?"):
            result = Result.create(
                path=args.path,
                description=args.description,
                author=args.author,
                creation_time=time,
                elog=args.elog,
                analysis_id=args.analysis,
                )
            if args.inputSamples is None:
                inputSampleIDs = prompt_samples()
            else:
                inputSampleIDs = [ int(x) for x in opt.inputSamples.split(",") ]
            for smpId in inputsampleIDs:
                Sample.get_by_id(smpId).results.append(result)
            print(result)

if __name__ == '__main__':
    main()
