#!/usr/bin/env python
""" Add a result to the database """
from datetime import datetime
import argparse
from cp3_llbb.SAMADhi.SAMADhi import Sample, Result, SampleResult, SAMADhiDB
from cp3_llbb.SAMADhi.utils import parsePath, userFromPath, timeFromPath, confirm_transaction, prompt_samples

def main(args=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=parsePath)
    parser.add_argument("-s", "--sample", dest="inputSamples", help="comma separated list of samples used as input to produce that result")
    parser.add_argument("-d", "--description", help="description of the result")
    parser.add_argument("-e", "--elog", help="elog with more details")
    parser.add_argument("-A", "--analysis", type=int, help="analysis whose result belong to")
    parser.add_argument("-a", "--author", help="author of the result. If not specified, is taken from the path")
    parser.add_argument("-t", "--time", help="result timestamp. If set to \"path\", timestamp will be taken from the path. Otherwise, it must be formated like YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--database", default="~/.samadhi", help="JSON Config file with database connection settings and credentials")
    parser.add_argument("-y", "--continue", dest="assumeDefault", action="store_true", help="Assume defaults instead of prompt")
    args = parser.parse_args(args=args)

    if args.author is None:
        args.author = userFromPath(args.path)
    if args.time == "path":
        time = timeFromPath(args.path)
    elif args.time is not None:
        time = datetime.strptime(args.time, '%Y-%m-%d %H:%M:%S')
    else:
        time = datetime.now()

    with SAMADhiDB(credentials=args.database) as db:
        with confirm_transaction(db, "Insert into the database?", assumeDefault=args.assumeDefault):
            result = Result.create(
                path=args.path,
                description=args.description,
                author=args.author,
                creation_time=time,
                elog=args.elog,
                analysis=args.analysis,
                )
            if args.inputSamples is None:
                inputSampleIDs = prompt_samples()
            else:
                inputSampleIDs = [ int(x) for x in args.inputSamples.split(",") ]
            for smpId in inputSampleIDs:
                smp = Sample.get_or_none(Sample.id == smpId)
                if not smp:
                    print("Could not find sample #{0:d}".format(smpId))
                else:
                    SampleResult.create(sample=smp, result=result)
            print(result)

if __name__ == '__main__':
    main()
