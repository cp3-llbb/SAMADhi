#!/usr/bin/env python
""" Simple script to compute the luminosity of a set of samples """

from itertools import chain
import subprocess
import argparse
from cp3_llbb.SAMADhi.SAMADhi import Sample, SAMADhiDB
from cp3_llbb.SAMADhi.utils import replaceWildcards

def parse_luminosity_csv(result):
    """ Parse the CSV file produced by brilcalc, and return the total recorded luminosity in /pb """
    import csv
    import StringIO

    f = StringIO.StringIO(result)

    lumi = 0
    reader = csv.reader(f, delimiter=',')
    for row in reader:
        if not row:
            continue

        if row[0][0] == '#':
            continue
        lumi += float(row[-1])

    return lumi / 1000. / 1000.

def compute_luminosity(sample, local=False, normtag=None, username=None):
    print("Computing luminosity for %r") % str(sample.name)

    lumi = 0
    if not local:
        print("Running brilcalc on lxplus... You'll probably need to enter your lxplus password in a moment")
        print('')

        cmds = ['brilcalc', 'lumi', '--normtag', normtag, '--output-style', 'csv', '-i', '"%s"' % str(sample.processed_lumi.replace('"', ''))]
        cmd = 'export PATH="$HOME/.local/bin:/afs/cern.ch/cms/lumi/brilconda-1.1.7/bin:$PATH"; ' + ' '.join(cmds)
        ssh_cmds = ['ssh', '%s@lxplus.cern.ch' % username, cmd]
        brilcalc_result = subprocess.check_output(ssh_cmds)

        lumi = parse_luminosity_csv(brilcalc_result)
    else:
        print("Running brilcalc locally...")
        # FIXME one day
        print("Error: running brilcalc locally is not supported for the moment.")
        return 0

    print("Sample luminosity: %.3f /pb" % lumi)
    print('')

    # Update luminosity in the database
    sample.luminosity = lumi

    return lumi

def install_brilcalc(local=False, username=None):

    if local:
        print("Local installation of brilcalc is not supported.")
        return

    print("Installing brilcalc on lxplus... You'll probably need to enter your lxplus password in a moment")

    cmds = ['pip', 'install', '--install-option="--prefix=$HOME/.local"', '--upgrade', 'brilws']
    cmd = 'export PATH="$HOME/.local/bin:/afs/cern.ch/cms/lumi/brilconda-1.1.7/bin:$PATH"; %s' % (" ".join(cmds))
    ssh_cmds = ['ssh', '%s@lxplus.cern.ch' % username, cmd]
    subprocess.call(ssh_cmds)

def update_brilcalc(local=False, username=None):

    if local:
        print("Local installation of brilcalc is not supported.")
        return

    print("Updating brilcalc on lxplus... You'll probably need to enter your lxplus password in a moment")

    cmds = ['pip', 'install', '--install-option="--prefix=$HOME/.local"', '--upgrade', '--force-reinstall', 'brilws']
    cmd = 'export PATH="$HOME/.local/bin:/afs/cern.ch/cms/lumi/brilconda-1.1.7/bin:$PATH"; %s' % (" ".join(cmds))
    ssh_cmds = ['ssh', '%s@lxplus.cern.ch' % username, cmd]
    subprocess.call(ssh_cmds)

def main(args=None):
    parser = argparse.ArgumentParser(description='Compute luminosity of a set of samples')
    parser.add_argument('-i', '--id', type=int, nargs='+', dest='ids', help='IDs of the samples', metavar='ID')
    parser.add_argument('--name', type=str, nargs='+', dest='names', help='Names of the samples', metavar='NAME')
    parser.add_argument('--local', action='store_true', help='Run brilcalc locally instead of on lxplus')
    parser.add_argument('--bootstrap', action='store_true', help='Install brilcalc. Needs to be done only once')
    parser.add_argument('--update', action='store_true', help='Update brilcalc')
    parser.add_argument('-n', '--username', help='Remote lxplus username (local username by default)')
    parser.add_argument('-t', '--normtag', help='Normtag on /afs')
    parser.add_argument("--database", default="~/.samadhi", help="JSON Config file with database connection settings and credentials")
    options = parser.parse_args(args=args)

    if not options.bootstrap and not options.update and options.ids is None and options.names is None:
        parser.error('You must specify at least one sample id or sample name.')
    if not options.bootstrap and not options.update and not options.normtag:
        parser.error('You must specify a normtag file')
    if options.ids is None:
        options.ids = []
    if options.names is None:
        options.names = []
    if options.username is None:
        import pwd, os
        options.username = pwd.getpwuid(os.getuid()).pw_name

    if options.bootstrap:
        install_brilcalc(local=options.local, username=options.username)
    elif options.update:
        update_brilcalc(local=options.local, username=options.username)
    else:
        with SAMADhiDB(credentials=args.database):
            for sample in chain(
                    (Sample.get_by_id(id_) for id_ in options.ids),
                    (Sample.get_or_none(Sample.name % replaceWildcards(name)) for name in options.names)):
                compute_luminosity(sample, normtag=options.normtag, local=options.local, username=options.username)

if __name__ == '__main__':
    main()
