#! /usr/bin/env python
""" Simple script to compute the luminosity of a set of samples """

# Storms package
import sys
sys.path.append('/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/lib/python2.7/site-packages/storm-0.20-py2.7-linux-x86_64.egg')
sys.path.append('/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/lib/python2.7/site-packages/MySQL_python-1.2.3-py2.7-linux-x86_64.egg')

import subprocess
import argparse
from SAMADhi import Sample, DbStore

def get_options():
    parser = argparse.ArgumentParser(description='Compute luminosity of a set of samples.')

    parser.add_argument('-i', '--id', type=int, nargs='+', dest='ids', help='IDs of the samples', metavar='ID')
    parser.add_argument('--name', type=str, nargs='+', dest='names', help='Names of the samples', metavar='NAME')

    parser.add_argument('--local', dest='local', action='store_true', help='Run brilcalc locally instead of on lxplus')

    parser.add_argument('--bootstrap', dest='bootstrap', action='store_true', help='Install brilcalc. Needs to be done only once')

    parser.add_argument('-n', '--username', dest='username', help='Remote lxplus username (local username by default)')

    options = parser.parse_args()

    if not options.bootstrap and options.ids is None and options.names is None:
        parser.error('You must specify at least one sample id or sample name.')

    if options.ids is None:
        options.ids = []

    if options.names is None:
        options.names = []

    if options.username is None:
        import pwd, os
        options.username = pwd.getpwuid(os.getuid()).pw_name

    return options

def get_sample(id, name):

    dbstore = DbStore()

    if id is not None:
        result = dbstore.find(Sample, Sample.sample_id == id)
    elif name is not None:
        result = dbstore.find(Sample, Sample.name.like(unicode(name.replace('*', '%').replace('?', '_'))))

    return result.one()

def parse_luminosity_csv(result):
    """ Parse the CSV file produced by brilcalc, and return the total recorded luminosity in /pb """
    import csv
    import StringIO

    f = StringIO.StringIO(result)

    lumi = 0
    reader = csv.reader(f, delimiter=',')
    for row in reader:
        if row[0][0] == '#':
            continue
        lumi += float(row[-1])

    return lumi / 1000. / 1000.

def compute_luminosity(sample, options):
    print("Computing luminosity for %r") % str(sample.name)

    lumi = 0
    if not options.local:
        print("Running brilcalc on lxplus... You'll probably need to enter your lxplus password in a moment")
        print('')

        cmds = ['brilcalc', 'lumi', '--normtag', '~lumipro/public/normtag_file/OfflineNormtagV2.json', '--output-style', 'csv', '-i', '"%s"' % str(sample.processed_lumi.replace('"', ''))]
        cmd = 'export PATH="$HOME/.local/bin:/afs/cern.ch/cms/lumi/brilconda-1.0.3/bin:$PATH"; ' + ' '.join(cmds)
        ssh_cmds = ['ssh', '%s@lxplus.cern.ch' % options.username, cmd]
        brilcalc_result = subprocess.check_output(ssh_cmds)

        lumi = parse_luminosity_csv(brilcalc_result)
    else:
        print("Running brilcalc locally...")
        # FIXME one day
        print("Error: running brilcalc locally is not supported for the moment.")
        return 0

    print("Sample luminosity: %.3f /pb" % lumi)
    print('')

    store = DbStore()
    # Update luminosity in the database
    store.find(Sample, Sample.sample_id == sample.sample_id).set(luminosity = lumi)

    store.commit()

    return lumi

def install_brilcalc(options):

    if options.local:
        print("Local installation of brilcalc is not supported.")
        return

    print("Installing brilcalc on lxplus... You'll probably need to enter your lxplus password in a moment")

    cmds = ['pip', 'install', '--install-option="--prefix=$HOME/.local"', '--upgrade', 'brilws']
    cmd = 'export PATH="$HOME/.local/bin:/afs/cern.ch/cms/lumi/brilconda-1.0.3/bin:$PATH"; %s' % (" ".join(cmds))
    ssh_cmds = ['ssh', '%s@lxplus.cern.ch' % options.username, cmd]
    subprocess.call(ssh_cmds)

def main():

    options = get_options()

    if options.bootstrap:
        install_brilcalc(options)
        return

    for id_ in options.ids:
        sample = get_sample(id_, None)
        compute_luminosity(sample, options)

    for name in options.names:
        sample = get_sample(None, name)
        compute_luminosity(sample, options)


#
# main
#
if __name__ == '__main__':
    main()
