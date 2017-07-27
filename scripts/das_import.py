#! /usr/bin/env python

import argparse
import re
import json
import subprocess

from cp3_llbb.SAMADhi.SAMADhi import Dataset, DbStore
from cp3_llbb.SAMADhi.userPrompt import confirm

def do_das_query(query):
    """
    Execute das_client for the specified query, and return parsed JSON output
    """

    args = ['das_client', '--format', 'JSON', '--query', query]
    result = subprocess.check_output(args)

    return json.loads(result)

def fillDataset(dataset, dct):
  """
  Fill an instance of Dataset with values from a dictionnary
  """
  import datetime

  # definition of the conversion key -> column
  conversion = { "process": u'process', 
                 "user_comment": u'comment',
                 "energy": u'energy',
                 "nevents": u'nevents',
                 "cmssw_release": u'release',
                 "dsize": u'size',
                 "globaltag": u'globalTag',
                 "xsection": u'xsection' }

  for column, key in conversion.items():
    setattr(dataset, column, dct[key])

  # special cases
  dataset.creation_time = datetime.datetime.strptime(dct[u'creation_time'], "%Y-%m-%d %H:%M:%S")

  return dataset

def import_cms_dataset(dataset, process=None, energy=None, xsection=1.0, comment="", prompt=False):
    """
    Do a DAS request for the given dataset and insert it into SAMAdhi
    """

    # Guess default sane values for unspecifed parameters
    if not process:
        splitString = dataset.split('/', 2)
        if len(splitString) > 1:
            process = splitString[1]

    if not energy:
        energyRe = re.search(r"([\d.]+)TeV", dataset)
        if energyRe:
            energy = float(energyRe.group(1))


    metadata_query  = "dataset=%s | grep dataset.name, dataset.nevents, dataset.size, dataset.tag, dataset.datatype, dataset.creation_time" % dataset
    release_query  = "release dataset=%s | grep release.name" % dataset
    config_query  = "config dataset=%s | grep config.global_tag, config.name=cmsRun" % dataset

    metadata_results = do_das_query(metadata_query)
    release_results = do_das_query(release_query)
    config_results = do_das_query(config_query)

    if not 'nresults' in metadata_results:
        raise Exception("Invalid DAS response")

    if metadata_results['nresults'] > 1:
        raise Exception("Error: more than one result for DAS query")

    # Grab results from DAS
    metadata = {}
    for d in metadata_results["data"][0]["dataset"]:
        for key, value in d.items():
            metadata[key] = value

    metadata.update({
        u'release': unicode(release_results["data"][0]["release"][0]["name"]),
        u'globalTag': unicode(config_results["data"][0]["config"][0]["global_tag"]),
        u"process": unicode(process),
        u"xsection": xsection, 
        u"energy": energy,
        u"comment": unicode(comment)
    })

    # Connect to the database
    dbstore = DbStore()

    # Check if the dataset is already in the dataset
    update = False
    dbResult = dbstore.find(Dataset, Dataset.name == unicode(metadata['name']))
    if (dbResult.is_empty()):
        dataset = Dataset(metadata['name'], metadata['datatype'])
    else:
        update = True
        dataset = dbResult.one()

    fillDataset(dataset, metadata)

    if prompt:
        if not update:
            dbstore.add(dataset)
            dbstore.flush()

        print dataset
        prompt = "Insert into the database?" if not update else "Update this dataset?"
        if confirm(prompt=prompt, resp=True):
            dbstore.commit()
        else:
            dbstore.rollback()

    else:
        if not update:
            dbstore.add(dataset)
        dbstore.commit()


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
