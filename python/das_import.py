import re
import json
import subprocess

from .SAMADhi import Dataset, DbStore
from .userPrompt import confirm

def do_das_query(query):
    """
    Execute das_client for the specified query, and return parsed JSON output
    """

    args = ['dasgoclient', '-json', '-format', 'json', '--query', query]
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
                 "dsize": u'file_size',
                 "globaltag": u'globalTag',
                 "xsection": u'xsection' }

  for column, key in conversion.items():
    setattr(dataset, column, dct[key])

  # special cases
  #dataset.creation_time = datetime.datetime.strptime(dct[u'creation_time'], "%Y-%m-%d %H:%M:%S")
  dataset.creation_time = datetime.datetime.fromtimestamp(dct[u'creation_time'])

  return dataset

def query_das(dataset):
    """
    Do a DAS request for the given dataset and return the metadata collected
    """

    summary_query  = "summary dataset=%s" % dataset
    metadata_query  = "dataset=%s" % dataset
    release_query  = "release dataset=%s" % dataset
    config_query  = "config dataset=%s system=dbs3" % dataset

    summary_results = do_das_query(summary_query)
    metadata_results = do_das_query(metadata_query)
    release_results = do_das_query(release_query)
    config_results = do_das_query(config_query)

    if not 'nresults' in summary_results:
        raise Exception("Invalid DAS response")

    if summary_results['nresults'] > 1:
        raise Exception("Error: more than one result for DAS query:%d"%summary_results['nresults'])

    # Grab results from DAS
    metadata = {}
    for d in metadata_results["data"][0]["dataset"]:
        for key, value in d.items():
            metadata[key] = value
    for d in summary_results["data"][0]["summary"]:
        for key, value in d.items():
            metadata[key] = value

    # Set release in global tag
    metadata.update({
        u'release': unicode(release_results["data"][0]["release"][0]["name"][0]),
        u'globalTag': unicode(config_results["data"][0]["config"][0]["global_tag"])
    })

    # Last chance for the global tag
    for d in config_results["data"]:
      if metadata[u'globalTag']==u'UNKNOWN':
        metadata[u'globalTag']=unicode(d["config"][0]["global_tag"])
    if metadata[u'globalTag']==u'UNKNOWN':
      del metadata[u'globalTag']

    return metadata

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

    metadata = query_das(dataset)

    metadata.update({
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
