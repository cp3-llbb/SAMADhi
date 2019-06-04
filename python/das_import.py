import re
import json
import subprocess

from .SAMADhi import Dataset, SAMADhiDB
from .utils import confirm_transaction

def do_das_query(query):
    """
    Execute das_client for the specified query, and return parsed JSON output
    """

    args = ['dasgoclient', '-json', '-format', 'json', '--query', query]
    result = subprocess.check_output(args)

    return json.loads(result)

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

    if 'nresults' not in summary_results:
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
        'release': release_results["data"][0]["release"][0]["name"][0],
        'globalTag': config_results["data"][0]["config"][0]["global_tag"]
    })

    # Last chance for the global tag
    for d in config_results["data"]:
      if metadata['globalTag']=='UNKNOWN':
        metadata['globalTag']=d["config"][0]["global_tag"]
    if metadata['globalTag']=='UNKNOWN':
      del metadata['globalTag']

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

    # definition of the conversion key -> column
    column_conversion = {
            "process": u'process', 
            "user_comment": u'comment',
            "energy": u'energy',
            "nevents": u'nevents',
            "cmssw_release": u'release',
            "dsize": u'file_size',
            "globaltag": u'globalTag',
            "xsection": u'xsection'
            }
    # columns of the dataset to create (if needed)
    dset_columns = dict((col, metadata[key]) for col, key in column_conversion.items())
    dset_columns["creation_time"] = datetime.datetime.fromtimestamp(metadata[u"creation_time"])

    with SAMADhiDB() as db:
        existing = Dataset.get_or_none(Dataset.name == metadata["name"])
        with confirm_transaction(db, "Insert into the database?" if existing is None else "Update this dataset?"):
            dataset, created = Dataset.get_or_create(
                    name=metadata["name"], datatype=metadata["datatype"],
                    defaults=dset_columns
                    )
            print(dataset)
