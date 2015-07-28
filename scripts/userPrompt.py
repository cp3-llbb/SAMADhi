#!/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/bin/python
from SAMADhi import Sample, Dataset

def confirm(prompt=None, resp=False):
    """prompts for yes or no response from the user. Returns True for yes and
    False for no. 'resp' should be set to the default value assumed by the caller when
    user simply types ENTER.
    >>> confirm(prompt='Create Directory?', resp=True)
    Create Directory? [y]|n: 
    True
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y: 
    False
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y: y
    True
    """
    if prompt is None:
        prompt = 'Confirm'
    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')
    while True:
        ans = raw_input(prompt)
        if not ans:
            return resp
        if ans not in ['y', 'Y', 'n', 'N']:
            print 'please enter y or n.'
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False

def parse_samples(inputString):
  """parse a comma-separated list of samples"""
  return [ int(x) for x in inputString.split(',') ]

def prompt_samples(store):
  """prompts for the source sample among the existing ones"""
  print "No source sample defined."
  print "Please select the samples associated with this result."
  # full list of samples
  print "Sample\t\tName"
  check = store.find(Sample)
  all_samples = check.values(Sample.sample_id,Sample.name)
  for dset in all_samples:
    print "%i\t\t%s"%(dset[0], dset[1])
  # prompt
  while True:
    try:
      return parse_samples(raw_input("Comma-separated list of sample id [None]?"))
    except:
      continue

def prompt_sample(sample,store):
  """prompts for the source sample among the existing ones"""
  print "Please select the sample associated with this sample."
  # full list of samples
  print "Sample\t\tName"
  check = store.find(Sample)
  all_samples = check.values(Sample.sample_id,Sample.name)
  for dset in all_samples:
    print "%i\t\t%s"%(dset[0], dset[1])
  # prompt
  while True:
    try:
      ans = int(raw_input("Sample id [None]?"))
    except:
      sample.source_sample_id = None
      return
    check = store.find(Sample,Sample.sample_id==ans)
    if check.is_empty(): continue
    else:
      sample.source_sample_id = ans
      return

def prompt_dataset(sample,store):
  """prompts for the source dataset among the existing ones"""
  print "Please select the dataset associated with this sample."
  # full list of datasets
  print "Dataset\t\tName"
  check = store.find(Dataset)
  all_datasets = check.values(Dataset.dataset_id,Dataset.name)
  for dset in all_datasets:
    print "%i\t\t%s"%(dset[0], dset[1])
  # datasets whose name contain the sample name
  check = store.find(Dataset,Dataset.name.contains_string(sample.name))
  if not check.is_empty():
    print "Suggestions:"
    print "Dataset\t\tName"
    suggested_datasets = check.values(Dataset.dataset_id,Dataset.name)
    for dset in suggested_datasets:
      print "%i\t\t%s"%(dset[0], dset[1])
  # prompt
  while True:
    try:
      ans = int(raw_input("Dataset id [None]?"))
    except:
      sample.source_dataset_id = None
      return
    check = store.find(Dataset,Dataset.dataset_id==ans)
    if check.is_empty(): continue
    else:
      sample.source_dataset_id = ans
      return

