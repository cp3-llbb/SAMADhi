from cp3_llbb.SAMADhi.SAMADhi import Sample, Dataset

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
            print('please enter y or n.')
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False

def prompt_samples():
    """prompts for the source sample among the existing ones"""
    print("No source sample defined.")
    print("Please select the samples associated with this result.")
    # full list of samples
    print("Sample\t\tName")
    for smp in Sample.select():
        print("%i\t\t%s"%(smp.sample_id, smp.name))
    # prompt
    while True:
        try:
            return [ int(x) for x in raw_input("Comma-separated list of sample id [None]?").split(",") ]
        except:
            continue

def prompt_sample(sample):
    """prompts for the source sample among the existing ones"""
    print("Please select the sample associated with this sample.")
    # full list of samples
    print("Sample\t\tName")
    for smp in Sample.select():
        print("%i\t\t%s"%(smp.sample_id, smp.name))
    # prompt
    while True:
        try:
            ans = int(raw_input("Sample id [None]?"))
        except:
            sample.source_sample_id = None
            return
        smp_db = Sample.get_or_none(Sample.sample_id == ans)
        if smp_db is not None:
            sample.source_sample_id = smp_db.sample_id
        else:
            continue 

def prompt_dataset(sample):
    """prompts for the source dataset among the existing ones"""
    print("Please select the dataset associated with this sample.")
    # full list of datasets
    print("Dataset\t\tName")
    for ds in Dataset.select():
        print("%i\t\t%s"%(ds.dataset_id, ds.name))
    # datasets whose name contain the sample name
    suggestions = Dataset.select().where(Dataset.name.contains(sample.name))
    if suggestions.count() > 0:
        print("Suggestions:")
        print("Dataset\t\tName")
        suggested_datasets = check.values(Dataset.dataset_id,Dataset.name)
        for ds in suggested_datasets:
            print("%i\t\t%s"%(ds.dataset_id, ds.name))
    # prompt
    while True:
        try:
            ans = int(raw_input("Dataset id [None]?"))
        except:
            sample.source_dataset_id = None
            return
        dset_db = Dataset.get_or_none(Dataset.dataset_idSample == ans)
        if dset_db is not None:
            sample.source_dataset_id = ans
        else:
            continue

from contextlib import contextmanager
@contextmanager
def confirm_transaction(db, prompt):
    with db.manual_commit():
        db.begin()
        try:
            yield
        except Exception as ex:
            db.rollback()
            raise ex
        else:
            try:
                if confirm(prompt=prompt, resp=True):
                    db.commit()
                else:
                    db.rollback()
            except Exception as ex:
                db.rollback()
                raise ex

@contextmanager
def maybe_dryrun(db, dryRun=False, dryMessage=None):
    with db.manual_commit():
        db.begin()
        try:
            yield
        except Exception as ex:
            db.rollback()
            raise ex
        else:
            try:
                if not dryRun:
                    db.commit()
                else:
                    if dryMessage:
                        print(dryMessage)
                    db.rollback()
            except Exception as ex:
                db.rollback()
                raise ex
