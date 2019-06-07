from __future__ import unicode_literals, print_function
import pytest

@pytest.fixture(scope="module")
def sqlitetestdb():
    from cp3_llbb.SAMADhi.SAMADhi import _models as MODELS
    from peewee import SqliteDatabase
    test_db = SqliteDatabase(":memory:")
    test_db.bind(MODELS, bind_refs=False, bind_backrefs=False)
    test_db.connect()
    test_db.create_tables(MODELS)
    yield
    test_db.drop_tables(MODELS)
    test_db.close()

def test_createAnalysis(sqlitetestdb):
    from cp3_llbb.SAMADhi.SAMADhi import Analysis
    ana = Analysis.create(
            cadiline="NP-20-001",
            contact="me <me@anywhere>",
            description="Evidence for new physics"
            )
    print(str(ana))

def test_createDataset_minimal(sqlitetestdb):
    from cp3_llbb.SAMADhi.SAMADhi import Dataset
    dset = Dataset.create(
            name="test_createDataset_minimal dataset",
            datatype="mc"
            )
    print(str(dset))

def test_createDataset_fullNoRel(sqlitetestdb):
    from cp3_llbb.SAMADhi.SAMADhi import Dataset
    from datetime import datetime, timedelta
    dset = Dataset.create(
            name="/NewPhysics/test_createDataset_fullNoRel/NANOAODSIM",
            datatype="mc",
            cmssw_release="CMSSW_10_6_0",
            dsize=1024,
            energy=14.,
            globaltag="mc_run2_106X_v0",
            nevents=1000,
            process="New Physics",
            xsection=.001,
            user_comment="Your favourite sample",
            creation_time=datetime.now()-timedelta(days=7),
            )
    print(str(dset))

def test_createSample_minimal(sqlitetestdb):
    from cp3_llbb.SAMADhi.SAMADhi import Sample
    smp = Sample.create(
            name="test_createSample_minimal sample",
            path="/test/sample/minimal",
            sampletype="NTUPLES",
            nevents_processed=1000
            )
    print(str(smp))

def test_createSample_fullNoRel(sqlitetestdb):
    from cp3_llbb.SAMADhi.SAMADhi import Sample
    smp = Sample.create(
            name="test_createSample_fullNoRel sample",
            path="/test/sample/fullNoRel",
            sampletype="NTUPLES",
            nevents_processed=1000,
            author="me <me@anywhere>",
            code_version="Framework_x.y.z_MyAnalysis_u.v.w",
            event_weight_sum=1000.0,
            extras_event_weight_sum="variations go here - not available",
            luminosity=2.,
            nevents=215,
            processed_lumi="almost all",
            user_comment="hello world"
            )
    print(str(smp))

def test_createFile(sqlitetestdb):
    from cp3_llbb.SAMADhi.SAMADhi import Sample, File
    smp = Sample.create(
            name="test_createFile_minimal sample",
            path="/test/sample/minimal_for_fie",
            sampletype="NTUPLES",
            nevents_processed=1000
            )
    from cp3_llbb.SAMADhi.SAMADhi import File
    f = File.create(
            lfn="/foo/bar/test_createFile_minimal.root",
            pfn="/my/storage/foo/bar/test_createFile_minimal.root",
            nevents=1,
            event_weight_sum=1.,
            sample=smp,
            )
    print(str(f))

def test_createResult_minimal(sqlitetestdb):
    from cp3_llbb.SAMADhi.SAMADhi import Result
    result = Result.create(
            path="/my/home/test_minimal_result.pdf"
            )
    print(str(result))

def test_createResult_fullNoRel(sqlitetestdb):
    from cp3_llbb.SAMADhi.SAMADhi import Result
    result = Result.create(
            path="/my/home/test_fullNoRel_result.pdf",
            author="me <me@anywhere>",
            description="An interesting result",
            elog="TODO",
            )
    print(str(result))

def test_modelRels(sqlitetestdb):
    from cp3_llbb.SAMADhi.SAMADhi import Analysis, Dataset, Sample, File, Result
    ana = Analysis.create(
            cadiline="NP-20-002",
            contact="me <me@anywhere>",
            description="Measurement of XY->ZUVW"
            )
    datasets = (
        [ Dataset.create(
            name="test_modelRels_data{0:d}".format(i),
            datatype="data"
            ) for i in range(3) ] +
        [ Dataset.create(
            name="test_modelRels_mc{0:d}".format(i),
            datatype="mc"
            ) for i in range(2) ]
        )
    samples = [
        Sample.create(
            name="test_modelRels_{0}".format(ds.name.split("_")[-1]),
            path="/test/sample/{0}".format(ds.name),
            sampletype="NTUPLES",
            nevents_processed=1000,
            source_dataset=ds,
            )
        for ds in datasets
        ]
    for smp in samples:
        for i in range(4):
            File.create(sample=smp, lfn="{0}/{1:d}.root".format(smp.name, i), pfn="/store/me{0}/{1:d}.root".format(smp.name, i), nevents=250, event_weight_sum=250)
    res1 = Result.create(
        analysis=ana,
        author="me <me@anywhere>",
        description="Preliminary result",
        elog="TODO",
        path="/home/my/ana"
        )
    res2 = Result.create(
        analysis=ana,
        author="me <me@anywhere>",
        description="Final result",
        elog="TODO",
        path="/home/my/paper"
        )
    print(str(ana))
    print(str(datasets[-1]))
    print(str(samples[-1]))
    print(str(res1))
    print(str(res2))
