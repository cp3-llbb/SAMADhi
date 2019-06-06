import os.path
import pytest
from pytest_console_scripts import script_runner

testDBArg = "--database={0}".format(os.path.join(os.path.dirname(__file__), "data", "params.json"))

@pytest.fixture
def tmptestdbcopy(tmpdir):
    import shutil
    shutil.copy2(os.path.join(os.path.dirname(__file__), "data", "params.json"), str(tmpdir.join("params.json")))
    shutil.copy2(os.path.join(os.path.dirname(__file__), "data", "test.db"), str(tmpdir.join("test.db")))
    yield "--database={0}".format(str(tmpdir.join("params.json")))

def checkSuccessOutLines(ret, nOut=None, nErr=None):
    print(ret.stdout)
    if not ret.success:
        print(ret.stderr)
    assert ret.success
    if nOut is not None:
        assert ( nOut == 0 and len(ret.stdout.strip()) == 0 ) or len(ret.stdout.strip().split("\n")) == nOut
    if nErr is not None:
        assert ( nErr == 0 and len(ret.stderr.strip()) == 0 ) or len(ret.stderr.strip().split("\n")) == nErr

def test_search_sample(script_runner):
    checkSuccessOutLines(script_runner.run("search_SAMADhi.py", "sample", "--name=test*", testDBArg), nOut=8, nErr=0)

def test_add_sample(script_runner, tmptestdbcopy):
    checkSuccessOutLines(script_runner.run("add_sample.py", "--continue", tmptestdbcopy, "NTUPLES", "--name=test_cli_addSample_1", "--processed=-1", "--nevents=10", "--norm=2.", "--weight-sum=12.", "--lumi=0.3", "--code_version=0.1.0", "--comment='testing add_sample.py'", "--source_dataset=7", "--source_sample=8", "--author=pytest", "--files=/foo/bar/test_cli_addSample/1.root,/foo/bar/test_cli_addSample/2.root", "/tmp"))
    checkSuccessOutLines(script_runner.run("search_SAMADhi.py", tmptestdbcopy, "--long", "sample", "--name=test_cli_addSample_1"))
