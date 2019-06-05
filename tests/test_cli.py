import os.path
import pytest
from pytest_console_scripts import script_runner

testDBArg = "--database={0}".format(os.path.join(os.path.dirname(__file__), "data", "params.json"))

def checkSuccessOutLines(ret, nOut=None, nErr=None):
    print(ret.stdout)
    assert ret.success
    if nOut is not None:
        assert ( nOut == 0 and len(ret.stdout.strip()) == 0 ) or len(ret.stdout.strip().split("\n")) == nOut
    if nErr is not None:
        assert ( nErr == 0 and len(ret.stderr.strip()) == 0 ) or len(ret.stderr.strip().split("\n")) == nErr

def test_search_sample(script_runner):
    checkSuccessOutLines(script_runner.run("search_SAMADhi.py", "sample", "--name=test*", testDBArg), nOut=8, nErr=0)
