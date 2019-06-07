from __future__ import unicode_literals, print_function
import os, os.path
import pytest
from pytest_console_scripts import script_runner

needCredentials = pytest.mark.skipif(not os.path.isfile(os.path.expandvars(os.path.expanduser(os.getenv("SAMADHI_CREDENTIALS", "~/.samadhi")))), reason="Needs valid SAMADhi credentials")
dbArg = ("--database={0}".format(os.getenv("SAMADHI_CREDENTIALS")) if os.getenv("SAMADHI_CREDENTIALS") is not None else None)

def checkSuccessOutLines(ret, nOut=None, nErr=None):
    print(ret.stdout)
    assert ret.success
    if nOut is not None:
        assert ( nOut == 0 and len(ret.stdout.strip()) == 0 ) or len(ret.stdout.strip().split("\n")) == nOut
    if nErr is not None:
        assert ( nErr == 0 and len(ret.stderr.strip()) == 0 ) or len(ret.stderr.strip().split("\n")) == nErr

@needCredentials
def test_search_sample(script_runner):
    args = ["search_SAMADhi.py", "dataset", "--name=/DoubleMuon/Run2016*-03Feb2017-v*/MINIAOD"]
    if dbArg:
        args.append(dbArg)
    checkSuccessOutLines(script_runner.run(*args), nOut=5, nErr=0)
