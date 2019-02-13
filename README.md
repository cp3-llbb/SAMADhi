SAMADhi
=======

SAmple MAnagement Database

Samādhi in Hinduism, Buddhism, Jainism, Sikhism and yogic schools is a higher level of concentrated meditation, or dhyāna. In the yoga tradition, it is the eighth and final limb identified in the Yoga Sūtras of Patañjali.

This project is to develop a database to keep track of samples used by our group for CMS data analysis, and of (groups of) analysis results.

A python interface is provided via the STORM package.

Setup inside a CMSSW project area:
```
cmsrel $MY_CMSSW_VERSION

cd $MY_CMSSW_VERSION/src
cmsenv
git clone -o upstream https://github.com/cp3-llbb/SAMADhi.git cp3_llbb/SAMADhi
cd cp3_llbb/SAMADhi
source installdeps_cmssw.sh   ## only on first use
scram b
```

Standalone setup on ingrid:
```
source setup_standalone.sh    ## in every new shell
```
this will create an install tree and symlink if needed, and otherwise only set some environment variables.
The python installation used can be customized with the `--python` option (e.g. `--python=/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/bin/python` on `ingrid-ui2`),
and the install tree location can be set with the `--install` option.


To start the xataface interface in a docker image:
```
docker build -t samadhi-web .
docker run -d --name samadhi-frontend -p 8070:80 --link samadhi-mysql:mysql --rm  samadhi-web
```
where samadhi-mysql is a running mysql container configured with the proper database and set to use the default auth method (see example in database/).
