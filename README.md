SAMADhi
=======

SAmple MAnagement Database

Samādhi in Hinduism, Buddhism, Jainism, Sikhism and yogic schools is a higher level of concentrated meditation, or dhyāna. In the yoga tradition, it is the eighth and final limb identified in the Yoga Sūtras of Patañjali.

This project is to develop a database to keep track of samples used by our group for CMS data analysis, and of (groups of) analysis results.

A python interface is provided via the [peewee](http://docs.peewee-orm.com/en/latest/) package.

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

For standalone use the python interface can be installed with setuptools or pip,
e.g. in a [virtual environment](https://packaging.python.org/tutorials/installing-packages/#creating-virtual-environments) with
```bash
python -m venv samadhi_env
source samadhi_env/bin/activate
pip install git+https://github.com/cp3-llbb/SAMADhi.git
```


To start the xataface interface in a docker image:
```
docker build -t samadhi-web .
docker run -d --name samadhi-frontend -p 8070:80 --link samadhi-mysql:mysql --rm  samadhi-web
```
where samadhi-mysql is a running mysql container configured with the proper database and set to use the default auth method (see example in database/).
