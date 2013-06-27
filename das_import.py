#!/usr/bin/env python

# This little script allows the importation of sample data from 
# the CMS Data Aggregation System (DAS).
# It is derived from the DAS CLI code that can be downloaded at
# https://cmsweb.cern.ch/das/cli

import sys
if  sys.version_info < (2, 6):
    raise Exception("DAS requires python 2.6 or greater")

import os
import re
import time
import json
import urllib
import urllib2
import httplib
from   optparse import OptionParser
from SAMADhi import Dataset

class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    """
    Simple HTTPS client authentication class based on provided
    key/ca information
    """
    def __init__(self, key=None, cert=None, level=0):
        if  level:
            urllib2.HTTPSHandler.__init__(self, debuglevel=1)
        else:
            urllib2.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        """Open request method"""
        #Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.get_connection, req)

    def get_connection(self, host, timeout=300):
        """Connection method"""
        if  self.key:
            return httplib.HTTPSConnection(host, key_file=self.key,
                                                cert_file=self.cert)
        return httplib.HTTPSConnection(host)

class DASOptionParser: 
    """
    DAS cache client option parser
    """
    def __init__(self):
        usage  = "Usage: %prog [options]\n"
        usage += "For more help please visit https://cmsweb.cern.ch/das/faq"
        self.parser = OptionParser(usage=usage)
        self.parser.add_option("-v", "--verbose", action="store", 
                               type="int", default=0, dest="verbose",
             help="verbose output")
        self.parser.add_option("--query", action="store", type="string", 
                               default=False, dest="query",
             help="specify query for your request")
        msg  = "host name of DAS cache server, default is https://cmsweb.cern.ch"
        self.parser.add_option("--host", action="store", type="string", 
                       default='https://cmsweb.cern.ch', dest="host", help=msg)
        msg  = "start index for returned result set, aka pagination,"
        msg += " use w/ limit (default is 0)"
        self.parser.add_option("--idx", action="store", type="int", 
                               default=0, dest="idx", help=msg)
        msg  = "number of returned results (default is 10),"
        msg += " use --limit=0 to show all results"
        self.parser.add_option("--limit", action="store", type="int", 
                               default=10, dest="limit", help=msg)
        msg  = 'query waiting threshold in sec, default is 5 minutes'
        self.parser.add_option("--threshold", action="store", type="int",
                               default=300, dest="threshold", help=msg)
        msg  = 'specify private key file name'
        self.parser.add_option("--key", action="store", type="string",
                               default="", dest="ckey", help=msg)
        msg  = 'specify private certificate file name'
        self.parser.add_option("--cert", action="store", type="string",
                               default="", dest="cert", help=msg)
        msg = 'specify number of retries upon busy DAS server message'
        self.parser.add_option("--retry", action="store", type="string",
                               default=0, dest="retry", help=msg)
        msg = 'drop DAS headers'
        self.parser.add_option("--das-headers", action="store_true",
                               default=False, dest="das_headers", help=msg)
    def get_opt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()

def fullpath(path):
    "Expand path to full path"
    if  path and path[0] == '~':
        path = path.replace('~', '')
        path = path[1:] if path[0] == '/' else path
        path = os.path.join(os.environ['HOME'], path)
    return path

def get_data(host, query, idx, limit, debug, threshold=300, ckey=None,
        cert=None, das_headers=True):
    """Contact DAS server and retrieve data for given DAS query"""
    params  = {'input':query, 'idx':idx, 'limit':limit}
    path    = '/das/cache'
    pat     = re.compile('http[s]{0,1}://')
    if  not pat.match(host):
        msg = 'Invalid hostname: %s' % host
        raise Exception(msg)
    url = host + path
    headers = {"Accept": "application/json"}
    encoded_data = urllib.urlencode(params, doseq=True)
    url += '?%s' % encoded_data
    req  = urllib2.Request(url=url, headers=headers)
    if  ckey and cert:
        ckey = fullpath(ckey)
        cert = fullpath(cert)
        hdlr = HTTPSClientAuthHandler(ckey, cert, debug)
    else:
        hdlr = urllib2.HTTPHandler(debuglevel=debug)
    opener = urllib2.build_opener(hdlr)
    fdesc = opener.open(req)
    data = fdesc.read()
    fdesc.close()

    pat = re.compile(r'^[a-z0-9]{32}')
    if  data and isinstance(data, str) and pat.match(data) and len(data) == 32:
        pid = data
    else:
        pid = None
    iwtime  = 2  # initial waiting time in seconds
    wtime   = 20 # final waiting time in seconds
    sleep   = iwtime
    time0   = time.time()
    while pid:
        params.update({'pid':data})
        encoded_data = urllib.urlencode(params, doseq=True)
        url  = host + path + '?%s' % encoded_data
        req  = urllib2.Request(url=url, headers=headers)
        try:
            fdesc = opener.open(req)
            data = fdesc.read()
            fdesc.close()
        except urllib2.HTTPError as err:
            return {"status":"fail", "reason":str(err)}
        if  data and isinstance(data, str) and pat.match(data) and len(data) == 32:
            pid = data
        else:
            pid = None
        time.sleep(sleep)
        if  sleep < wtime:
            sleep *= 2
        elif sleep == wtime:
            sleep = iwtime # start new cycle
        else:
            sleep = wtime
        if  (time.time()-time0) > threshold:
            reason = "client timeout after %s sec" % int(time.time()-time0)
            return {"status":"fail", "reason":reason}
    jsondict = json.loads(data)
    if  das_headers:
        return jsondict
    # drop DAS headers, users usually don't need them
    status = jsondict.get('status')
    if  status != 'ok':
        return jsondict
    drop_keys = ['das_id', 'cache_id', 'qhash', '_id', 'das']
    for row in jsondict['data']:
        for key in drop_keys:
            del row[key]
    return jsondict['data']

def main():
    """Main function"""
    optmgr  = DASOptionParser()
    opts, _ = optmgr.get_opt()
    host    = opts.host
    debug   = opts.verbose
    query   = opts.query
    #TODO: the query should just be a dataset and we should build here the proper DAS query
    # "dataset=/PYTHIA6_Tauola_TTbar_TuneZ2star_14TeV/Summer12-428SLHCstd_DESIGN42_V17-v3/AODSIM | grep dataset.name, dataset.nevents, dataset.size, dataset.tag, dataset.datatype"
    idx     = opts.idx
    limit   = opts.limit
    thr     = opts.threshold
    ckey    = opts.ckey
    cert    = opts.cert
    das_h   = opts.das_headers
    jsondict = get_data(host, query, idx, limit, debug, thr, ckey, cert, das_h)
    print(json.dumps(jsondict))
    #TODO: here we should do a json.loads with a properly defined object_hook. Example:
    #>>> def asDataset(dct):
    #...   if 'dataset' in dct:
    #...     return dct['dataset'][0]['nevents']
    #...   return dct
    #... 
    #>>> json.loads(jsoncontent, object_hook=asDataset)
    #[999183]
    #TODO: complement the object via command line options (process, cross-section, comment)


#
# main
#
if __name__ == '__main__':
    main()
