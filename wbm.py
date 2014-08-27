import re
import httplib

from htmlnode import HTMLNode

class WBM(object):

    HOST = 'cmswbm2.cms'

    def __init__(self):
        self._conn = httplib.HTTPConnection(WBM.HOST)

    def __del__(self):
        self.close()

    def close(self):
        self._conn.close()

    def request(self, req):
        try:
            self._conn.request("GET", req)
            return self._conn.getresponse()
        except httplib.HTTPException:
            self._conn.close()
            self._conn = httplib.HTTPConnection(WBM.HOST)
            self._conn.request("GET", req)
            return self._conn.getresponse()

    def getRunParameters(self, run, keys = []):
        if len(keys):
            for key in keys:
        try:
            self._conn.request("GET", '/cmsdb/servlet/RunParameters?RUN=' + str(run))
            response = self._conn.getresponse()
        except httplib.HTTPException:
            self._conn.close()
            self._conn = httplib.HTTPConnection('cmswbm2.cms')
            self._conn.request("GET", '/cmsdb/servlet/RunParameters?RUN=' + str(run))
            response = self._conn.getresponse()

        rows = HTMLNode(html = response.read()).findDaughtersByTag('tr')
        data = {}
        for row in rows:
            key = row.daughters[0].daughters[0].generateHTML()
    
            if len(keys) and key not in keys: continue
    
            value = row.daughters[1].daughters[0].generateHTML()
                
            data[key] = value
    
        return data
    
    # Functions above and below are independent - will be nice to use the same APIs
    
    def _getParamSingle(self, run, key):

        try:
            self._conn.request('GET', '/cmsdb/servlet/RunParameters?RUNNUMBER=%d&NAME=%s' % (run, key.upper()))
            response = self._conn.getresponse()
        except httplib.HTTPException:
            self._conn.close()
            self._conn = httplib.HTTPConnection('cmswbm2.cms')
            self._conn.request('GET', '/cmsdb/servlet/RunParameters?RUNNUMBER=%d&NAME=%s' % (run, key.upper()))
            response = self._conn.getresponse()

        if response.status != 200:
            return ''
    
        data = response.read().replace('\n', '').replace('\r', '').lower()
        tosearch = '<td>%s</td><td>' % key.lower()
        index = data.find(tosearch) + len(tosearch)
        return data[index:data.find('</td>', index)]
    
    
    def getParam(self, run, key):
    
        if type(key) is list:
            result = []
            for k in key:
                result.append(self._getParamSingle(run, k))
    
            return result
        else:
            return self._getParamSingle(run, key)
    
    
    def findNewRun(self, last_):

        try:
            self._conn.request('GET', '/cmsdb/servlet/RunParameters')
            response = self._conn.getresponse()
        except httplib.HTTPException:
            self._conn.close()
            self._conn = httplib.HTTPConnection('cmswbm2.cms')
            self._conn.request('GET', '/cmsdb/servlet/RunParameters')
            response = self._conn.getresponse()

        if response.status != 200:
            return (0, False, False)
    
        data = response.read().replace('\n', '').replace('\r', '').lower()
        matches = re.search('href=runsummary[?]run=([0-9]+)', data)
        if not matches:
            return (0, False, False)
    
        current = int(matches.group(1))
    
        run = last_ + 1
        while run <= current:
            ecal, es = self.getParam(run, ['CMS.LVL0:ECAL', 'CMS.LVL0:ES'])
            if ecal == 'in' or es == 'in':
                return (run, ecal == 'in', es == 'in')
    
            run += 1
    
        return (0, False, False)


if __name__ == '__main__':
    # For module debugging
    
    import sys

    try:
        keys = sys.argv[1:]
    except:
        keys = []

    wbm = WBM()

    runParams = wbm.getRunParameters(225487, keys)
    for key, value in runParams.items():
        print key, value

    wbm.close()
