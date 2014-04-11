import re
import httplib

class HTMLNode(object):
    def __init__(self, tag_, id_ = '', name_ = '', attr_ = None, nodes_ = None):
        self.tag = tag_
        self.attributes = {}
        if attr_:
            for key, value in attr_.items():
                self.attributes[key] = value
                
        if id_: self.attributes['id'] = id_
        if name_: self.attributes['name'] = name_

        self.daughters = []
        if nodes_:
            for node in nodes_:
                self.daughters.append(node)

    def addDaughter(self, tag_, id_ = '', name_ = '', attr_ = None, nodes_ = None):
        newnode = HTMLNode(tag_, id_, name_, attr_, nodes_)
        self.daughters.append(newnode)
        return newnode

    def addText(self, text_):
        self.daughters.append(text_.strip())

    def getDaughtersByTag(self, tag_):
        return filter(lambda n : n.tag == tag_, self.daughters)

    def findDaughtersByTag(self, tag_):
        result = []
        if self.tag == tag_: result.append(self)
        for node in self.daughters:
            try:
                result += node.findDaughtersByTag(tag_)
            except AttributeError:
                pass

        return result

    def generateHTML(self, indent = 0):
        html = ' ' * indent
        html += '<' + self.tag
        if self.attributes:
            for key, value in self.attributes.items():
                html += ' ' + key + '="' + value + '"'

        if len(self.daughters) == 0:
            html += ' />\n'
        else:
            html += '>\n'
            
            for node in self.daughters:
                try:
                    html += node.generateHTML(indent + 2)
                except AttributeError:
                    html += ' ' * (indent + 2) + node + '\n'

            html += ' ' * indent + '</' + self.tag + '>\n'

        return html

def createHTMLNode(html_):
    return _createHTMLNode([html_])

def _createHTMLNode(cont_):
    node = HTMLNode('')

    html = cont_[0].lstrip()

    matches = re.match('<\\s*(\\w+)([^>]*)>', html, re.MULTILINE)
    if not matches: return node

    node.tag = matches.group(1).lower()

    attrlist = re.findall('(\\w+)\\s*=\\s*(\'[^\']*\'|"[^"]*"|[^ ]*)', matches.group(2))
    for attr in attrlist:
        node.attributes[attr[0].lower()] = attr[1].strip('\'"')

    html = html[len(matches.group(0)):].lstrip()

    if len(matches.group(2)) == 0 or matches.group(2)[-1] != '/':
        while html:
            matches = re.match('(<\\s*\\w|</|[^<])', html)
            if not matches: break
    
            if matches.group(1) == '</':
                html = html[html.find('>') + 1:].lstrip()
                break
            elif '<' in matches.group(1):
                cont_[0] = html
                node.daughters.append(_createHTMLNode(cont_))
                html = cont_[0].lstrip()
            else:
                node.daughters.append(html[:html.find('<')].strip())
                html = html[html.find('<'):].lstrip()

    cont_[0] = html

    return node

def wbmRunParameters(run_):
    conn = httplib.HTTPConnection('cmswbm2.cms')
    conn.request("GET", '/cmsdb/servlet/RunParameters?RUN=' + str(run_))
    response = conn.getresponse()
    rows = createHTMLNode(response.read()).findDaughtersByTag('tr')
    data = {}
    for row in rows:
        try:
            key = row.daughters[0].daughters[0].generateHTML()
        except AttributeError:
            key = row.daughters[0].daughters[0]
        try:
            value = row.daughters[1].daughters[0].generateHTML()
        except AttributeError:
            value = row.daughters[1].daughters[0]
            
        data[key] = value

    return data
        
#import HTMLParser
#class KeyValueTableParser(HTMLParser.HTMLParser):
#
#    INITIAL = 0
#    KEYTD = 1
#    VALUETD = 2
#    
#    def __init__(self):
#        HTMLParser.HTMLParser.__init__(self)
#        self._state = KeyValueTableParser.INITIAL
#        self._currentKey = ''
#
#        self.data = {}
#        
#    def handle_starttag(self, tag_, attrs_):
#        if self._state == KeyValueTableParser.INITIAL and tag_ == 'td':
#            self._state = KeyValueTableParser.KEYTD
#        elif self._state == KeyValueTableParser.KEYTD:
#            self._state = KeyValueTableParser.VALUETD
#        else:
#            self._state = KeyValueTableParser.INITIAL
#            self._currentKey = ''
#
#    def handle_data(self, data_):
#        if self._state == KeyValueTableParser.KEYTD:
#            self._currentKey = data_.strip()
#        elif self._state == KeyValueTableParser.VALUETD:
#            self.data[self._currentKey] = data_.strip()
#            self._state = KeyValueTableParser.INITIAL
#            self._currentKey = ''
#        else:
#            self._state = KeyValueTableParser.INITIAL
#            self._currentKey = ''
#
#    def getData(self, url_):
#        self.data = {}
#        url = url_
#        if type(url) == str:
#            url = (url[:url.find('/')], url[url.find('/'):])
#
#        conn = httplib.HTTPConnection(url[0])
#        conn.request("GET", url[1])
#        response = conn.getresponse()
#        self.feed(response.read())


if __name__ == '__main__':
    # For module debugging
    
#    html = createHTMLNode('<HTML><HEAD><LINK TYPE="text/css" REL="stylesheet" HREF="../../daq/runSummary/style.css"/></HEAD></HTML>')
#    print html.generateHTML()

     print wbmRunParameters(221208)
