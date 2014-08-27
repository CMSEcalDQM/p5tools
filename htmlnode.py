import re
import httplib

class HTMLNode(object):

    _tmpText = ''

    def __init__(self, tag = '', id = '', name = '', attr = None, nodes = None, html = ''):
        self.attributes = {}
        self.daughters = []

        if tag:
            self.tag = tag
            if attr:
                for key, value in attr.items():
                    self.attributes[key] = value
                    
            if id: self.attributes['id'] = id
            if name: self.attributes['name'] = name
    
            if nodes:
                for node in nodes:
                    self.daughters.append(node)

        elif html:
            print 'html given:', html

            matches = re.match('<\\s*(\\w+)([^>]*)>', html, re.MULTILINE)

            if matches:
                # if html starts with a flag, that becomes the root node
                self.tag = matches.group(1).lower()
        
                attrlist = re.findall('(\\w+)\\s*=\\s*(\'[^\']*\'|"[^"]*"|[^ ]*)', matches.group(2))
                for attr in attrlist:
                    self.attributes[attr[0].lower()] = attr[1].strip('\'"')

                isSingle = matches.group(2) and matches.group(2)[-1] == '/'

                HTMLNode._tmpText = html[len(matches.group(0)):].lstrip()

                if isSingle:
                    return

            else:
                # html starts with a normal string
                self.tag = ''

                text = re.split('<[^>]*>', html, 1, re.MULTILINE)[0]
                self.attributes['text'] = text
        
                HTMLNode._tmpText = html[len(text):].lstrip()

            while HTMLNode._tmpText:
                if HTMLNode._tmpText.startswith('</'):
                    if not self.tag:
                        # we are in a daughter text tag -> return
                        return

                    closeMatch = re.match('</\\s*%s\\s*>' % self.tag, HTMLNode._tmpText, re.IGNORECASE)
                    if not closeMatch:
                        raise RuntimeError('Non-matching close tag: ' + HTMLNode._tmpText[:20])

                    HTMLNode._tmpText = HTMLNode._tmpText[len(closeMatch.group(0)):].lstrip()
                    return

                else:
                    self.daughters.append(HTMLNode(html = HTMLNode._tmpText))

        else:
            raise RuntimeError('HTMLNode needs tag or html')

    def addDaughter(self, tag = '', id = '', name = '', attr = None, nodes = None, html = ''):
        newnode = HTMLNode(tag = tag, id = id, name = name, attr = attr, nodes = nodes, html = '')
        self.daughters.append(newnode)
        return newnode

    def addText(self, text):
        self.daughters.append(HTMLNode(html = text.strip()))

    def getDaughtersByTag(self, tag):
        return filter(lambda n : n.tag == tag, self.daughters)

    def findDaughtersByTag(self, tag):
        result = []
        if self.tag == tag: result.append(self)
        for node in self.daughters:
            try:
                result += node.findDaughtersByTag(tag)
            except AttributeError:
                pass

        return result

    def generateHTML(self, indent = 0):
        html = ' ' * indent
        if self.tag:
            html += '<' + self.tag
            for key, value in self.attributes.items():
                html += ' ' + key + '="' + value + '"'
    
            if len(self.daughters) == 0:
                html += ' />\n'
            else:
                html += '>\n'
                
                for node in self.daughters:
                    html += node.generateHTML(indent + 2)
    
                html += ' ' * indent + '</' + self.tag + '>\n'
        else:
            html += self.attributes['text'] + '\n'

        return html
