from lxml import etree


def attrsextension(base, namespaces):
    resp = ''
    types = root.findall('xs:complexType', namespaces)
    if len(types) > 0:
        for t in types:
            for atr in t.attrib:
                if atr == 'name':
                    if base.find(t.attrib[atr]) != -1:
                        resp = ''
                        for attr in t:
                            if attr.tag.lower().find('attribute') != -1:
                                resp = resp + '\t' + getattrvalues(attr, namespaces) + "\n"
    return resp


def getattrvalues(attr, namespaces):
    name = ''
    use = ''
    fixed = ''
    base = ""
    for k in attr.attrib:
        if k.lower() == 'name':
            name = attr.attrib[k]
        else:
            if name == '':
                name = '??'
        if k.lower() == 'fixed':
            fixed = attr.attrib[k]
        else:
            if fixed == '':
                fixed = ''
        if k.lower() == 'use':
            if attr.attrib[k].lower() == 'required':
                use = 'NOT NULL'
            else:
                if use == '':
                    use = ''
        if k.lower() == 'base':
            base = attr.attrib[k]
        else:
            if base == '':
                base = '??'
    if name != '??':
        return '\t' + name + '  ' + use + '  ' + fixed
    else:
        return attrsextension(base, namespaces)


def getroot(root, indent, namespaces):
    for element in root:
        if element.tag.lower().find('element') != -1:
            for k in element.attrib:
                if k.lower().find('name') != -1:
                    print('\t' * indent, 'Table:', element.attrib[k])
                if k.lower().find('type') != -1:
                    print('\t' * indent, attrsextension(element.attrib[k], namespaces))
            for data in element:
                if data.tag.lower().find('complextype') != -1:
                    for attr in data:
                        if attr.tag.lower().find('attribute') != -1:
                            print('\t' * indent, getattrvalues(attr, namespaces))
                    for attr in data:
                        if attr.tag.lower().find('sequence') != -1:
                            getroot(attr, indent + 1, namespaces)
                    for attr in data:
                        if attr.tag.lower().find('choice') != -1:
                            getroot(attr, indent + 1, namespaces)
                    for attr in data:
                        if attr.tag.lower().find('complexcontent') != -1:
                            getroot(attr, indent + 1, namespaces)
        if element.tag.lower().find('choice') != -1:
            getroot(element, indent + 1, namespaces)
        if element.tag.lower().find('extension') != -1:
            print('\t' * indent, getattrvalues(element, namespaces))


tree = etree.parse('C:\\Users\\Charly\\Dropbox\\Work\\CFDIs\\XSDs\\Complementos\\Recepción de pagos​\\Pagos10.xsd')
root = tree.getroot()
namespaces = root.nsmap
getroot(root, 0, namespaces)
