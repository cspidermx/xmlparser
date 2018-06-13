comps = {'instEducativas', 'VentaVehiculos', 'PorCuentadeTerceros', 'acreditamientoIEPS'}


def readcompC(tipo, root):
    if tipo not in comps:
        return None

    resp = {'schema': str(tipo).lower()}

    for k in root.attrib:
        if k.lower().find('schema') == -1:
            resp[k.lower()] = root.attrib[k]
    i = 0
    for child in root:
        i += 1
        chld = {}
        for k in child.attrib:
            if k.lower().find('schema') == -1:
                chld[k.lower()] = child.attrib[k]
        ii = 0
        for subchild in child:
            ii += 1
            schld = {}
            for k in subchild.attrib:
                if k.lower().find('schema') == -1:
                    schld[k.lower()] = subchild.attrib[k]
            iii = 0
            for ssubchild in subchild:
                iii += 1
                sschld = {}
                for k in ssubchild.attrib:
                    if k.lower().find('schema') == -1:
                        sschld[k.lower()] = ssubchild.attrib[k]
                schld[(ssubchild.tag[ssubchild.tag.find('}') + 1:len(ssubchild.tag)] + str(iii)).lower()] = sschld
            chld[(subchild.tag[subchild.tag.find('}') + 1:len(subchild.tag)] + str(ii)).lower()] = schld
        resp[(child.tag[child.tag.find('}') + 1:len(child.tag)] + str(i)).lower()] = chld

    return resp
