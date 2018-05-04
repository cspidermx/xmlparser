import xml.etree.ElementTree as etree
import os
import csv
from dateutil import parser
from dateutil.tz import gettz
from datetime import datetime


def att(a, v):
    if a == "RFC":
        if v == 3.2:
            return 'rfc'
        else:
            return 'Rfc'
    if a == "RS":
        if v == 3.2:
            return 'nombre'
        else:
            return 'Nombre'
    if a == "FECHA":
        if v == 3.2:
            return 'fecha'
        else:
            return 'Fecha'
    if a == "SERIE":
        if v == 3.2:
            return 'serie'
        else:
            return 'Serie'
    if a == "FOLIO":
        if v == 3.2:
            return 'folio'
        else:
            return 'Folio'
    if a == "SUBTOT":
        if v == 3.2:
            return 'subTotal'
        else:
            return 'SubTotal'
    if a == "IMPUESTOS":
        if v == 3.2:
            return 'totalImpuestosTrasladados'
        else:
            return 'TotalImpuestosTrasladados'
    if a == "TOT":
        if v == 3.2:
            return 'total'
        else:
            return 'Total'
    if a == "OTCG":
        if v == 3.2:
            return 'TotalCargos'
        else:
            return 'TotalCargos'


def parseCFDI(fln):
    tree = etree.parse(fln)
    root = tree.getroot()
    v = tua = otcr = imp = 0
    rfce = rse = rfcr = rsr = uuid = ''
    tzinfos = {"CST": gettz("America/Mexico_City")}
    print('ARCHIVO: ', fln)
    if 'version' in root.attrib:
        # print('VERSION: ', root.attrib['version'])
        v = 3.2
    else:
        if 'Version' in root.attrib:
            # print('VERSION: ', root.attrib['Version'])
            v = 3.3
    if v == 0:
        return None
    # print('FECHA: ', root.attrib[att('FECHA', v)])
    fch = parser.parse(root.attrib[att('FECHA', v)], tzinfos=tzinfos).strftime('%d/%m/%Y %H:%M:%S')
    fecha = datetime.strptime(fch, '%d/%m/%Y %H:%M:%S')
    if att('SERIE', v) in root.attrib:
        folio = root.attrib[att('SERIE', v)] + " - " + root.attrib[att('FOLIO', v)]
        # print('FOLIO: ', root.attrib[att('SERIE', v)], "-", root.attrib[att('FOLIO', v)])
    else:
        folio = str(root.attrib[att('FOLIO', v)])
        # print('FOLIO: ', root.attrib[att('FOLIO', v)])
    try:
        folio = int(folio)
        folio = "'" + str(folio)
    except:
        folio = folio
    sbt = float(root.attrib[att('SUBTOT', v)])
    # print('SUBTOTAL: ', '${:0,.2f}'.format(sbt).replace('$-', '-$'))
    tot = float(root.attrib[att('TOT', v)])
    # print('TOTAL: ', '${:0,.2f}'.format(tot).replace('$-', '-$'))
    for child in root:
        if child.tag.find('Emisor') > 0:
            rfce = child.attrib[att('RFC', v)]
            rse = child.attrib[att('RS', v)]
            # print('RFC EMISOR: ', child.attrib[att('RFC', v)])
            # print('RAZON SOCIAL EMISOR: ', child.attrib[att('RS', v)])
        else:
            if child.tag.find('Receptor') > 0:
                rfcr = child.attrib[att('RFC', v)]
                rsr = child.attrib[att('RS', v)]
                # print('RFC RECEPTOR: ', child.attrib[att('RFC', v)])
                # print('RAZON SOCIAL RECEPTOR: ', child.attrib[att('RS', v)])
            else:
                if child.tag.find('Complemento') > 0:
                    for sch in child:
                        if sch.tag.find('TimbreFiscalDigital') > 0:
                            uuid = sch.attrib['UUID']
                            # print('UUID: ', sch.attrib['UUID'])
                        else:
                            if sch.tag.find('Aerolineas') > 0:
                                tua = float(sch.attrib['TUA'])
                                # print('TUA: ', '${:0,.2f}'.format(tua).replace('$-', '-$'))
                                for ssch in sch:
                                    if ssch.tag.find('OtrosCargos') > 0:
                                        if att('OTCG', v) in ssch.attrib:
                                            otcr = float(ssch.attrib[att('OTCG', v)])
                                            # print('OTROS CARGOS: ', '${:0,.2f}'.format(otcr).replace('$-', '-$'))
                else:
                    if child.tag.find('Impuestos') > 0:
                        imp = float(child.attrib[att('IMPUESTOS', v)])
                        # print('IMPUESTOS: ', '${:0,.2f}'.format(imp).replace('$-', '-$'))
    dtdict = {'ARCHIVO': fln, 'VERSION': v, 'FECHA': fecha, 'FOLIO': folio, 'SUBTOTAL': sbt, 'TOTAL': tot,
                'RFC EMISOR': rfce, 'EMISOR': rse, 'RFC RECEPTOR': rfcr, 'RECEPTOR': rsr, 'IMPUESTOS': imp,
                'TUA': tua, 'OTROS CARGOS': otcr, 'UUID': uuid}
    return dtdict


fieldnames = ['ARCHIVO', 'VERSION', 'FECHA', 'FOLIO', 'SUBTOTAL', 'TOTAL', 'RFC EMISOR', 'EMISOR', 'RFC RECEPTOR',
              'RECEPTOR', 'IMPUESTOS', 'TUA', 'OTROS CARGOS', 'UUID']
csvfile2 = open('CFDIs.csv', 'w', newline='')
writer = csv.DictWriter(csvfile2, fieldnames=fieldnames)
writer.writeheader()
for f in os.listdir('.'):
    if f[-3:] == 'xml':
        filename = f
        datadict = parseCFDI(filename)
        if datadict is not None:
            try:
                writer.writerow(datadict)
            except:
                print('Error al extraer datos')
csvfile2.close()

'''
filename = '3_2_Aerolinea_33C72F86-6BAE-4029-91F9-1B9CF5588607.xml'
parseCFDI(filename)
print('-------------------------------------------------------------')
filename = '3_3 _ CFDI Estandar AC 20180-5.xml'
parseCFDI(filename)
'''
