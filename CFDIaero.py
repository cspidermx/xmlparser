import xml.etree.ElementTree as etree
import os
import csv
from dateutil import parser
from dateutil.tz import gettz
from datetime import datetime


v3_2 = {'RFC': 'rfc', 'RS': 'nombre', 'FECHA': 'fecha', 'SERIE': 'serie', 'FOLIO': 'folio', 'SUBTOT': 'subTotal',
        'IMPUESTOS': 'totalImpuestosTrasladados', 'TOT': 'total', 'OTCG': 'TotalCargos'}
v3_3 = {'RFC': 'Rfc', 'RS': 'Nombre', 'FECHA': 'Fecha', 'SERIE': 'Serie', 'FOLIO': 'Folio', 'SUBTOT': 'SubTotal',
        'IMPUESTOS': 'TotalImpuestosTrasladados', 'TOT': 'Total', 'OTCG': 'TotalCargos'}


def parseCFDI(fln):
    tree = etree.parse(fln)
    root = tree.getroot()
    cfdidict = {}
    v = tua = otcr = imp = 0
    rfce = rse = rfcr = rsr = uuid = ''
    tzinfos = {"CST": gettz("America/Mexico_City")}
    print('ARCHIVO: ', fln)
    if 'version' in root.attrib:
        cfdidict = v3_2
        v = 3.2
    else:
        if 'Version' in root.attrib:
            cfdidict = v3_3
            v = 3.3
    if v == 0:
        return None
    fch = parser.parse(root.attrib[cfdidict['FECHA']], tzinfos=tzinfos).strftime('%d/%m/%Y %H:%M:%S')
    fecha = datetime.strptime(fch, '%d/%m/%Y %H:%M:%S')
    if cfdidict['SERIE'] in root.attrib:
        folio = root.attrib[cfdidict['SERIE']] + " - " + root.attrib[cfdidict['FOLIO']]
    else:
        folio = str(root.attrib[cfdidict['FOLIO']])
    try:
        folio = int(folio)
        folio = "'" + str(folio)
    except:
        folio = folio
    sbt = float(root.attrib[cfdidict['SUBTOT']])
    tot = float(root.attrib[cfdidict['TOT']])
    for child in root:
        if child.tag.find('Emisor') > 0:
            rfce = child.attrib[cfdidict['RFC']]
            rse = child.attrib[cfdidict['RS']]
        else:
            if child.tag.find('Receptor') > 0:
                rfcr = child.attrib[cfdidict['RFC']]
                rsr = child.attrib[cfdidict['RS']]
            else:
                if child.tag.find('Complemento') > 0:
                    for sch in child:
                        if sch.tag.find('TimbreFiscalDigital') > 0:
                            uuid = sch.attrib['UUID']
                        else:
                            if sch.tag.find('Aerolineas') > 0:
                                tua = float(sch.attrib['TUA'])
                                for ssch in sch:
                                    if ssch.tag.find('OtrosCargos') > 0:
                                        if cfdidict['OTCG'] in ssch.attrib:
                                            otcr = float(ssch.attrib[cfdidict['OTCG']])
                else:
                    if child.tag.find('Impuestos') > 0:
                        imp = float(child.attrib[cfdidict['IMPUESTOS']])
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
