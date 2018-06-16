from lxml import etree
from compConcepto import readcompC
from compCFDI import readcomp
from dbmgmnt import dbopen, dbclose, dbinsertCFDI, dbinsertCFDIrels, dbinsertemisor, dbinsertreceptor, dbinsertimpuestos
from dbmgmnt import dbinsertconceptos, dbinsertcomplementos
import os
# from dateutil import parser
# from dateutil.tz import gettz
# from datetime import datetime

db = 'C:\\Users\\Charly\\Dropbox\\Work\\CFDIs\\CFDIs.sqlite'
dbcon = dbopen(db)
tree = etree.parse(os.path.join(os.getcwd(), "XMLs\\For3_3Testing_1.xml"))
root = tree.getroot()
namespaces = root.nsmap
if root.tag.lower().find('comprobante') == -1:
    print('ERROR')
comp = root.findall('cfdi:Complemento', root.nsmap)[0]
for child in comp:
    if not child.tag.lower().find('timbrefiscaldigital') == -1:
        for k in child.attrib:
            if str(k).upper() == 'UUID':
                UUID = child.attrib[k]
# Version(3.3) | *Serie | *Folio | Fecha(AAAA-MM-DDThh:mm:ss) | Sello | *FormaPago(c_FormaPago) |
# NoCertificado | Certificado | *CondicionesDePago(Texto Libre) | SubTotal | *Descuento |
# Moneda(c_Moneda) | *TipoCambio | Total | TipoDeComprobante(c_TipoDeComprobante) |
# *MetodoPago | LugarExpedicion(Codigo Postal) | *Confirmacion
CFDIkeys = ('version', 'serie', 'folio', 'fecha', 'sello', 'formapago', 'nocertificado', 'certificado',
            'condicionesdepago', 'subtotal', 'descuento', 'moneda', 'tipocambio', 'total', 'tipodecomprobante',
            'metodopago', 'lugarexpedicion', 'confirmacion')
cfdidata = dict.fromkeys(CFDIkeys)  # Create data dictionary from definition
rootdata = {k.lower(): v for k, v in root.attrib.items()}
cfdidata = {**cfdidata, **rootdata}  # Merge 2 dictionaries, keep data from second one
if 'UUID' not in cfdidata:
    cfdidata['UUID'] = UUID
for k in rootdata:
    if not k.lower().find('schema') == -1:
        cfdidata.pop(k, None)
dbinsertCFDI(dbcon, cfdidata)

for child in root:
    if not child.tag.lower().find('cfdirelacionados') == -1:  # * Opcional
        rels = {'nodo': 'cfdirelacionados', 'UUID': UUID}
        for k in child.attrib:
            # TipoRelacion(c_TipoRelacion)
            if k.lower().find('schema') == -1:
                rels[k.lower()] = child.attrib[k]
        for subchild in child:
            if not subchild.tag.lower().find('cfdirelacionado') == -1:
                i = 0
                for k in subchild.attrib:
                    # UUID
                    i += 1
                    if k.lower().find('schema') == -1:
                        rels[(k + str(i)).lower()] = subchild.attrib[k]
        dbinsertCFDIrels(dbcon, rels)

    if not child.tag.lower().find('emisor') == -1:
        emisorkeys = ('rfc', 'nombre', 'regimenfiscal')
        emisor = dict.fromkeys(emisorkeys)
        emisor['UUID'] = UUID
        for k in child.attrib:
            # RFC | *Nombre | RegimenFiscal(c_RegimenFiscal)
            if k.lower().find('schema') == -1:
                emisor[k.lower()] = child.attrib[k]
        dbinsertemisor(dbcon, emisor)

    if not child.tag.lower().find('receptor') == -1:
        receptorkeys = ('rfc', 'nombre', 'residenciafiscal', 'numregidtrib', 'usocfdi')
        receptor = dict.fromkeys(receptorkeys)
        receptor['UUID'] = UUID
        for k in child.attrib:
            # RFC | *Nombre | *ResidenciaFiscal | *NumRegIdTrib | UsoCFDI(c_UsoCFDI)
            if k.lower().find('schema') == -1:
                receptor[k.lower()] = child.attrib[k]
        dbinsertreceptor(dbcon, receptor)

    if not child.tag.lower().find('conceptos') == -1:
        conceptos = {'UUID': UUID}
        i = 0
        for subchild in child:
            if not subchild.tag.lower().find('concepto') == -1:
                i += 1
                concepto = {}
                for k in subchild.attrib:
                    # ClaveProdServ(c_ClaveProdServ) | *NoIdentificacion | Cantidad | ClaveUnidad(c_ClaveUnidad)
                    # *Unidad | Descripcion | ValorUnitario | Importe | *Descuento |
                    if k.lower().find('schema') == -1:
                        concepto[k.lower()] = subchild.attrib[k]
                w = 0
                for ssubchild in subchild:
                    if not ssubchild.tag.lower().find('impuestos') == -1:  # * Opcional
                        c_imp = {}
                        for sssubchild in ssubchild:
                            if not sssubchild.tag.lower().find('traslados') == -1:   # * Opcional
                                c_imp_tras = {}
                                j = 0
                                for ssssubchild in sssubchild:
                                    if not ssssubchild.tag.lower().find('traslado') == -1:
                                        j += 1
                                        for k in ssssubchild.attrib:
                                            # Base | Impuesto(c_Impuesto) | TipoFactor(c_TipoFactor) |
                                            # *TasaOCuota(c_TasaOCuota) | *Importe
                                            if k.lower().find('schema') == -1:
                                                c_imp_tras[k.lower()] = ssssubchild.attrib[k]
                                        c_imp['traslado' + str(j)] = c_imp_tras
                            if not sssubchild.tag.lower().find('retenciones') == -1:  # * Opcional
                                c_imp_ret = {}
                                kk = 0
                                for ssssubchild in sssubchild:
                                    if not ssssubchild.tag.lower().find('retencion') == -1:
                                        kk += 1
                                        for k in ssssubchild.attrib:
                                            # Base | Impuesto(c_Impuesto) | TipoFactor(c_TipoFactor) |
                                            # TasaOCuota(c_TasaOCuota) | Importe
                                            if k.lower().find('schema') == -1:
                                                c_imp_ret[k.lower()] = ssssubchild.attrib[k]
                                        c_imp['retencion' + str(kk)] = c_imp_ret
                        concepto['impuestos'] = c_imp
                    if not ssubchild.tag.lower().find('informacionaduanera') == -1:  # * Opcional
                        inf_ad = {}
                        for k in ssubchild.attrib:
                            # NumeroPedimento
                            if k.lower().find('schema') == -1:
                                inf_ad[k.lower()] = ssubchild.attrib[k]
                        concepto['informacionaduanera'] = inf_ad
                    if not ssubchild.tag.lower().find('cuentapredial') == -1:  # * Opcional
                        cta_pred = {}
                        for k in ssubchild.attrib:
                            # Numero
                            if k.lower().find('schema') == -1:
                                cta_pred[k.lower()] = ssubchild.attrib[k]
                        concepto['cuentapredial'] = cta_pred
                    if not ssubchild.tag.lower().find('complementoconcepto') == -1:  # * Opcional
                        for sssubchild in ssubchild:
                            # opciones de schema:
                            # instEducativas | VentaVehiculos | PorCuentadeTerceros | acreditamientoIEPS
                            s = sssubchild.tag
                            complemento = s[s.find('}') + 1:len(s)]
                            comp = readcompC(complemento, sssubchild)
                        if comp is not None:
                            concepto['complementoconcepto'] = comp
                    if not ssubchild.tag.lower().find('parte') == -1:  # * Opcional
                        pte = {}
                        w += 1
                        for k in ssubchild.attrib:
                            # ClaveProdServ(c_ClaveProdServ) | *NoIdentificacion | Cantidad | *Unidad | Descripcion
                            # *ValorUnitario | Importe
                            if k.lower().find('schema') == -1:
                                pte[k.lower()] = ssubchild.attrib[k]
                        ii = 0
                        for sssubchild in child:
                            if not sssubchild.tag.lower().find('informacionaduanera') == -1:  # * Opcional
                                ii += 1
                                for k in sssubchild.attrib:
                                    # NumeroPedimento
                                    if k.lower().find('schema') == -1:
                                        pte[(k + str(ii)).lower()] = sssubchild.attrib[k]
                        concepto['parte' + str(w)] = pte
                conceptos['concepto' + str(i)] = concepto
        dbinsertconceptos(dbcon, conceptos)
    if not child.tag.lower().find('impuestos') == -1:
        impuestos = {'UUID': UUID, 'totalimpuestosretenidos': None, 'totalimpuestostrasladados': None}
        for k in child.attrib:
            # *TotalImpuestosRetenidos | *TotalImpuestosTrasladados
            if k.lower().find('schema') == -1:
                impuestos[k.lower()] = child.attrib[k]
        impuestos['retenciones'] = None
        impuestos['traslados'] = None
        for subchild in child:
            if not subchild.tag.lower().find('retenciones') == -1:
                rtcs = {}
                i_rtc = 0
                for ssubchild in subchild:
                    if not ssubchild.tag.lower().find('retencion') == -1:
                        i_rtc += 1
                        rtc = {}
                        for k in ssubchild.attrib:
                            # Impuesto(c_Impuesto) | Importe
                            if k.lower().find('schema') == -1:
                                rtc[k.lower()] = ssubchild.attrib[k]
                        rtcs['retencion' + str(i_rtc)] = rtc
                impuestos['retenciones'] = rtcs
            if not subchild.tag.lower().find('traslados') == -1:
                trslds = {}
                i_trld = 0
                for ssubchild in subchild:
                    if not ssubchild.tag.lower().find('traslado') == -1:
                        i_trld += 1
                        trld = {}
                        for k in ssubchild.attrib:
                            # Impuesto(c_Impuesto) | TipoFactor | TasaOCuota(c_TasaOCuota) | Importe
                            if k.lower().find('schema') == -1:
                                trld[k.lower()] = ssubchild.attrib[k]
                        trslds['traslado' + str(i_trld)] = trld
                impuestos['traslados'] = trslds
        dbinsertimpuestos(dbcon, impuestos)
    if not child.tag.lower().find('complemento') == -1:  # * Opcional
        comp = {'nodo': 'complemento', 'UUID': UUID}
        for subchild in child:
            if not subchild.tag.lower().find('timbrefiscaldigital') == -1:
                tifidi = {}
                for k in subchild.attrib:
                    # Version | UUID | FechaTimbrado | RfcProvCertif | *Leyenda | SelloCFD |
                    # NoCertificadoSAT | SelloSAT
                    if k.lower().find('schema') == -1:
                        tifidi[k.lower()] = subchild.attrib[k]
                comp['timbrefiscaldigital'] = tifidi
            else:
                s = subchild.tag
                complemento = s[s.find('}') + 1:len(s)]
                comp[complemento.lower()] = readcomp(complemento, subchild)
        dbinsertcomplementos(dbcon, comp)
    if not child.tag.lower().find('addenda') == -1:  # * Opcional
        # print('La definición es personalizada por empresa')
        None

dbclose(dbcon)
