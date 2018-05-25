import xml.etree.ElementTree as etree
import os
import csv
from dateutil import parser
from dateutil.tz import gettz
from datetime import datetime

# tree = etree.parse('For3_3Testing_1.xml')
tree = etree.parse('For3_3Testing_2.xml')
root = tree.getroot()
if root.tag.lower().find('comprobante') == -1:
    print('ERROR')
for k in root.attrib:
    # Version(3.3) | *Serie | *Folio | Fecha(AAAA-MM-DDThh:mm:ss) | Sello | *FormaPago(c_FormaPago) |
    # NoCertificado | Certificado | *CondicionesDePago(Texto Libre) | SubTotal | *Descuento |
    # Moneda(c_Moneda) | *TipoCambio | Total | TipoDeComprobante(c_TipoDeComprobante) |
    # *MetodoPago | LugarExpedicion(Codigo Postal) | *Confirmacion
    print(k, ' = ', root.attrib[k])  # key, 'corresponds to', d[key]

for child in root:
    if not child.tag.lower().find('cfdirelacionados') == -1:  # * Opcional
        for k in child.attrib:
            # TipoRelacion(c_TipoRelacion)
            print(k, ' = ', child.attrib[k])
        for subchild in child:
            if not subchild.tag.lower().find('cfdirelacionado') == -1:
                for k in subchild.attrib:
                    # UUID
                    print(k, ' = ', subchild.attrib[k])

    if not child.tag.lower().find('emisor') == -1:
        for k in child.attrib:
            # RFC | *Nombre | RegimenFiscal(c_RegimenFiscal)
            print(k, ' = ', child.attrib[k])

    if not child.tag.lower().find('receptor') == -1:
        for k in child.attrib:
            # RFC | *Nombre | *ResidenciaFiscal | *NumRegIdTrib | UsoCFDI(c_UsoCFDI)
            print(k, ' = ', child.attrib[k])

    if not child.tag.lower().find('conceptos') == -1:
        for subchild in child:
            if not subchild.tag.lower().find('concepto') == -1:
                for k in subchild.attrib:
                    # ClaveProdServ(c_ClaveProdServ) | *NoIdentificacion | Cantidad | ClaveUnidad(c_ClaveUnidad)
                    # *Unidad | Descripcion | ValorUnitario | Importe | *Descuento |
                    print(k, ' = ', subchild.attrib[k])
                for ssubchild in child:
                    if not ssubchild.tag.lower().find('impuestos') == -1:  # * Opcional
                        for sssubchild in child:
                            if not sssubchild.tag.lower().find('traslados') == -1:   # * Opcional
                                for ssssubchild in child:
                                    if not ssssubchild.tag.lower().find('traslado') == -1:
                                        for k in ssssubchild.attrib:
                                            # Base | Impuesto(c_Impuesto) | TipoFactor(c_TipoFactor) |
                                            # *TasaOCuota(c_TasaOCuota) | *Importe
                                            print(k, ' = ', ssssubchild.attrib[k])
                            if not sssubchild.tag.lower().find('retenciones') == -1:  # * Opcional
                                for ssssubchild in child:
                                    if not ssssubchild.tag.lower().find('retencion') == -1:
                                        for k in ssssubchild.attrib:
                                            # Base | Impuesto(c_Impuesto) | TipoFactor(c_TipoFactor) |
                                            # TasaOCuota(c_TasaOCuota) | Importe
                                            print(k, ' = ', ssssubchild.attrib[k])
                    if not ssubchild.tag.lower().find('informacionaduanera') == -1:  # * Opcional
                        for k in ssubchild.attrib:
                            # NumeroPedimento
                            print(k, ' = ', ssubchild.attrib[k])
                    if not ssubchild.tag.lower().find('cuentapredial') == -1:  # * Opcional
                        for k in ssubchild.attrib:
                            # Numero
                            print(k, ' = ', ssubchild.attrib[k])
                    if not ssubchild.tag.lower().find('complementoconcepto') == -1:  # * Opcional
                        for sssubchild in ssubchild:
                            print(sssubchild.tag)  # o.o'
                            for k in sssubchild.attrib:
                                # ¯\_(ツ)_/¯
                                print(k, ' = ', sssubchild.attrib[k])
                    if not ssubchild.tag.lower().find('parte') == -1:  # * Opcional
                        for k in ssubchild.attrib:
                            # ClaveProdServ(c_ClaveProdServ) | *NoIdentificacion | Cantidad | *Unidad | Descripcion
                            # *ValorUnitario | Importe
                            print(k, ' = ', ssubchild.attrib[k])
                        for sssubchild in child:
                            if not sssubchild.tag.lower().find('informacionaduanera') == -1:  # * Opcional
                                for k in sssubchild.attrib:
                                    # NumeroPedimento
                                    print(k, ' = ', sssubchild.attrib[k])

    if not child.tag.lower().find('impuestos') == -1:
        for k in child.attrib:
            # *TotalImpuestosRetenidos | *TotalImpuestosTrasladados
            print(k, ' = ', child.attrib[k])
        for subchild in child:
            if not subchild.tag.lower().find('retenciones') == -1:
                for ssubchild in subchild:
                    if not ssubchild.tag.lower().find('retencion') == -1:
                        for k in ssubchild.attrib:
                            # Impuesto(c_Impuesto) | Importe
                            print(k, ' = ', ssubchild.attrib[k])
            if not subchild.tag.lower().find('traslados') == -1:
                for ssubchild in subchild:
                    if not ssubchild.tag.lower().find('traslado') == -1:
                        for k in ssubchild.attrib:
                            # Impuesto(c_Impuesto) | TipoFactor | TasaOCuota(c_TasaOCuota) | Importe
                            print(k, ' = ', ssubchild.attrib[k])

    if not child.tag.lower().find('complemento') == -1:  # * Opcional
        print('La definición es libre (dependiente de industria), siempre debe traer: ')
        for subchild in child:
            if not subchild.tag.lower().find('timbrefiscaldigital') == -1:
                for k in subchild.attrib:
                    #
                    print(k, ' = ', subchild.attrib[k])

    if not child.tag.lower().find('addenda') == -1:  # * Opcional
        print('La definición es libre')
