from lxml import etree
from compConcepto import readcompC
from compCFDI import readcomp
from dbmgmnt import dbopen, dbclose, dbinsert_cfdi, dbinsert_cfdi_rels, dbinsertemisor, dbinsertreceptor, \
    dbinsertimpuestos
from dbmgmnt import dbinsertconceptos, dbinsertcomplementos
import os
# from dateutil import parser
# from dateutil.tz import gettz
# from datetime import datetime


def storexml(cdfi_file):
    db = 'C:\\Users\\Charly\\Dropbox\\Work\\CFDIs\\CFDIs.sqlite'
    # db = 'E:\\Dropbox\\Dropbox\\Work\\CFDIs\\CFDIs.sqlite'
    dbcon = dbopen(db)
    tree = etree.parse(os.path.join(os.getcwd(), "XMLs\\" + cdfi_file))
    root = tree.getroot()
    # namespaces = root.nsmap
    if root.tag.lower().find('comprobante') == -1:
        print('ERROR')
    comp = root.findall('cfdi:Complemento', root.nsmap)[0]
    uuid = ''
    for child in comp:
        if not child.tag.lower().find('timbrefiscaldigital') == -1:
            for k in child.attrib:
                if str(k).upper() == 'UUID':
                    uuid = child.attrib[k]
    # Version(3.3) | *Serie | *Folio | Fecha(AAAA-MM-DDThh:mm:ss) | Sello | *FormaPago(c_FormaPago) |
    # NoCertificado | Certificado | *CondicionesDePago(Texto Libre) | SubTotal | *Descuento |
    # Moneda(c_Moneda) | *TipoCambio | Total | TipoDeComprobante(c_TipoDeComprobante) |
    # *MetodoPago | LugarExpedicion(Codigo Postal) | *Confirmacion
    cfdikeys = ('version', 'serie', 'folio', 'fecha', 'sello', 'formapago', 'nocertificado', 'certificado',
                'condicionesdepago', 'subtotal', 'descuento', 'moneda', 'tipocambio', 'total', 'tipodecomprobante',
                'metodopago', 'lugarexpedicion', 'confirmacion')
    cfdidata = dict.fromkeys(cfdikeys)  # Create data dictionary from definition
    rootdata = {k.lower(): v for k, v in root.attrib.items()}
    # cfdidata = {**cfdidata, **rootdata}  # Merge 2 dictionaries, keep data from second one
    cfdidata.update(rootdata)
    if 'UUID' not in cfdidata:
        cfdidata['UUID'] = uuid
    for k in rootdata:
        if not k.lower().find('schema') == -1:
            cfdidata.pop(k, None)
    incfdi = dbinsert_cfdi(dbcon, cfdidata)
    if incfdi != 'OK':
        print('Error al insertar ({})'.format(incfdi))
        raise SystemExit(0)

    for child in root:
        if not child.tag.lower().find('cfdirelacionados') == -1:  # * Opcional
            rels = {'nodo': 'cfdirelacionados', 'UUID': uuid}
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
            dbinsert_cfdi_rels(dbcon, rels)

        if not child.tag.lower().find('emisor') == -1:
            emisorkeys = ('rfc', 'nombre', 'regimenfiscal')
            emisor = dict.fromkeys(emisorkeys)
            emisor['UUID'] = uuid
            for k in child.attrib:
                # RFC | *Nombre | RegimenFiscal(c_RegimenFiscal)
                if k.lower().find('schema') == -1:
                    emisor[k.lower()] = child.attrib[k]
            dbinsertemisor(dbcon, emisor)

        if not child.tag.lower().find('receptor') == -1:
            receptorkeys = ('rfc', 'nombre', 'residenciafiscal', 'numregidtrib', 'usocfdi')
            receptor = dict.fromkeys(receptorkeys)
            receptor['UUID'] = uuid
            for k in child.attrib:
                # RFC | *Nombre | *ResidenciaFiscal | *NumRegIdTrib | UsoCFDI(c_UsoCFDI)
                if k.lower().find('schema') == -1:
                    receptor[k.lower()] = child.attrib[k]
            dbinsertreceptor(dbcon, receptor)

        if not child.tag.lower().find('conceptos') == -1:
            conceptos = {'UUID': uuid}
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
                                if not sssubchild.tag.lower().find('traslados') == -1:  # * Opcional
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
            impuestos = {'UUID': uuid, 'totalimpuestosretenidos': None, 'totalimpuestostrasladados': None}
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
            comp = {'nodo': 'complemento', 'UUID': uuid}
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

    dbcon.commit()
    dbclose(dbcon)


def cleandb():
    db = 'C:\\Users\\Charly\\Dropbox\\Work\\CFDIs\\CFDIs.sqlite'
    # db = 'E:\\Dropbox\\Dropbox\\Work\\CFDIs\\CFDIs.sqlite'
    dbcon = dbopen(db)
    dbcon.execute("Delete from `TimbreFiscalDigital`;")
    dbcon.execute("Delete from `Receptor`;")
    dbcon.execute("Delete from `Impuestos`;")
    dbcon.execute("Delete from `Emisor`;")
    dbcon.execute("Delete from `Conceptos`;")
    dbcon.execute("Delete from `C_Parte`;")
    dbcon.execute("Delete from `T_Impuestos`;")
    dbcon.execute("Delete from `CFDIrelacionados`;")
    dbcon.execute("Delete from `CFDI`;")
    dbcon.execute("Delete from `compC_instEducativas`;")
    dbcon.execute("Delete from `compC_acreditamientoIEPS`;")
    dbcon.execute("Delete from `compC_VentaVehiculos`;")
    dbcon.execute("Delete from `compC_PorCuentadeTerceros`;")
    dbcon.execute("Delete from `cC_PCdT_InformacionFiscalTercero`;")
    dbcon.execute("Delete from `T_Predial`;")
    dbcon.execute("Delete from `T_Aduana`;")
    dbcon.execute("Delete from `Donatarias`;")
    dbcon.execute("Delete from `Divisas`;")
    dbcon.execute("Delete from `ImpuestosLocales`;")
    dbcon.execute("Delete from `LeyendasFiscales`;")
    dbcon.execute("Delete from `PFintegranteCoordinado`;")
    dbcon.execute("Delete from `TuristaPasajeroExtranjero`;")
    dbcon.execute("Delete from `datosTransito`;")
    dbcon.execute("Delete from `Complemento_SPEI`;")
    dbcon.execute("Delete from `CFDIRegistroFiscal`;")
    dbcon.execute("Delete from `PagoEnEspecie`;")
    dbcon.execute("Delete from `ValesDeDespensa`;")
    dbcon.execute("Delete from `Conceptos_ValesDeDespensa`;")
    dbcon.execute("Delete from `Aerolineas`;")
    dbcon.execute("Delete from `Aerolineas_OtrosCargos`;")
    dbcon.execute("Delete from `INE`;")
    dbcon.execute("Delete from `INE_Entidad`;")
    dbcon.execute("Delete from `obrasarteantiguedades`;")
    dbcon.execute("Delete from `parcialesconstruccion`;")
    dbcon.execute("Delete from `Inmueble_parcialesconstruccion`;")
    dbcon.execute("Delete from `VehiculoUsado`;")
    dbcon.execute("Delete from `EstadoDeCuentaCombustible`;")
    dbcon.execute("Delete from `Concepto_EstadoDeCuentaCombustible`;")
    dbcon.execute("Delete from `ConsumoDeCombustibles`;")
    dbcon.execute("Delete from `Concepto_ConsumoDeCombustibles`;")
    dbcon.execute("Delete from `certificadodedestruccion`;")
    dbcon.execute("Delete from `VehiculoDestruido`;")
    dbcon.execute("Delete from `NotariosPublicos`;")
    dbcon.execute("Delete from `DescInmueble`;")
    dbcon.execute("Delete from `DatosOperacion`;")
    dbcon.execute("Delete from `DatosNotario`;")
    dbcon.execute("Delete from `DatosEnajenanteAdquiriente`;")
    dbcon.execute("Delete from `renovacionysustitucionvehiculos`;")
    dbcon.execute("Delete from `DecretoRenovSustitVehicular`;")
    dbcon.execute("Delete from `VehiculosUsadosEnajenadoPermAlFab`;")
    dbcon.execute("Delete from `VehiculoNuvoSemEnajenadoFabAlPerm`;")
    dbcon.execute("Delete from `DatosBancarios`;")
    dbcon.execute("Delete from `ComercioExterior`;")
    dbcon.execute("Delete from `Domicilio_CE`;")
    dbcon.execute("Delete from `Emisor_CE`;")
    dbcon.execute("Delete from `Propietario_CE`;")
    dbcon.execute("Delete from `Receptor_CE`;")
    dbcon.execute("Delete from `Destinatario_CE`;")
    dbcon.execute("Delete from `Mercancias_CE`;")
    dbcon.execute("Delete from `DescripcionesEspecificas_CE`;")
    dbcon.execute("Delete from `Pagos`;")
    dbcon.execute("Delete from `DoctoRelacionado_Pagos`;")
    dbcon.execute("Delete from `Nomina`;")
    dbcon.execute("Delete from `Emisor_Nomina`;")
    dbcon.execute("Delete from `EntidadSNCF`;")
    dbcon.execute("Delete from `Receptor_Nomina`;")
    dbcon.execute("Delete from `SubContratacion`;")
    dbcon.execute("Delete from `Percepciones`;")
    dbcon.execute("Delete from `Percepcion`;")
    dbcon.execute("Delete from `AccionesOTitulos`;")
    dbcon.execute("Delete from `HorasExtra`;")
    dbcon.execute("Delete from `JubilacionPensionRetiro`;")
    dbcon.execute("Delete from `SeparacionIndemnizacion`;")
    dbcon.execute("Delete from `Deducciones`;")
    dbcon.execute("Delete from `Deduccion`;")
    dbcon.execute("Delete from `OtrosPagos`;")
    dbcon.execute("Delete from `SubsidioAlEmpleo`;")
    dbcon.execute("Delete from `CompensacionSaldosAFavor`;")
    dbcon.execute("Delete from `Incapacidades`;")
    dbcon.commit()
    dbclose(dbcon)


cleandb()
# storexml("ejemploVentaVehiculos3_2.xml")
for f in os.listdir('.\\XMLs\\'):
    if f[-3:].lower() == 'xml':
        filename = f
        print(f)
        storexml(f)
        # cleandb()
