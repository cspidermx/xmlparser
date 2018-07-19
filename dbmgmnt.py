import sqlite3
from datetime import datetime
import random
import string


def dbopen(db):
    try:
        con = sqlite3.connect(db)
    except sqlite3.Error as e:
        print(e)
        return None
    return con


def dbclose(con):
    try:
        con.close
    except sqlite3.Error as e:
        print(e)
        return False
    return True


def rndmstr(s):
    choices = string.ascii_lowercase + string.digits + string.ascii_uppercase
    return ''.join(random.SystemRandom().choice(choices) for _ in range(s))


def idcomp(c, tbla, field='id', size=5):
    cur = c.cursor()
    id_comp = rndmstr(size)
    sql = 'Select {0} from {1} where {0}="{2}"'.format(field, tbla, id_comp)
    while not len(cur.execute(sql).fetchall()) == 0:
        id_comp = rndmstr(size)
        sql = 'Select {0} from {1} where {0}="{2}"'.format(field, tbla, id_comp)
    cur.close()
    return id_comp


def convtipo(strdta, tipo):
    r = None
    if strdta is not None:
        if tipo == 'float':
            r = float(strdta)
        elif tipo == 'int':
            r = int(strdta)
        elif tipo == 'date1':
            r = datetime.strptime(strdta, '%Y-%m-%dT%H:%M:%S')
        elif tipo == 'date2':
            r = datetime.strptime(strdta, '%Y-%m-%d')
        elif tipo == 'hora':
            r = datetime.strptime(strdta, '%H:%M:%S.%f')
    return r


def dbinsert_cfdi(c, data):
    cur = c.cursor()
    if data['UUID'] == '' or data['UUID'] is None:
        return 'UUID Vacio'
    sql = 'Select UUID from CFDI where UUID="' + data['UUID'] + '"'
    if len(cur.execute(sql).fetchall()) == 0:
        dbdata = ('UUID', 'version', 'serie', 'folio', 'fecha', 'sello', 'formapago', 'nocertificado', 'certificado',
                  'condicionesdepago', 'subtotal', 'descuento', 'moneda', 'tipocambio', 'total', 'tipodecomprobante',
                  'metodopago', 'lugarexpedicion', 'confirmacion')
        detcfdi = dict.fromkeys(dbdata)
        detcfdi.update(data)
        detcfdi['version'] = convtipo(detcfdi['version'], 'float')
        detcfdi['fecha'] = convtipo(detcfdi['fecha'], 'date1')
        detcfdi['subtotal'] = convtipo(detcfdi['subtotal'], 'float')
        detcfdi['descuento'] = convtipo(detcfdi['descuento'], 'float')
        detcfdi['tipocambio'] = convtipo(detcfdi['tipocambio'], 'float')
        detcfdi['total'] = convtipo(detcfdi['total'], 'float')
        reg = (detcfdi['UUID'], detcfdi['version'], detcfdi['serie'], detcfdi['folio'], detcfdi['fecha'],
               detcfdi['sello'], detcfdi['formapago'], detcfdi['nocertificado'], detcfdi['certificado'],
               detcfdi['condicionesdepago'], detcfdi['subtotal'], detcfdi['descuento'], detcfdi['moneda'],
               detcfdi['tipocambio'], detcfdi['total'], detcfdi['tipodecomprobante'], detcfdi['metodopago'],
               detcfdi['lugarexpedicion'], detcfdi['confirmacion'], 'verde', 'x:\\cliente\\CFDIs')
        # valida - indicara si está valida en el SAT
        # path - link a donde está guardado el archivo
        cur.execute("INSERT INTO CFDI VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", reg)
        # c.commit()
        return 'OK'
    else:
        return 'DUPLICADO'


def dbinsert_cfdi_rels(c, data):
    # TipoRelacion(c_TipoRelacion)
    # UUID  --  Es posible que sean varios, data los trae como UUID#
    cur = c.cursor()
    i = 1
    while 'uuid' + str(i) in data:
        reg = (data['UUID'], data['tiporelacion'], data['uuid' + str(i)])
        cur.execute("INSERT INTO CFDIrelacionados VALUES(?,?,?)", reg)
        # c.commit()
        i += 1
    cur.close()


def dbinsertemisor(c, data):
    # RFC | *Nombre | RegimenFiscal(c_RegimenFiscal)
    cur = c.cursor()
    dbdata = ('UUID', 'rfc', 'nombre', 'regimenfiscal')
    detemi = dict.fromkeys(dbdata)
    detemi.update(data)
    reg = (detemi['UUID'], detemi['rfc'], detemi['nombre'], detemi['regimenfiscal'])
    cur.execute("INSERT INTO Emisor VALUES(?,?,?,?)", reg)
    # c.commit()
    cur.close()


def dbinsertreceptor(c, data):
    # RFC | *Nombre | *ResidenciaFiscal | *NumRegIdTrib | UsoCFDI(c_UsoCFDI)
    cur = c.cursor()
    dbdata = ('UUID', 'rfc', 'nombre', 'residenciafiscal', 'numregidtrib', 'usocfdi')
    detrec = dict.fromkeys(dbdata)
    detrec.update(data)
    reg = (detrec['UUID'], detrec['rfc'], detrec['nombre'], detrec['residenciafiscal'], detrec['numregidtrib'],
           detrec['usocfdi'])
    cur.execute("INSERT INTO Receptor VALUES(?,?,?,?,?,?)", reg)
    # c.commit()
    cur.close()


def insertimp(c, imp, idimp, tipo):
    detalle = ('id', 'tipo', 'base', 'impuesto', 'tipofactor', 'tasaocuota', 'importe')
    # for i in imp:
    impuesto = dict.fromkeys(detalle)
    impuesto.update(imp)  # [i])
    impuesto['id'] = idimp
    impuesto['tipo'] = tipo
    impuesto['base'] = convtipo(impuesto['base'], 'float')
    impuesto['tasaocuota'] = convtipo(impuesto['tasaocuota'], 'float')
    impuesto['importe'] = convtipo(impuesto['importe'], 'float')
    reg = (impuesto['id'], impuesto['tipo'], impuesto['base'], impuesto['impuesto'], impuesto['tipofactor'],
           impuesto['tasaocuota'], impuesto['importe'])
    c.execute("INSERT INTO T_Impuestos VALUES(?,?,?,?,?,?,?)", reg)


def dbinsertimpuestos(c, data):
    # *TotalImpuestosRetenidos | *TotalImpuestosTrasladados
    cur = c.cursor()
    dbdata = ('UUID', 'totalimpuestosretenidos', 'totalimpuestostrasladados', 'id_timpuestos')
    detimp = dict.fromkeys(dbdata)
    detimp['id_timpuestos'] = idcomp(c, 'T_Impuestos', 'id', 10)  # Identificador para insertar el detalle
    detimp['UUID'] = data['UUID']
    if 'totalimpuestosretenidos' in data:
        detimp['totalimpuestosretenidos'] = data['totalimpuestosretenidos']
    if 'totalimpuestostrasladados' in data:
        detimp['totalimpuestostrasladados'] = data['totalimpuestostrasladados']
    detimp['totalimpuestosretenidos'] = convtipo(detimp['totalimpuestosretenidos'], 'float')
    detimp['totalimpuestostrasladados'] = convtipo(detimp['totalimpuestostrasladados'], 'float')
    reg = (detimp['UUID'], detimp['totalimpuestosretenidos'], detimp['totalimpuestostrasladados'],
           detimp['id_timpuestos'])
    cur.execute("INSERT INTO Impuestos VALUES(?,?,?,?)", reg)
    # c.commit()
    if 'retenciones' in data:
        # insertimp(c, data['retenciones'], detimp['id_timpuestos'], 'RETENCION')
        if data['retenciones'] is not None:
            for r in data['retenciones']:
                insertimp(c, data['retenciones'][r], detimp['id_timpuestos'], 'RETENCION')
    if 'traslados' in data:
        # insertimp(c, data['traslados'], detimp['id_timpuestos'], 'TRASLADO')
        if data['traslados'] is not None:
            for r in data['traslados']:
                insertimp(c, data['traslados'][r], detimp['id_timpuestos'], 'TRASLADO')


def inseradd(c, add, idadd, tipo):
    if tipo == 'aduana':
        detalle = ('cve_aduana', 'numero', 'fecha', 'aduana')
        cve = 'cve_aduana'
    elif tipo == 'predial':
        detalle = ('cve_predial', 'numero')
        cve = 'cve_predial'
    detalleadd = dict.fromkeys(detalle)
    detalleadd[cve] = idadd
    detalleadd.update(add)
    if tipo == 'aduana':
        detalleadd['fecha'] = convtipo(detalleadd['fecha'], 'date2')
        reg = (detalleadd['cve_aduana'], detalleadd['numero'], detalleadd['fecha'], detalleadd['aduana'])
        c.execute("INSERT INTO T_Aduana VALUES(?,?,?,?)", reg)
    elif tipo == 'predial':
        reg = (detalleadd['cve_predial'], detalleadd['numero'])
        c.execute("INSERT INTO T_Predial VALUES(?,?)", reg)


def insertpart(c, parte, idparte, numparte):
    detparte = ('id', 'numparte', 'claveprodserv', 'cantidad', 'unidad', 'noidentificacion', 'descripcion',
                'valorunitario', 'importe', 'descuento', 'id_taduana')
    detalleparte = dict.fromkeys(detparte)
    detalleparte['id'] = idparte
    i = numparte
    detalleparte['numparte'] = '{0:03d}'.format(i)
    for det in parte:
        if str(type(parte[det])) == "<class 'str'>":
            detalleparte[det] = parte[det]
        else:
            if not det.find('informacionaduanera') == -1:
                detalleparte['id_taduana'] = idcomp(c, 'T_Aduana', 'cve_Aduana', 7)
                inseradd(c, parte[det], detalleparte['id_taduana'], 'aduana')
            # c.commit()
    detalleparte['cantidad'] = convtipo(detalleparte['cantidad'], 'float')
    detalleparte['valorunitario'] = convtipo(detalleparte['valorunitario'], 'float')
    detalleparte['importe'] = convtipo(detalleparte['importe'], 'float')
    detalleparte['descuento'] = convtipo(detalleparte['descuento'], 'float')
    reg = (detalleparte['id'], detalleparte['numparte'], detalleparte['claveprodserv'], detalleparte['cantidad'],
           detalleparte['unidad'], detalleparte['noidentificacion'], detalleparte['descripcion'],
           detalleparte['valorunitario'], detalleparte['importe'], detalleparte['descuento'],
           detalleparte['id_taduana'])
    c.execute("INSERT INTO C_Parte VALUES(?,?,?,?,?,?,?,?,?,?,?)", reg)


def insertadomce(c, data_, iddml):
    # calle | numeroexterior | numerointerior | colonia | localidad | referencia | municipio | estado | pais |
    # codigopostal
    domdata = ('id', 'calle', 'numeroexterior', 'numerointerior', 'colonia', 'localidad', 'referencia', 'municipio',
               'estado', 'pais', 'codigopostal')
    domicilio = dict.fromkeys(domdata)
    domicilio.update(data_)
    domicilio['id'] = iddml
    reg = (domicilio['id'], domicilio['calle'], domicilio['numeroexterior'], domicilio['numerointerior'],
           domicilio['colonia'], domicilio['localidad'], domicilio['referencia'], domicilio['municipio'],
           domicilio['estado'], domicilio['pais'], domicilio['codigopostal'])
    c.execute("INSERT INTO Domicilio_CE VALUES(?,?,?,?,?,?,?,?,?,?,?)", reg)


def dbinsertconceptos(c, data):
    # ClaveProdServ(c_ClaveProdServ) | *NoIdentificacion | Cantidad | ClaveUnidad(c_ClaveUnidad)
    # *Unidad | Descripcion | ValorUnitario | Importe | *Descuento |
    cur = c.cursor()
    concepto = ('UUID', 'numcpto', 'claveprodserv', 'noidentificacion', 'cantidad', 'claveunidad', 'unidad',
                'descripcion', 'valorunitario', 'importe', 'descuento', 'id_aduana', 'id_predial', 'id_impuestos',
                'cvePartes')
    for con in data:
        if not con.find('concepto') == -1:
            detalleconcepto = dict.fromkeys(concepto)
            conc = data[con]
            detalleconcepto['UUID'] = data['UUID']
            detalleconcepto['numcpto'] = idcomp(c, 'Conceptos', 'numcpto', 15)
            for dt in conc:
                if str(type(conc[dt])) == "<class 'str'>":
                    detalleconcepto[dt] = conc[dt]
                else:
                    # impuestos  |  informacionaduanera  |  cuentapredial  |  complementoconcepto  |  parte#
                    addon = dt
                    addondict = conc[dt]
                    if addon == 'impuestos':
                        detalleconcepto['id_impuestos'] = idcomp(c, 'T_Impuestos', 'id', 10)
                        for imp in addondict:
                            if not str(imp).find("traslado") == -1:
                                tp = 'TRASLADO'
                            else:
                                tp = 'RETENCION'
                            insertimp(c, addondict[imp], detalleconcepto['id_impuestos'], tp)
                            # c.commit()
                    else:
                        if addon == 'informacionaduanera':
                            detalleconcepto['id_aduana'] = idcomp(c, 'T_Aduana', 'cve_Aduana', 7)
                            inseradd(c, addondict, detalleconcepto['id_aduana'], 'aduana')
                            # c.commit()
                        else:
                            if addon == 'cuentapredial':
                                detalleconcepto['id_predial'] = idcomp(c, 'T_Predial', 'cve_Predial', 5)
                                inseradd(c, addondict, detalleconcepto['id_predial'], 'predial')
                                # c.commit()
                            else:
                                if addon == 'complementoconcepto':
                                    # opciones de schema:
                                    # instEducativas | VentaVehiculos | PorCuentadeTerceros | acreditamientoIEPS
                                    if addondict['schema'] == 'insteducativas':
                                        # version | nombrealumno | curp | niveleducativo | autrvoe | rfcpago
                                        detcomp = ('id', 'version', 'nombrealumno', 'curp', 'niveleducativo', 'autrvoe',
                                                   'rfcpago')
                                        detallecomp = dict.fromkeys(detcomp)
                                        detallecomp['id'] = detalleconcepto['numcpto']
                                        detallecomp.update(addondict)
                                        detallecomp.pop('schema', None)
                                        reg = (detallecomp['id'], detallecomp['version'], detallecomp['nombrealumno'],
                                               detallecomp['curp'], detallecomp['niveleducativo'],
                                               detallecomp['autrvoe'], detallecomp['rfcpago'])
                                        cur.execute("INSERT INTO compC_instEducativas VALUES(?,?,?,?,?,?,?)", reg)
                                        # c.commit()
                                    elif addondict['schema'] == 'acreditamientoieps':
                                        # version | TAR
                                        detcomp = ('id', 'version', 'tar')
                                        detallecomp = dict.fromkeys(detcomp)
                                        detallecomp['id'] = detalleconcepto['numcpto']
                                        detallecomp.update(addondict)
                                        detallecomp.pop('schema', None)
                                        reg = (detallecomp['id'], detallecomp['version'], detallecomp['tar'])
                                        cur.execute("INSERT INTO compC_acreditamientoIEPS VALUES(?,?,?)", reg)
                                        # c.commit()
                                    elif addondict['schema'] == 'ventavehiculos':
                                        # version | clavevehicular | NIV
                                        detcomp = ('id', 'version', 'clavevehicular', 'niv', 'cve_aduana', 'cve_partes')
                                        detallecomp = dict.fromkeys(detcomp)
                                        detallecomp['id'] = detalleconcepto['numcpto']
                                        for dt1 in addondict:
                                            if str(type(addondict[dt1])) == "<class 'str'>":
                                                detallecomp[dt1] = addondict[dt1]
                                            elif not dt1.find('aduanera') == -1:
                                                    detallecomp['id_aduana'] = idcomp(c, 'T_Aduana', 'cve_Aduana', 7)
                                                    inseradd(c, addondict[dt1], detallecomp['cve_aduana'], 'aduana')
                                                    # c.commit()
                                            elif not dt1.find('parte') == -1:
                                                detallecomp['cve_partes'] = idcomp(c, 'C_Parte', 'id', 5)
                                                i = convtipo(dt1.replace('parte', ''), 'int')
                                                insertpart(c, addondict[dt1], detallecomp['cve_partes'], i)
                                                # c.commit()
                                        detallecomp.pop('schema', None)
                                        reg = (detallecomp['id'], detallecomp['version'], detallecomp['clavevehicular'],
                                               detallecomp['niv'], detallecomp['cve_aduana'], detallecomp['cve_partes'])
                                        cur.execute("INSERT INTO compC_VentaVehiculos VALUES(?,?,?,?,?,?)", reg)
                                        # c.commit()
                                    elif addondict['schema'] == 'porcuentadeterceros':
                                        dettercero = ('id', 'cve_partes', 'cve_impuestos', 'version', 'id_ift',
                                                      'nombre', 'cve_aduana', 'cve_predial')
                                        detalletercero = dict.fromkeys(dettercero)
                                        detalletercero['id'] = detalleconcepto['numcpto']
                                        detalletercero['version'] = addondict['version']
                                        if 'nombre' in addondict:
                                            detalletercero['nombre'] = addondict['nombre']
                                        # addondict['rfc']
                                        pte = 0
                                        for dt_ in addondict:
                                            if not str(dt_).find('parte') == -1:
                                                pte += 1
                                                if detalletercero['cve_partes'] is None:
                                                    detalletercero['cve_partes'] = idcomp(c, 'C_Parte', 'cve_Predial',
                                                                                          5)
                                                insertpart(c, addondict[dt_], detalletercero['cve_partes'], pte)
                                                # c.commit()
                                            elif not str(dt_).find('impuestos') == -1:
                                                if detalletercero['cve_impuestos'] is None:
                                                    detalletercero['cve_impuestos'] = idcomp(c, 'T_Impuestos', 'id', 10)
                                                # detimp = ('id', 'tipo', 'base', 'impuesto', 'tipofactor', 'tasaocuota'
                                                #           , 'importe')
                                                for lvl in addondict[dt_]:
                                                    for imp in addondict[dt_][lvl]:
                                                        '''detalleimp = dict.fromkeys(detimp)
                                                        detalleimp['id'] = detalletercero['cve_impuestos']'''
                                                        if not str(imp).find("traslado") == -1:
                                                            # detalleimp['tipo'] = 'TRASLADO'
                                                            tipo = 'TRASLADO'
                                                        else:
                                                            # detalleimp['tipo'] = 'RETENCION'
                                                            tipo = 'RETENCION'
                                                        insertimp(c, addondict[dt_][lvl][imp],
                                                                  detalletercero['cve_impuestos'], tipo)
                                                        # c.commit()
                                            elif not str(dt_).find('fiscal') == -1:
                                                detalletercero['id_ift'] = idcomp(c, 'cC_PCdT_InformacionFiscalTercero',
                                                                                  'id', 5)
                                                det_ift = ('id', 'rfc', 'calle', 'noexterior', 'nointerior', 'colonia',
                                                           'localidad', 'referencia', 'municipio', 'estado', 'pais',
                                                           'codigopostal')
                                                detalle_ift = dict.fromkeys(det_ift)
                                                detalle_ift['id'] = detalletercero['id_ift']
                                                detalle_ift.update(addondict[dt_])
                                                detalle_ift['rfc'] = addondict['rfc']
                                                reg = (detalle_ift['id'], detalle_ift['rfc'], detalle_ift['calle'],
                                                       detalle_ift['noexterior'], detalle_ift['nointerior'],
                                                       detalle_ift['colonia'], detalle_ift['localidad'],
                                                       detalle_ift['referencia'], detalle_ift['municipio'],
                                                       detalle_ift['estado'], detalle_ift['pais'],
                                                       detalle_ift['codigopostal'])
                                                cur.execute("INSERT INTO cC_PCdT_InformacionFiscalTercero " +
                                                            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", reg)
                                                # c.commit()
                                            elif not str(dt_).find('aduanera') == -1:
                                                detalletercero['cve_aduana'] = idcomp(c, 'T_Aduana', 'cve_Aduana', 7)
                                                inseradd(c, addondict[dt_], detalletercero['cve_aduana'], 'aduana')
                                                # c.commit()
                                            elif not str(dt_).find('predial') == -1:
                                                detalletercero['cve_predial'] = idcomp(c, 'T_Predial', 'cve_Predial', 5)
                                                inseradd(c, addondict[dt_], detalletercero['cve_predial'], 'predial')
                                                # c.commit()
                                        reg = (detalletercero['id'], detalletercero['cve_partes'],
                                               detalletercero['cve_impuestos'], detalletercero['version'],
                                               detalletercero['id_ift'], detalletercero['nombre'],
                                               detalletercero['cve_aduana'], detalletercero['cve_predial'])
                                        cur.execute("INSERT INTO compC_PorCuentadeTerceros VALUES(?,?,?,?,?,?,?,?)",
                                                    reg)
                                        # c.commit()
                                else:
                                    # el addon es 'parte#'
                                    detalleconcepto['cvePartes'] = idcomp(c, 'C_Parte', 'id', 5)
                                    i = convtipo(addon.replace('parte', ''), 'int')
                                    insertpart(c, addondict, detalleconcepto['cvePartes'], i)
                                    # c.commit()
            detalleconcepto['cantidad'] = convtipo(detalleconcepto['cantidad'], 'float')
            detalleconcepto['valorunitario'] = convtipo(detalleconcepto['valorunitario'], 'float')
            detalleconcepto['importe'] = convtipo(detalleconcepto['importe'], 'float')
            detalleconcepto['descuento'] = convtipo(detalleconcepto['descuento'], 'float')
            reg = (detalleconcepto['UUID'], detalleconcepto['numcpto'], detalleconcepto['claveprodserv'],
                   detalleconcepto['noidentificacion'], detalleconcepto['cantidad'], detalleconcepto['claveunidad'],
                   detalleconcepto['unidad'], detalleconcepto['descripcion'], detalleconcepto['valorunitario'],
                   detalleconcepto['importe'], detalleconcepto['descuento'], detalleconcepto['id_aduana'],
                   detalleconcepto['id_predial'], detalleconcepto['id_impuestos'], detalleconcepto['cvePartes'])
            cur.execute("INSERT INTO Conceptos VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", reg)
            # c.commit()
    cur.close()


def dbinsertcomplementos(c, data):
    uuid = data['UUID']
    cur = c.cursor()
    for con in data:
        if con == 'timbrefiscaldigital':
            # Version | UUID | FechaTimbrado | RfcProvCertif | *Leyenda | SelloCFD |
            # NoCertificadoSAT | SelloSAT
            tfd = ('uuid', 'version', 'fechatimbrado', 'rfcprovcertif', 'leyenda', 'sellocfd',
                   'nocertificadosat', 'sellosat')
            detalletfd = dict.fromkeys(tfd)  # Create data dictionary from definition
            detalletfd.update(data[con])
            detalletfd['fechatimbrado'] = convtipo(detalletfd['fechatimbrado'], 'date1')
            reg = (detalletfd['uuid'], detalletfd['version'], detalletfd['fechatimbrado'],
                   detalletfd['rfcprovcertif'], detalletfd['leyenda'], detalletfd['sellocfd'],
                   detalletfd['nocertificadosat'], detalletfd['sellosat'])
            cur.execute("INSERT INTO TimbreFiscalDigital VALUES(?,?,?,?,?,?,?,?)", reg)
            # c.commit()
        elif con == 'aerolineas':
            # Version | TUA | TotalOtrosCargos
            compl = ('UUID', 'id', 'version', 'tua', 'totalotroscargos')
            detallecompl = dict.fromkeys(compl)
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'Aerolineas')
            for dt in data[con]:
                if dt == 'tua' or dt == 'version':
                    detallecompl[dt] = data[con][dt]
                if not dt.find('otroscargos') == -1:
                    otcgs = data[con][dt]
                    for det in otcgs:
                        if det == 'totalcargos':
                            detallecompl['totalotroscargos'] = otcgs[det]
                        elif not det.find('cargo') == -1:
                            reg = (otcgs[det]['codigocargo'], float(otcgs[det]['importe']), detallecompl['id'])
                            cur.execute("INSERT INTO Aerolineas_OtrosCargos VALUES(?,?,?)", reg)
                            # c.commit()
            detallecompl['version'] = convtipo(detallecompl['version'], 'float')
            detallecompl['tua'] = convtipo(detallecompl['tua'], 'float')
            detallecompl['totalotroscargos'] = convtipo(detallecompl['totalotroscargos'], 'float')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['tua'],
                   detallecompl['totalotroscargos'])
            cur.execute("INSERT INTO Aerolineas VALUES(?,?,?,?,?)", reg)
            # c.commit()
        elif con == 'donatarias':
            # version | noautorizacion | fechaautorizacion | leyenda
            compl = ('UUID', 'id', 'version', 'noautorizacion', 'fechaautorizacion', 'leyenda')
            detallecompl = dict.fromkeys(compl)
            detallecompl.update(data[con])
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'Donatarias')
            detallecompl['version'] = convtipo(detallecompl['version'], 'float')
            detallecompl['fechaautorizacion'] = convtipo(detallecompl['fechaautorizacion'], 'date2')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['noautorizacion'],
                   detallecompl['fechaautorizacion'], detallecompl['leyenda'])
            cur.execute("INSERT INTO Donatarias VALUES(?,?,?,?,?,?)", reg)
            # c.commit()
        elif con == 'divisas':
            # version | tipooperacion
            compl = ('UUID', 'id', 'version', 'tipooperacion')
            detallecompl = dict.fromkeys(compl)
            detallecompl.update(data[con])
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'Divisas')
            detallecompl['version'] = convtipo(detallecompl['version'], 'float')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['tipooperacion'])
            cur.execute("INSERT INTO Divisas VALUES(?,?,?,?)", reg)
            # c.commit()
        elif con == 'pfintegrantecoordinado':
            # version | clavevehicular | placa | rfcpf
            compl = ('UUID', 'id', 'version', 'clavevehicular', 'placa', 'rfcpf')
            detallecompl = dict.fromkeys(compl)
            detallecompl.update(data[con])
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'PFintegranteCoordinado')
            detallecompl['version'] = convtipo(detallecompl['version'], 'float')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['clavevehicular'],
                   detallecompl['placa'], detallecompl['rfcpf'])
            cur.execute("INSERT INTO PFintegranteCoordinado VALUES(?,?,?,?,?,?)", reg)
            # c.commit()
        elif con == 'cfdiregistrofiscal':
            # version | folio
            compl = ('UUID', 'id', 'version', 'folio')
            detallecompl = dict.fromkeys(compl)
            detallecompl.update(data[con])
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'CFDIRegistroFiscal')
            detallecompl['version'] = convtipo(detallecompl['version'], 'float')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['folio'])
            cur.execute("INSERT INTO CFDIRegistroFiscal VALUES(?,?,?,?)", reg)
            # c.commit()
        elif con == 'pagoenespecie':
            # version | cvepic | foliosoldon | pzaartnombre | pzaarttecn | pzaartaprod | pzaartdim
            compl = ('UUID', 'id', 'version', 'cvepic', 'foliosoldon', 'pzaartnombre', 'pzaarttecn', 'pzaartaprod',
                     'pzaartdim')
            detallecompl = dict.fromkeys(compl)
            detallecompl.update(data[con])
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'PagoEnEspecie')
            detallecompl['version'] = convtipo(detallecompl['version'], 'float')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['cvepic'],
                   detallecompl['foliosoldon'], detallecompl['pzaartnombre'], detallecompl['pzaarttecn'],
                   detallecompl['pzaartaprod'], detallecompl['pzaartdim'])
            cur.execute("INSERT INTO PagoEnEspecie VALUES(?,?,?,?,?,?,?,?,?)", reg)
            # c.commit()
        elif con == 'obrasarteantiguedades':
            # version | tipobien | otrostipobien | tituloadquirido | otrostituloadquirido | subtotal | iva |
            # fechaadquisicion | característicasdeobraopieza
            compl = ('UUID', 'id', 'version', 'tipobien', 'otrostipobien', 'tituloadquirido', 'otrostituloadquirido',
                     'subtotal', 'iva', 'fechaadquisicion', 'característicasdeobraopieza')
            detallecompl = dict.fromkeys(compl)
            detallecompl.update(data[con])
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'obrasarteantiguedades')
            detallecompl['version'] = convtipo(detallecompl['version'], 'float')
            detallecompl['subtotal'] = convtipo(detallecompl['subtotal'], 'float')
            detallecompl['iva'] = convtipo(detallecompl['iva'], 'float')
            detallecompl['fechaadquisicion'] = convtipo(detallecompl['fechaadquisicion'], 'date2')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['tipobien'],
                   detallecompl['otrostipobien'], detallecompl['tituloadquirido'], detallecompl['otrostituloadquirido'],
                   detallecompl['subtotal'], detallecompl['iva'], detallecompl['fechaadquisicion'],
                   detallecompl['característicasdeobraopieza'])
            cur.execute("INSERT INTO obrasarteantiguedades VALUES(?,?,?,?,?,?,?,?,?,?,?)", reg)
            # c.commit()
        elif con == 'leyendasfiscales':
            # version
            compl = ('UUID', 'id', 'version', 'disposicionfiscal', 'norma', 'textoleyenda')
            if 'version' in data[con]:
                vers = data[con]['version']
            else:
                vers = '0.0'
            for dt in data[con]:
                if not dt.find('leyenda') == -1:
                    #  disposicionfiscal | norma | textoleyenda
                    ley = data[con][dt]
                    detallecompl = dict.fromkeys(compl)
                    detallecompl.update(ley)
                    detallecompl['UUID'] = uuid
                    detallecompl['id'] = idcomp(c, 'LeyendasFiscales')
                    detallecompl['version'] = vers
                    detallecompl['version'] = convtipo(detallecompl['version'], 'float')
                    reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'],
                           detallecompl['disposicionfiscal'], detallecompl['norma'], detallecompl['textoleyenda'])
                    cur.execute("INSERT INTO LeyendasFiscales VALUES(?,?,?,?,?,?)", reg)
                    # c.commit()
        elif con == 'turistapasajeroextranjero':
            # version | fechadetransito | tipotransito
            compl = ('UUID', 'id', 'version', 'fechadetransito', 'tipotransito')
            detallecompl = dict.fromkeys(compl)
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'TuristaPasajeroExtranjero')
            for dt in data[con]:
                if dt in detallecompl:
                    detallecompl[dt] = data[con][dt]
                elif not dt.find('datostransito') == -1:
                    dtstr = data[con][dt]
                    # via | tipoid | numeroid | nacionalidad | empresatransporte | idtransporte
                    subcompl = ('id', 'via', 'tipoid', 'numeroid', 'nacionalidad', 'empresatransporte', 'idtransporte')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl.update(dtstr)
                    detallesubcompl['id'] = detallecompl['id']
                    reg = (detallesubcompl['id'], detallesubcompl['via'], detallesubcompl['tipoid'],
                           detallesubcompl['numeroid'], detallesubcompl['nacionalidad'],
                           detallesubcompl['empresatransporte'], detallesubcompl['idtransporte'])
                    cur.execute("INSERT INTO datosTransito VALUES(?,?,?,?,?,?,?)", reg)
                    # c.commit()
            detallecompl['version'] = convtipo(detallecompl['version'], 'float')
            detallecompl['fechadetransito'] = convtipo(detallecompl['fechadetransito'], 'date1')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['fechadetransito'],
                   detallecompl['tipotransito'])
            cur.execute("INSERT INTO TuristaPasajeroExtranjero VALUES(?,?,?,?,?)", reg)
            # c.commit()
        elif con == 'valesdedespensa':
            # version | tipooperacion | registropatronal | numerodecuenta | total
            compl = ('UUID', 'id', 'version', 'tipooperacion', 'registropatronal', 'numerodecuenta', 'total')
            detallecompl = dict.fromkeys(compl)
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'ValesDeDespensa')
            for dt in data[con]:
                if dt in detallecompl:
                    detallecompl[dt] = data[con][dt]
                elif not dt.find('conceptos') == -1:
                    for dets in data[con][dt]:
                        dtstr = data[con][dt][dets]
                        # via | tipoid | numeroid | nacionalidad | empresatransporte | idtransporte
                        subcompl = ('id', 'identificador', 'fecha', 'rfc', 'curp', 'nombre', 'numseguridadsocial',
                                    'importe')
                        detallesubcompl = dict.fromkeys(subcompl)
                        detallesubcompl.update(dtstr)
                        detallesubcompl['id'] = detallecompl['id']
                        detallesubcompl['fecha'] = convtipo(detallesubcompl['fecha'], 'date1')
                        detallesubcompl['importe'] = convtipo(detallesubcompl['importe'], 'float')
                        reg = (detallesubcompl['id'], detallesubcompl['identificador'], detallesubcompl['fecha'],
                               detallesubcompl['rfc'], detallesubcompl['curp'], detallesubcompl['nombre'],
                               detallesubcompl['numseguridadsocial'], detallesubcompl['importe'])
                        cur.execute("INSERT INTO Conceptos_ValesDeDespensa VALUES(?,?,?,?,?,?,?,?)", reg)
                        # c.commit()
            detallecompl['version'] = convtipo(detallecompl['version'], 'float')
            detallecompl['total'] = convtipo(detallecompl['total'], 'float')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['tipooperacion'],
                   detallecompl['registropatronal'], detallecompl['numerodecuenta'], detallecompl['total'])
            cur.execute("INSERT INTO ValesDeDespensa VALUES(?,?,?,?,?,?,?)", reg)
            # c.commit()
        elif con == 'vehiculousado':
            # version | montoadquisicion | montoenajenacion | clavevehicular | marca | tipo | modelo | numeromotor |
            # numeroserie | niv | valor
            compl = ('UUID', 'id', 'version', 'montoadquisicion', 'montoenajenacion', 'clavevehicular', 'marca', 'tipo',
                     'modelo', 'numeromotor', 'numeroserie', 'niv', 'valor', 'id_aduana')
            detallecompl = dict.fromkeys(compl)
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'VehiculoUsado')
            for dt in data[con]:
                if dt in detallecompl:
                    detallecompl[dt] = data[con][dt]
                elif not dt.find('informacionaduanera') == -1:
                    dtstr = data[con][dt]
                    detallecompl['id_aduana'] = idcomp(c, 'T_Aduana', 'cve_Aduana', 7)
                    inseradd(c, dtstr, detallecompl['id_aduana'], 'aduana')
                    # numero | fecha | aduana
                    # c.commit()
            detallecompl['version'] = convtipo(detallecompl['version'], 'float')
            detallecompl['montoadquisicion'] = convtipo(detallecompl['montoadquisicion'], 'float')
            detallecompl['montoenajenacion'] = convtipo(detallecompl['montoenajenacion'], 'float')
            detallecompl['valor'] = convtipo(detallecompl['valor'], 'float')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['montoadquisicion'],
                   detallecompl['montoenajenacion'], detallecompl['clavevehicular'], detallecompl['marca'],
                   detallecompl['tipo'], detallecompl['modelo'], detallecompl['numeromotor'],
                   detallecompl['numeroserie'], detallecompl['niv'], detallecompl['valor'], detallecompl['id_aduana'])
            cur.execute("INSERT INTO VehiculoUsado VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", reg)
        elif con == 'parcialesconstruccion':
            # version | numperlicoaut
            compl = ('UUID', 'id', 'version', 'numperlicoaut')
            detallecompl = dict.fromkeys(compl)
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'parcialesconstruccion')
            for dt in data[con]:
                if dt in detallecompl:
                    detallecompl[dt] = data[con][dt]
                elif not dt.find('inmueble') == -1:
                    dtstr = data[con][dt]
                    # calle | noexterior | nointerior | colonia | localidad | referencia | municipio | estado |
                    # codigopostal
                    subcompl = ('id', 'calle', 'noexterior', 'nointerior', 'colonia', 'localidad', 'referencia',
                                'municipio', 'estado', 'codigopostal')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl.update(dtstr)
                    detallesubcompl['id'] = detallecompl['id']
                    reg = (detallesubcompl['id'], detallesubcompl['calle'], detallesubcompl['noexterior'],
                           detallesubcompl['nointerior'], detallesubcompl['colonia'], detallesubcompl['localidad'],
                           detallesubcompl['referencia'], detallesubcompl['municipio'], detallesubcompl['estado'],
                           detallesubcompl['codigopostal'])
                    cur.execute("INSERT INTO Inmueble_parcialesconstruccion VALUES(?,?,?,?,?,?,?,?,?,?)", reg)
                    # c.commit()
            detallecompl['version'] = convtipo(detallecompl['version'], 'float')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['numperlicoaut'])
            cur.execute("INSERT INTO parcialesconstruccion VALUES(?,?,?,?)", reg)
            # c.commit()
        elif con == 'ine':
            # version | tipoproceso | tipocomite | idcontabilidad
            compl = ('UUID', 'id', 'version', 'tipoproceso', 'tipocomite', 'idcontabilidad')
            detallecompl = dict.fromkeys(compl)
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'INE')
            for dt in data[con]:
                if dt in detallecompl:
                    detallecompl[dt] = data[con][dt]
                elif not dt.find('entidad') == -1:
                    dtstr = data[con][dt]
                    # claveentidad | ambito | idcontabilidad
                    subcompl = ('id', 'claveentidad', 'ambito', 'idcontabilidad')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl['id'] = detallecompl['id']
                    for det in dtstr:
                        if det in detallesubcompl:
                            detallesubcompl[det] = dtstr[det]
                        elif not det.find('contabilidad') == -1:
                            detallesubcompl['idcontabilidad'] = dtstr[det]['idcontabilidad']
                    reg = (detallesubcompl['id'], detallesubcompl['claveentidad'], detallesubcompl['ambito'],
                           detallesubcompl['idcontabilidad'])
                    cur.execute("INSERT INTO INE_Entidad VALUES(?,?,?,?)", reg)
                    # c.commit()
            detallecompl['version'] = convtipo(detallecompl['version'], 'float')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['tipoproceso'],
                   detallecompl['tipocomite'], detallecompl['idcontabilidad'])
            cur.execute("INSERT INTO INE VALUES(?,?,?,?,?,?)", reg)
            # c.commit()
        elif con == 'estadodecuentacombustible':
            # version | tipooperacion | numerodecuenta | subtotal | total
            compl = ('UUID', 'id', 'version', 'tipooperacion', 'numerodecuenta', 'subtotal', 'total')
            detallecompl = dict.fromkeys(compl)
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'estadodecuentacombustible')
            for dt in data[con]:
                if dt in detallecompl:
                    detallecompl[dt] = data[con][dt]
                elif not dt.find('conceptos') == -1:
                    dtstr = data[con][dt]
                    for dt_ in dtstr:
                        cptos = dtstr[dt_]
                        cpto = ('id', 'id_impuestos', 'identificador', 'fecha', 'rfc', 'claveestacion', 'cantidad',
                                'tipocombustible', 'unidad', 'nombrecombustible', 'foliooperacion', 'valorunitario',
                                'importe')
                        detallesubcompl = dict.fromkeys(cpto)
                        detallesubcompl['id'] = detallecompl['id']
                        detallesubcompl['id_impuestos'] = idcomp(c, 'T_Impuestos', 'id', 10)
                        for detail in cptos:
                            # identificador | fecha | rfc | claveestacion | cantidad | tipocombustible | unidad |
                            # nombrecombustible | foliooperacion | valorunitario | importe
                            if detail in detallesubcompl:
                                detallesubcompl[detail] = cptos[detail]
                            else:
                                trld = cptos[detail]
                                for dis in trld:
                                    insertimp(c, trld[dis], detallesubcompl['id_impuestos'], 'TRASLADO')
                                # c.commit()
                        detallesubcompl['cantidad'] = convtipo(detallesubcompl['cantidad'], 'float')
                        detallesubcompl['valorunitario'] = convtipo(detallesubcompl['valorunitario'], 'float')
                        detallesubcompl['importe'] = convtipo(detallesubcompl['importe'], 'float')
                        detallesubcompl['fecha'] = convtipo(detallesubcompl['fecha'], 'date1')
                        reg = (detallesubcompl['id'], detallesubcompl['id_impuestos'], detallesubcompl['identificador'],
                               detallesubcompl['fecha'], detallesubcompl['rfc'], detallesubcompl['claveestacion'],
                               detallesubcompl['cantidad'], detallesubcompl['tipocombustible'],
                               detallesubcompl['unidad'], detallesubcompl['nombrecombustible'],
                               detallesubcompl['foliooperacion'], detallesubcompl['valorunitario'],
                               detallesubcompl['importe'])
                        cur.execute("INSERT INTO Concepto_EstadoDeCuentaCombustible VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    reg)
                        # c.commit()
            detallecompl['version'] = convtipo(detallecompl['version'], 'float')
            detallecompl['subtotal'] = convtipo(detallecompl['subtotal'], 'float')
            detallecompl['total'] = convtipo(detallecompl['total'], 'float')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['tipooperacion'],
                   detallecompl['numerodecuenta'], detallecompl['subtotal'], detallecompl['total'])
            cur.execute("INSERT INTO EstadoDeCuentaCombustible VALUES(?,?,?,?,?,?,?)", reg)
            # c.commit()
        elif con == 'consumodecombustibles':
            # version | tipooperacion | numerodecuenta | subtotal | total
            compl = ('UUID', 'id', 'version', 'tipooperacion', 'numerodecuenta', 'subtotal', 'total')
            detallecompl = dict.fromkeys(compl)
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'consumodecombustibles')
            for dt in data[con]:
                if dt in detallecompl:
                    detallecompl[dt] = data[con][dt]
                elif not dt.find('conceptos') == -1:
                    dtstr = data[con][dt]
                    for dt_ in dtstr:
                        cptos = dtstr[dt_]
                        cpto = ('id', 'id_impuestos', 'identificador', 'fecha', 'rfc', 'claveestacion',
                                'tipocombustible', 'cantidad', 'nombrecombustible', 'foliooperacion', 'valorunitario',
                                'importe')
                        detallesubcompl = dict.fromkeys(cpto)
                        detallesubcompl['id'] = detallecompl['id']
                        detallesubcompl['id_impuestos'] = idcomp(c, 'T_Impuestos', 'id', 10)
                        for detail in cptos:
                            # identificador | fecha | rfc | claveestacion | tipocombustible | cantidad |
                            # nombrecombustible | foliooperacion | valorunitario | importe
                            if detail in detallesubcompl:
                                detallesubcompl[detail] = cptos[detail]
                            else:
                                trld = cptos[detail]
                                for dis in trld:
                                    insertimp(c, trld[dis], detallesubcompl['id_impuestos'], 'DETERMINADO')
                                # c.commit()
                        detallesubcompl['cantidad'] = convtipo(detallesubcompl['cantidad'], 'float')
                        detallesubcompl['valorunitario'] = convtipo(detallesubcompl['valorunitario'], 'float')
                        detallesubcompl['importe'] = convtipo(detallesubcompl['importe'], 'float')
                        detallesubcompl['fecha'] = convtipo(detallesubcompl['fecha'], 'date1')
                        reg = (detallesubcompl['id'], detallesubcompl['id_impuestos'], detallesubcompl['identificador'],
                               detallesubcompl['fecha'], detallesubcompl['rfc'], detallesubcompl['claveestacion'],
                               detallesubcompl['tipocombustible'], detallesubcompl['cantidad'],
                               detallesubcompl['nombrecombustible'], detallesubcompl['foliooperacion'],
                               detallesubcompl['valorunitario'], detallesubcompl['importe'])
                        cur.execute("INSERT INTO Concepto_ConsumoDeCombustibles VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", reg)
                        # c.commit()
            detallecompl['version'] = convtipo(detallecompl['version'], 'float')
            detallecompl['subtotal'] = convtipo(detallecompl['subtotal'], 'float')
            detallecompl['total'] = convtipo(detallecompl['total'], 'float')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['tipooperacion'],
                   detallecompl['numerodecuenta'], detallecompl['subtotal'], detallecompl['total'])
            cur.execute("INSERT INTO ConsumoDeCombustibles VALUES(?,?,?,?,?,?,?)", reg)
            # c.commit()
        elif con == 'certificadodedestruccion':
            # version | serie | numfoldesveh
            compl = ('UUID', 'id', 'version', 'serie', 'numfoldesveh', 'cve_aduana')
            detallecompl = dict.fromkeys(compl)
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'certificadodedestruccion')
            for dt in data[con]:
                if dt in detallecompl:
                    detallecompl[dt] = data[con][dt]
                elif not dt.find('vehiculodestruido') == -1:
                    # marca | tipooclase | año | modelo | niv | numserie | numplacas | nummotor | numfoltarjcir
                    dtstr = data[con][dt]
                    subcompl = ('id', 'marca', 'tipooclase', 'año', 'modelo', 'niv', 'numserie', 'numplacas',
                                'nummotor', 'numfoltarjcir')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl.update(dtstr)
                    detallesubcompl['id'] = detallecompl['id']
                    detallesubcompl['año'] = convtipo(detallesubcompl['año'], 'int')
                    reg = (detallesubcompl['id'], detallesubcompl['marca'], detallesubcompl['tipooclase'],
                           detallesubcompl['año'], detallesubcompl['modelo'], detallesubcompl['niv'],
                           detallesubcompl['numserie'], detallesubcompl['numplacas'], detallesubcompl['nummotor'],
                           detallesubcompl['numfoltarjcir'])
                    cur.execute("INSERT INTO VehiculoDestruido VALUES(?,?,?,?,?,?,?,?,?,?)", reg)
                    # c.commit()
                elif not dt.find('informacionaduanera') == -1:
                    # numpedimp | fecha | aduana
                    detallecompl['cve_aduana'] = idcomp(c, 'T_Aduana', 'cve_Aduana', 7)
                    dtstr = data[con][dt]
                    inseradd(c, dtstr, detallecompl['cve_aduana'], 'aduana')
                    # c.commit()
            detallecompl['version'] = convtipo(detallecompl['version'], 'float')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['serie'],
                   detallecompl['numfoldesveh'], detallecompl['cve_aduana'])
            cur.execute("INSERT INTO certificadodedestruccion VALUES(?,?,?,?,?,?)", reg)
            # c.commit()
        elif con == 'complemento_spei':
            for dt in data[con]:
                # fechaoperacion | hora | clavespei | sello | numerocertificado | cadenacda
                spter = data[con][dt]
                compl = ('UUID', 'id', 'version', 'fechaoperacion', 'hora', 'clavespei', 'sello', 'numerocertificado',
                         'cadenacda')
                detallecompl = dict.fromkeys(compl)
                detallecompl['UUID'] = uuid
                detallecompl['id'] = idcomp(c, 'Complemento_SPEI')
                detallecompl['version'] = 1.0
                for dt_ in spter:
                    if dt_ in detallecompl:
                        detallecompl[dt_] = data[con][dt][dt_]
                    elif not dt_.find('ordenante') == -1:
                        # bancoemisor | nombre | tipocuenta | cuenta | rfc
                        dtstr = spter[dt_]
                        subcompl = ('id', 'tipo', 'bancoemisor', 'nombre', 'tipocuenta', 'cuenta', 'rfc',
                                    'concepto', 'iva', 'montopago')
                        detallesubcompl = dict.fromkeys(subcompl)
                        detallesubcompl.update(dtstr)
                        detallesubcompl['id'] = detallecompl['id']
                        detallesubcompl['tipo'] = "ORDENANTE"
                        reg = (detallesubcompl['id'], detallesubcompl['tipo'], detallesubcompl['bancoemisor'],
                               detallesubcompl['nombre'], detallesubcompl['tipocuenta'], detallesubcompl['cuenta'],
                               detallesubcompl['rfc'], detallesubcompl['concepto'], detallesubcompl['iva'],
                               detallesubcompl['montopago'])
                        cur.execute("INSERT INTO DatosBancarios VALUES(?,?,?,?,?,?,?,?,?,?)", reg)
                        # c.commit()
                    elif not dt_.find('beneficiario') == -1:
                        # bancoreceptor | nombre | tipocuenta | cuenta | rfc | concepto | iva | montopago
                        dtstr = spter[dt_]
                        subcompl = ('id', 'tipo', 'bancoreceptor', 'nombre', 'tipocuenta', 'cuenta', 'rfc',
                                    'concepto', 'iva', 'montopago')
                        detallesubcompl = dict.fromkeys(subcompl)
                        detallesubcompl.update(dtstr)
                        detallesubcompl['id'] = detallecompl['id']
                        detallesubcompl['tipo'] = "BENEFICIARIO"
                        detallesubcompl['iva'] = convtipo(detallesubcompl['iva'], 'float')
                        detallesubcompl['montopago'] = convtipo(detallesubcompl['montopago'], 'float')
                        reg = (detallesubcompl['id'], detallesubcompl['tipo'], detallesubcompl['bancoreceptor'],
                               detallesubcompl['nombre'], detallesubcompl['tipocuenta'], detallesubcompl['cuenta'],
                               detallesubcompl['rfc'], detallesubcompl['concepto'], detallesubcompl['iva'],
                               detallesubcompl['montopago'])
                        cur.execute("INSERT INTO DatosBancarios VALUES(?,?,?,?,?,?,?,?,?,?)", reg)
                        # c.commit()
                detallecompl['version'] = convtipo(detallecompl['version'], 'float')
                detallecompl['fechaoperacion'] = convtipo(detallecompl['fechaoperacion'], 'date2')
                detallecompl['version'] = convtipo(detallecompl['version'], 'float')
                detallecompl['hora'] = convtipo(detallecompl['hora'], 'hora')
                reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'],
                       detallecompl['fechaoperacion'], detallecompl['hora'], detallecompl['clavespei'],
                       detallecompl['sello'], detallecompl['numerocertificado'], detallecompl['cadenacda'])
                cur.execute("INSERT INTO Complemento_SPEI VALUES(?,?,?,?,?,?,?,?,?)", reg)
                # c.commit()
        elif con == 'impuestoslocales':
            # version | totalderetenciones | totaldetraslados
            compl = ('UUID', 'id', 'version', 'totalderetenciones', 'totaldetraslados', 'id_impuestos')
            detallecompl = dict.fromkeys(compl)
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'ImpuestosLocales')
            detallecompl['id_impuestos'] = idcomp(c, 'T_Impuestos', 'id', 10)
            for dt in data[con]:
                if dt in detallecompl:
                    detallecompl[dt] = data[con][dt]
                elif not dt.find('retenciones') == -1:
                    # implocretenido | tasaderetencion | importe
                    dtstr = data[con][dt]
                    subcompl = ('id', 'tipo', 'base', 'implocretenido', 'tipofactor', 'tasaderetencion', 'importe')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl.update(dtstr)
                    detallesubcompl['impuesto'] = detallesubcompl.pop('implocretenido')
                    detallesubcompl['tasaocuota'] = detallesubcompl.pop('tasaderetencion')
                    insertimp(c, detallesubcompl, detallecompl['id_impuestos'], 'RETENCION')
                    # c.commit()
                elif not dt.find('traslados') == -1:
                    # imploctrasladado | tasadetraslado | importe
                    dtstr = data[con][dt]
                    subcompl = ('id', 'tipo', 'base', 'imploctrasladado', 'tipofactor', 'tasadetraslado', 'importe')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl.update(dtstr)
                    detallesubcompl['impuesto'] = detallesubcompl.pop('imploctrasladado')
                    detallesubcompl['tasaocuota'] = detallesubcompl.pop('tasadetraslado')
                    insertimp(c, detallesubcompl, detallecompl['id_impuestos'], 'TRASLADO')
                    # c.commit()
            detallecompl['version'] = convtipo(detallecompl['version'], 'float')
            detallecompl['totalderetenciones'] = convtipo(detallecompl['totalderetenciones'], 'float')
            detallecompl['totaldetraslados'] = convtipo(detallecompl['totaldetraslados'], 'float')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'],
                   detallecompl['totalderetenciones'], detallecompl['totaldetraslados'], detallecompl['id_impuestos'])
            cur.execute("INSERT INTO ImpuestosLocales VALUES(?,?,?,?,?,?)", reg)
            # c.commit()
        elif con == 'renovacionysustitucionvehiculos':
            # version | tipodedecreto
            compl = ('UUID', 'id', 'version', 'tipodedecreto')
            detallecompl = dict.fromkeys(compl)
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'renovacionysustitucionvehiculos')
            for dt in data[con]:
                if dt in detallecompl:
                    detallecompl[dt] = data[con][dt]
                elif not dt.find('decretorenovvehicular') == -1:
                    # vehenaj
                    dtstr = data[con][dt]
                    subcompl = ('id_rsv', 'id', 'tipo', 'vehenaj')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl['id_rsv'] = detallecompl['id']
                    detallesubcompl['id'] = idcomp(c, 'DecretoRenovSustitVehicular')
                    detallesubcompl['tipo'] = "RENOVACION"
                    detallesubcompl['vehenaj'] = dtstr['vehenaj']
                    for dt_ in dtstr:
                        if not dt_.find('enadopermalfab') == -1:
                            # preciovehusado | tipoveh | marca | tipooclase | año | modelo | niv | numserie | numplacas
                            # nummotor | numfoltarjcir | numpedim | aduana | fecharegulveh | foliofiscal
                            dtsub = dtstr[dt_]
                            subsubcompl = ('id', 'preciovehusado', 'tipoveh', 'marca', 'tipooclase', 'año', 'modelo',
                                           'niv', 'numserie', 'numplacas', 'nummotor', 'numfoltarjcir',
                                           'numfolavisoint', 'numpedim', 'aduana', 'fecharegulveh', 'foliofiscal')
                            detallesubsubcompl = dict.fromkeys(subsubcompl)
                            detallesubsubcompl.update(dtsub)
                            detallesubsubcompl['id'] = detallesubcompl['id']
                            reg = (detallesubsubcompl['id'], detallesubsubcompl['preciovehusado'],
                                   detallesubsubcompl['tipoveh'], detallesubsubcompl['marca'],
                                   detallesubsubcompl['tipooclase'], detallesubsubcompl['año'],
                                   detallesubsubcompl['modelo'], detallesubsubcompl['niv'],
                                   detallesubsubcompl['numserie'], detallesubsubcompl['numplacas'],
                                   detallesubsubcompl['nummotor'], detallesubsubcompl['numfoltarjcir'],
                                   detallesubsubcompl['numfolavisoint'], detallesubsubcompl['numpedim'],
                                   detallesubsubcompl['aduana'], detallesubsubcompl['fecharegulveh'],
                                   detallesubsubcompl['foliofiscal'])
                            detallesubsubcompl['año'] = convtipo(detallesubsubcompl['año'], 'int')
                            detallesubsubcompl['fecharegulveh'] = convtipo(detallesubsubcompl['fecharegulveh'], 'date2')
                            cur.execute("INSERT INTO VehiculosUsadosEnajenadoPermAlFab " +
                                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", reg)
                            # c.commit()
                        elif not dt_.find('enajenadofabalperm') == -1:
                            # año | modelo | numplacas | rfc
                            dtsub = dtstr[dt_]
                            subsubcompl = ('id', 'año', 'modelo', 'numplacas', 'rfc')
                            detallesubsubcompl = dict.fromkeys(subsubcompl)
                            detallesubsubcompl.update(dtsub)
                            detallesubsubcompl['id'] = detallesubcompl['id']
                            detallesubsubcompl['año'] = convtipo(detallesubsubcompl['año'], 'int')
                            reg = (detallesubsubcompl['id'], detallesubsubcompl['año'],
                                   detallesubsubcompl['modelo'], detallesubsubcompl['numplacas'],
                                   detallesubsubcompl['rfc'])
                            cur.execute("INSERT INTO VehiculoNuvoSemEnajenadoFabAlPerm " +
                                        "VALUES(?,?,?,?,?)", reg)
                            # c.commit()
                    reg = (detallesubcompl['id_rsv'], detallesubcompl['id'],
                           detallesubcompl['tipo'], detallesubcompl['vehenaj'])
                    cur.execute("INSERT INTO DecretoRenovSustitVehicular VALUES(?,?,?,?)", reg)
                    # c.commit()
                elif not dt.find('decretosustitvehicular') == -1:
                    # vehenaj
                    dtstr = data[con][dt]
                    subcompl = ('id_rsv', 'id', 'tipo', 'vehenaj')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl['id_rsv'] = detallecompl['id']
                    detallesubcompl['id'] = idcomp(c, 'DecretoRenovSustitVehicular')
                    detallesubcompl['tipo'] = "SUSTITUCION"
                    detallesubcompl['vehenaj'] = dtstr['vehenaj']
                    for dt_ in dtstr:
                        if not dt_.find('enadopermalfab') == -1:
                            # preciovehusado | tipoveh | marca | tipooclase | año | modelo | niv | numserie | numplacas
                            # nummotor | numfoltarjcir | numfolavisoint | numpedim | aduana | fecharegulveh
                            # foliofiscal
                            dtsub = dtstr[dt_]
                            subsubcompl = ('id', 'preciovehusado', 'tipoveh', 'marca', 'tipooclase', 'año', 'modelo',
                                           'niv', 'numserie', 'numplacas', 'nummotor', 'numfoltarjcir',
                                           'numfolavisoint', 'numpedim', 'aduana', 'fecharegulveh', 'foliofiscal')
                            detallesubsubcompl = dict.fromkeys(subsubcompl)
                            detallesubsubcompl.update(dtsub)
                            detallesubsubcompl['id'] = detallesubcompl['id']
                            reg = (detallesubsubcompl['id'], detallesubsubcompl['preciovehusado'],
                                   detallesubsubcompl['tipoveh'], detallesubsubcompl['marca'],
                                   detallesubsubcompl['tipooclase'], detallesubsubcompl['año'],
                                   detallesubsubcompl['modelo'], detallesubsubcompl['niv'],
                                   detallesubsubcompl['numserie'], detallesubsubcompl['numplacas'],
                                   detallesubsubcompl['nummotor'], detallesubsubcompl['numfoltarjcir'],
                                   detallesubsubcompl['numfolavisoint'], detallesubsubcompl['numpedim'],
                                   detallesubsubcompl['aduana'], detallesubsubcompl['fecharegulveh'],
                                   detallesubsubcompl['foliofiscal'])
                            detallesubsubcompl['año'] = convtipo(detallesubsubcompl['año'], 'int')
                            detallesubsubcompl['fecharegulveh'] = convtipo(detallesubsubcompl['fecharegulveh'], 'date2')
                            cur.execute("INSERT INTO VehiculosUsadosEnajenadoPermAlFab " +
                                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", reg)
                            # c.commit()
                        elif not dt_.find('enajenadofabalperm') == -1:
                            # año | modelo | numplacas | rfc
                            dtsub = dtstr[dt_]
                            subsubcompl = ('id', 'año', 'modelo', 'numplacas', 'rfc')
                            detallesubsubcompl = dict.fromkeys(subsubcompl)
                            detallesubsubcompl.update(dtsub)
                            detallesubsubcompl['id'] = detallesubcompl['id']
                            reg = (detallesubsubcompl['id'], detallesubsubcompl['año'],
                                   detallesubsubcompl['modelo'], detallesubsubcompl['numplacas'],
                                   detallesubsubcompl['rfc'])
                            detallesubsubcompl['año'] = convtipo(detallesubsubcompl['año'], 'int')
                            cur.execute("INSERT INTO VehiculoNuvoSemEnajenadoFabAlPerm " +
                                        "VALUES(?,?,?,?,?)", reg)
                            # c.commit()
                    reg = (detallesubcompl['id_rsv'], detallesubcompl['id'],
                           detallesubcompl['tipo'], detallesubcompl['vehenaj'])
                    cur.execute("INSERT INTO DecretoRenovSustitVehicular VALUES(?,?,?,?)", reg)
                    # c.commit()
            detallecompl['version'] = convtipo(detallecompl['version'], 'float')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['tipodedecreto'])
            cur.execute("INSERT INTO renovacionysustitucionvehiculos VALUES(?,?,?,?)", reg)
            # c.commit()
        elif con == 'notariospublicos':
            # version
            compl = ('UUID', 'id', 'version')
            detallecompl = dict.fromkeys(compl)
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'NotariosPublicos')
            detallecompl['version'] = data[con]['version']
            for dt in data[con]:
                if not dt.find('descinmuebles') == -1:
                    inmuebles = data[con][dt]
                    for inm in inmuebles:
                        # tipoinmueble | calle | noexterior | nointerior | colonia | localidad | referencia |
                        # municipio | estado | pais | codigopostal
                        subcompl = ('id', 'tipoinmueble', 'calle', 'noexterior', 'nointerior', 'colonia', 'localidad',
                                    'referencia', 'municipio', 'estado', 'pais', 'codigopostal')
                        detallesubcompl = dict.fromkeys(subcompl)
                        detallesubcompl.update(inmuebles[inm])
                        detallesubcompl['id'] = detallecompl['id']
                        reg = (detallesubcompl['id'], detallesubcompl['tipoinmueble'], detallesubcompl['calle'],
                               detallesubcompl['noexterior'], detallesubcompl['nointerior'], detallesubcompl['colonia'],
                               detallesubcompl['localidad'], detallesubcompl['referencia'],
                               detallesubcompl['municipio'], detallesubcompl['estado'], detallesubcompl['pais'],
                               detallesubcompl['codigopostal'])
                        cur.execute("INSERT INTO DescInmueble VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", reg)
                        # c.commit()
                elif not dt.find('datosoperacion') == -1:
                    # numinstrumentonotarial | fechainstnotarial | montooperacion | subtotal | iva
                    operacion = data[con][dt]
                    subcompl = ('id', 'numinstrumentonotarial', 'fechainstnotarial', 'montooperacion', 'subtotal',
                                'iva')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl.update(operacion)
                    detallesubcompl['id'] = detallecompl['id']
                    detallesubcompl['fechainstnotarial'] = convtipo(detallesubcompl['fechainstnotarial'], 'date2')
                    detallesubcompl['montooperacion'] = convtipo(detallesubcompl['montooperacion'], 'float')
                    detallesubcompl['subtotal'] = convtipo(detallesubcompl['subtotal'], 'float')
                    detallesubcompl['iva'] = convtipo(detallesubcompl['iva'], 'float')
                    reg = (detallesubcompl['id'], detallesubcompl['numinstrumentonotarial'],
                           detallesubcompl['fechainstnotarial'], detallesubcompl['montooperacion'],
                           detallesubcompl['subtotal'], detallesubcompl['iva'])
                    cur.execute("INSERT INTO DatosOperacion VALUES(?,?,?,?,?,?)", reg)
                    # c.commit()
                elif not dt.find('datosnotario') == -1:
                    # curp | numnotaria | entidadfederativa | adscripcion
                    notario = data[con][dt]
                    subcompl = ('id', 'curp', 'numnotaria', 'entidadfederativa', 'adscripcion')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl.update(notario)
                    reg = (detallesubcompl['id'], detallesubcompl['curp'], detallesubcompl['numnotaria'],
                           detallesubcompl['entidadfederativa'], detallesubcompl['adscripcion'])
                    cur.execute("INSERT INTO DatosNotario VALUES(?,?,?,?,?)", reg)
                    # c.commit()
                elif not dt.find('datosenajenante') == -1 or not dt.find('datosadquiriente') == -1:
                    datos = data[con][dt]
                    if not dt.find('datosenajenante') == -1:
                        tipo = 'ENAJENANTE'
                    else:
                        tipo = 'ADQUIRIENTE'
                    subcompl = ('id', 'tipo', 'coprosocconyugale', 'nombre', 'apellidopaterno', 'apellidomaterno',
                                'rfc', 'curp', 'porcentaje')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl['id'] = detallecompl['id']
                    detallesubcompl['tipo'] = tipo
                    detallesubcompl['coprosocconyugale'] = datos['coprosocconyugale']
                    i = 0
                    for dt_ in datos:
                        if not dt.find('datosun') == -1:
                            enadq = datos[dt_]
                            detallesubcompl.update(enadq)
                            detallesubcompl['porcentaje'] = convtipo(detallesubcompl['porcentaje'], 'float')
                            reg = (detallesubcompl['id'], detallesubcompl['tipo'], detallesubcompl['coprosocconyugale'],
                                   detallesubcompl['nombre'], detallesubcompl['apellidopaterno'],
                                   detallesubcompl['apellidomaterno'], detallesubcompl['rfc'], detallesubcompl['curp'],
                                   detallesubcompl['porcentaje'])
                            cur.execute("INSERT INTO DatosEnajenanteAdquiriente VALUES(?,?,?,?,?,?,?,?,?)", reg)
                            # c.commit()
                        elif not dt.find('copsc') == -1:
                            enadq = datos[dt_]
                            for soc in enadq:
                                if i != 0:
                                    detallesubcompl = dict.fromkeys(subcompl)
                                    detallesubcompl['id'] = detallecompl['id']
                                    detallesubcompl['tipo'] = tipo
                                    detallesubcompl['coprosocconyugale'] = datos['coprosocconyugale']
                                else:
                                    i += 1
                                detallesubcompl.update(enadq[soc])
                                detallesubcompl['porcentaje'] = convtipo(detallesubcompl['porcentaje'], 'float')
                                reg = (detallesubcompl['id'], detallesubcompl['tipo'],
                                       detallesubcompl['coprosocconyugale'], detallesubcompl['nombre'],
                                       detallesubcompl['apellidopaterno'], detallesubcompl['apellidomaterno'],
                                       detallesubcompl['rfc'], detallesubcompl['curp'], detallesubcompl['porcentaje'])
                                cur.execute("INSERT INTO DatosEnajenanteAdquiriente VALUES(?,?,?,?,?,?,?,?,?)", reg)
                                # c.commit()
            detallecompl['version'] = convtipo(detallecompl['version'], 'float')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'])
            cur.execute("INSERT INTO NotariosPublicos VALUES(?,?,?)", reg)
            # c.commit()
        elif con == 'comercioexterior':
            # version | motivotraslado | tipooperacion | clavedepedimento | certificadodeorigen |
            # numeroexportadorconfiable | incoterm | subdivision | observaciones | tipocambiousd | totalusd
            compl = ('UUID', 'id', 'version', 'motivotraslado', 'tipooperacion', 'clavedepedimento',
                     'certificadodeorigen', 'numeroexportadorconfiable', 'incoterm', 'subdivision', 'observaciones',
                     'tipocambiousd', 'totalusd')
            detallecompl = dict.fromkeys(compl)
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'ComercioExterior')
            for dt in data[con]:
                if dt in detallecompl:
                    detallecompl[dt] = data[con][dt]
                elif not dt.find('emisor') == -1:
                    # curp
                    dtstr = data[con][dt]
                    subcompl = ('id', 'curp', 'id_domicilio')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl['id'] = detallecompl['id']
                    if 'curp' in dtstr:
                        detallesubcompl['curp'] = dtstr['curp']
                    for dt_ in dtstr:
                        if not dt_.find('domicilio') == -1:
                            if detallesubcompl['id_domicilio'] is None:
                                detallesubcompl['id_domicilio'] = idcomp(c, 'Domicilio_CE')
                            insertadomce(c, dtstr[dt_], detallesubcompl['id_domicilio'])
                    reg = (detallesubcompl['id'], detallesubcompl['curp'], detallesubcompl['id_domicilio'])
                    cur.execute("INSERT INTO Emisor_CE VALUES(?,?,?)", reg)
                elif not dt.find('propietario') == -1:
                    # numregidtrib | residenciafiscal
                    dtstr = data[con][dt]
                    subcompl = ('id', 'numregidtrib', 'residenciafiscal')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl['id'] = detallecompl['id']
                    detallesubcompl.update(dtstr)
                    reg = (detallesubcompl['id'], detallesubcompl['numregidtrib'], detallesubcompl['residenciafiscal'])
                    cur.execute("INSERT INTO Propietario_CE VALUES(?,?,?)", reg)
                elif not dt.find('receptor') == -1:
                    # numregidtrib
                    dtstr = data[con][dt]
                    subcompl = ('id', 'numregidtrib', 'id_domicilio')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl['id'] = detallecompl['id']
                    if 'numregidtrib' in dtstr:
                        detallesubcompl['numregidtrib'] = dtstr['numregidtrib']
                    for dt_ in dtstr:
                        if not dt_.find('domicilio') == -1:
                            if detallesubcompl['id_domicilio'] is None:
                                detallesubcompl['id_domicilio'] = idcomp(c, 'Domicilio_CE')
                            insertadomce(c, dtstr[dt_], detallesubcompl['id_domicilio'])
                    reg = (detallesubcompl['id'], detallesubcompl['numregidtrib'], detallesubcompl['id_domicilio'])
                    cur.execute("INSERT INTO Receptor_CE VALUES(?,?,?)", reg)
                elif not dt.find('destinatario') == -1:
                    # numregidtrib | Nombre
                    dtstr = data[con][dt]
                    subcompl = ('id', 'numregidtrib', 'Nombre', 'id_domicilio')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl['id'] = detallecompl['id']
                    for dt_ in dtstr:
                        if dt_ in detallesubcompl:
                            detallesubcompl[dt_] = dtstr[dt_]
                        elif not dt_.find('domicilio') == -1:
                            if detallesubcompl['id_domicilio'] is None:
                                detallesubcompl['id_domicilio'] = idcomp(c, 'Domicilio_CE')
                            insertadomce(c, dtstr[dt_], detallesubcompl['id_domicilio'])
                    reg = (detallesubcompl['id'], detallesubcompl['numregidtrib'], detallesubcompl['Nombre'],
                           detallesubcompl['id_domicilio'])
                    cur.execute("INSERT INTO Destinatario_CE VALUES(?,?,?,?)", reg)
                elif not dt.find('mercancias') == -1:
                    # noidentificacion | fraccionarancelaria | cantidadaduana | unidadaduana | valorunitarioaduana |
                    # valordolares
                    dtstr = data[con][dt]
                    subcompl = ('id', 'noidentificacion', 'fraccionarancelaria', 'cantidadaduana', 'unidadaduana',
                                'valorunitarioaduana', 'valordolares', 'id_descripcionesespecificas')
                    for dt_ in dtstr:
                        detallesubcompl = dict.fromkeys(subcompl)
                        detallesubcompl['id'] = detallecompl['id']
                        merc = dtstr[dt_]
                        for _dt in merc:
                            if _dt in detallesubcompl:
                                detallesubcompl[_dt] = merc[_dt]
                            elif not _dt.find('descripciones') == -1:
                                # marca | modelo | submodelo | numeroserie
                                idds = 'id_descripcionesespecificas'
                                if detallesubcompl[idds] is None:
                                    detallesubcompl[idds] = idcomp(c, 'Mercancias_CE', idds)
                                descesp = merc[_dt]
                                descespkeys = ('id', 'marca', 'modelo', 'submodelo', 'numeroserie')
                                dtdescesp = dict.fromkeys(descespkeys)
                                dtdescesp.update(descesp)
                                dtdescesp['id'] = detallesubcompl[idds]
                                reg = (dtdescesp['id'], dtdescesp['marca'], dtdescesp['modelo'], dtdescesp['submodelo'],
                                       dtdescesp['numeroserie'])
                                cur.execute("INSERT INTO DescripcionesEspecificas_CE VALUES(?,?,?,?,?)", reg)
                        detallesubcompl['cantidadaduana'] = convtipo(detallesubcompl['cantidadaduana'], 'float')
                        detallesubcompl['valorunitarioaduana'] = convtipo(detallesubcompl['valorunitarioaduana'],
                                                                          'float')
                        detallesubcompl['valordolares'] = convtipo(detallesubcompl['valordolares'], 'float')
                        reg = (detallesubcompl['id'], detallesubcompl['noidentificacion'],
                               detallesubcompl['fraccionarancelaria'], detallesubcompl['cantidadaduana'],
                               detallesubcompl['unidadaduana'], detallesubcompl['valorunitarioaduana'],
                               detallesubcompl['valordolares'], detallesubcompl['id_descripcionesespecificas'])
                        cur.execute("INSERT INTO Mercancias_CE VALUES(?,?,?,?,?,?,?,?)", reg)
            detallecompl['version'] = convtipo(detallecompl['version'], 'float')
            detallecompl['tipocambiousd'] = convtipo(detallecompl['tipocambiousd'], 'float')
            detallecompl['totalusd'] = convtipo(detallecompl['totalusd'], 'float')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['motivotraslado'],
                   detallecompl['tipooperacion'], detallecompl['clavedepedimento'], detallecompl['certificadodeorigen'],
                   detallecompl['numeroexportadorconfiable'], detallecompl['incoterm'], detallecompl['subdivision'],
                   detallecompl['observaciones'], detallecompl['tipocambiousd'], detallecompl['totalusd'])
            cur.execute("INSERT INTO ComercioExterior VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", reg)
        elif con == 'pagos':
            # version | | fechapago | formadepagop | monedap | tipocambiop | monto | numoperacion | rfcemisorctaord |
            # nombancoordext | ctaordenante | rfcemisorctaben | ctabeneficiario | tipocadpago | certpago | cadpago
            # sellopago
            compl = ('UUID', 'id', 'version', 'fechapago', 'formadepagop', 'monedap', 'tipocambiop', 'monto',
                     'numoperacion', 'rfcemisorctaord', 'nombancoordext', 'ctaordenante', 'rfcemisorctaben',
                     'ctabeneficiario', 'tipocadpago', 'certpago', 'cadpago', 'sellopago', 'id_impuestos')
            v = data[con]['version']
            for dt in data[con]:
                if not dt.find('pago') == -1:
                    pago = data[con][dt]
                    detallecompl = dict.fromkeys(compl)
                    detallecompl['UUID'] = uuid
                    detallecompl['id'] = idcomp(c, 'Pagos', 'id', 7)
                    detallecompl['version'] = v
                    for _dt in pago:
                        if _dt in detallecompl:
                            detallecompl[_dt] = pago[_dt]
                        elif not _dt.find('doctorelacionado') == -1:
                            # iddocumento | serie | folio | monedadr | tipocambiodr | metododepagodr | metododepagodr |
                            # numparcialidad | impsaldoant | imppagado | impsaldoinsoluto
                            dcrl = ('id', 'iddocumento', 'serie', 'folio', 'monedadr', 'tipocambiodr', 'metododepagodr',
                                    'numparcialidad', 'impsaldoant', 'imppagado', 'impsaldoinsoluto')
                            dctorel = pago[_dt]
                            detalledctorel = dict.fromkeys(dcrl)
                            detalledctorel.update(dctorel)
                            detalledctorel['id'] = detallecompl['id']
                            detalledctorel['tipocambiodr'] = convtipo(detalledctorel['tipocambiodr'], 'float')
                            detalledctorel['impsaldoant'] = convtipo(detalledctorel['impsaldoant'], 'float')
                            detalledctorel['imppagado'] = convtipo(detalledctorel['imppagado'], 'float')
                            detalledctorel['impsaldoinsoluto'] = convtipo(detalledctorel['impsaldoinsoluto'], 'float')
                            reg = (detalledctorel['id'], detalledctorel['iddocumento'], detalledctorel['serie'],
                                   detalledctorel['folio'], detalledctorel['monedadr'], detalledctorel['tipocambiodr'],
                                   detalledctorel['metododepagodr'], detalledctorel['numparcialidad'],
                                   detalledctorel['impsaldoant'], detalledctorel['imppagado'],
                                   detalledctorel['impsaldoinsoluto'])
                            cur.execute("INSERT INTO DoctoRelacionado_Pagos VALUES(?,?,?,?,?,?,?,?,?,?,?)", reg)
                        elif not _dt.find('impuestos') == -1:
                            if detallecompl['id_impuestos'] is None:
                                detallecompl['id_impuestos'] = idcomp(c, 'Impuestos', 'UUID', 10)
                            imps = pago[_dt]
                            imps['UUID'] = detallecompl['id_impuestos']
                            for k in imps:
                                if not k.find('retenciones') == -1:
                                    imps['retenciones'] = imps.pop(k)
                            for k in imps:
                                if not k.find('traslados') == -1:
                                    imps['traslados'] = imps.pop(k)
                            dbinsertimpuestos(c, imps)
                    detallecompl['version'] = convtipo(detallecompl['version'], 'float')
                    detallecompl['fechapago'] = convtipo(detallecompl['fechapago'], 'date1')
                    detallecompl['tipocambiop'] = convtipo(detallecompl['tipocambiop'], 'float')
                    detallecompl['monto'] = convtipo(detallecompl['monto'], 'float')
                    reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['fechapago'],
                           detallecompl['formadepagop'], detallecompl['monedap'], detallecompl['tipocambiop'],
                           detallecompl['monto'], detallecompl['numoperacion'], detallecompl['rfcemisorctaord'],
                           detallecompl['nombancoordext'], detallecompl['ctaordenante'],
                           detallecompl['rfcemisorctaben'], detallecompl['ctabeneficiario'],
                           detallecompl['tipocadpago'], detallecompl['certpago'], detallecompl['cadpago'],
                           detallecompl['sellopago'], detallecompl['id_impuestos'])
                    cur.execute("INSERT INTO Pagos VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", reg)
        elif con == 'nomina':
            # version | tiponomina | fechapago | fechainicialpago | fechafinalpago | numdiaspagados | totalpercepciones
            # totaldeducciones | totalotrospagos
            compl = ('UUID', 'id', 'version', 'tiponomina', 'fechapago', 'fechainicialpago', 'fechafinalpago',
                     'numdiaspagados', 'totalpercepciones', 'totaldeducciones', 'totalotrospagos')
            nomdata = data[con]
            detallecompl = dict.fromkeys(compl)
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'Nomina', 'id', 10)
            for dt in nomdata:
                if dt in detallecompl:
                    detallecompl[dt] = nomdata[dt]
                else:
                    if not dt.find('emisor') == -1:
                        # curp | registropatronal | rfcpatronorigen
                        emi = nomdata[dt]
                        e = ('id_nomina', 'id', 'curp', 'registropatronal', 'rfcpatronorigen')
                        emisor = dict.fromkeys(e)
                        emisor['id_nomina'] = detallecompl['id']
                        emisor['id'] = idcomp(c, 'Emisor_Nomina', 'id', 5)
                        for dt_ in emi:
                            if dt_ in emisor:
                                emisor[dt_] = emi[dt_]
                            else:
                                if not dt_.find('entidadsncf') == -1:
                                    # origenrecurso | montorecursopropio
                                    esncf = ('id', 'origenrecurso', 'montorecursopropio')
                                    entidad = dict.fromkeys(esncf)
                                    entidad.update(emi[dt_])
                                    entidad['id'] = emisor['id']
                                    entidad['montorecursopropio'] = convtipo(entidad['montorecursopropio'], 'float')
                                    reg = (entidad['id'], entidad['origenrecurso'], entidad['montorecursopropio'])
                                    cur.execute("INSERT INTO EntidadSNCF VALUES(?,?,?)", reg)
                        reg = (emisor['id_nomina'], emisor['id'], emisor['curp'], emisor['registropatronal'],
                               emisor['rfcpatronorigen'])
                        cur.execute("INSERT INTO Emisor_Nomina VALUES(?,?,?,?,?)", reg)
                    elif not dt.find('receptor') == -1:
                        # curp | numseguridadsocial | fechainiciorellaboral | antigüedad | tipocontrato | sindicalizado
                        # tipojornada | tiporegimen | numempleado | departamento | puesto | riesgopuesto |
                        # periodicidadpago | banco | cuentabancaria | salariobasecotapor | salariodiariointegrado |
                        # claveentfed
                        rec = nomdata[dt]
                        r = ('id_nomina', 'id', 'curp', 'numseguridadsocial', 'fechainiciorellaboral', 'antigüedad',
                             'tipocontrato', 'sindicalizado', 'tipojornada', 'tiporegimen', 'numempleado',
                             'departamento', 'puesto', 'riesgopuesto', 'periodicidadpago', 'banco', 'cuentabancaria',
                             'salariobasecotapor', 'salariodiariointegrado', 'claveentfed')
                        receptor = dict.fromkeys(r)
                        receptor['id_nomina'] = detallecompl['id']
                        receptor['id'] = idcomp(c, 'Receptor_Nomina', 'id', 5)
                        for dt_ in rec:
                            if dt_ in receptor:
                                receptor[dt_] = rec[dt_]
                            else:
                                if not dt_.find('subcontratacion') == -1:
                                    # rfclabora | porcentajetiempo
                                    sc = ('id', 'rfclabora', 'porcentajetiempo')
                                    subc = dict.fromkeys(sc)
                                    subc.update(rec[dt_])
                                    subc['id'] = receptor['id']
                                    subc['porcentajetiempo'] = convtipo(subc['porcentajetiempo'], 'float')
                                    reg = (subc['id'], subc['rfclabora'], subc['porcentajetiempo'])
                                    cur.execute("INSERT INTO SubContratacion VALUES(?,?,?)", reg)
                        reg = (receptor['id_nomina'], receptor['id'], receptor['curp'], receptor['numseguridadsocial'],
                               receptor['fechainiciorellaboral'], receptor['antigüedad'], receptor['tipocontrato'],
                               receptor['sindicalizado'], receptor['tipojornada'], receptor['tiporegimen'],
                               receptor['numempleado'], receptor['departamento'], receptor['puesto'],
                               receptor['riesgopuesto'], receptor['periodicidadpago'], receptor['banco'],
                               receptor['cuentabancaria'], receptor['salariobasecotapor'],
                               receptor['salariodiariointegrado'], receptor['claveentfed'])
                        cur.execute("INSERT INTO Receptor_Nomina VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", reg)
                    elif not dt.find('percepciones') == -1:
                        # totalsueldos | totalseparacionindemnizacion | totaljubilacionpensionretiro | totalgravado
                        # totalexento
                        perc = nomdata[dt]
                        p = ('id_nomina', 'id', 'totalsueldos', 'totalseparacionindemnizacion',
                             'totaljubilacionpensionretiro', 'totalgravado', 'totalexento')
                        percepciones = dict.fromkeys(p)
                        percepciones['id_nomina'] = detallecompl['id']
                        percepciones['id'] = idcomp(c, 'Percepciones', 'id', 5)
                        for dt_ in perc:
                            if dt_ in percepciones:
                                percepciones[dt_] = perc[dt_]
                            else:
                                if not dt_.find('percepcion') == -1:
                                    # tipopercepcion  | clave | concepto | importegravado | importeexento
                                    detperc = perc[dt_]
                                    pdt = ('id_percepciones', 'id', 'tipopercepcion', 'clave', 'concepto',
                                           'importegravado', 'importeexento')
                                    percepcion = dict.fromkeys(pdt)
                                    percepcion['id_percepciones'] = percepciones['id']
                                    percepcion['id'] = idcomp(c, 'Percepcion', 'id', 5)
                                    for dt__ in detperc:
                                        if dt__ in percepcion:
                                            percepcion[dt__] = detperc[dt__]
                                        else:
                                            if not dt__.find('accionesotitulos') == -1:
                                                # valormercado | precioalotorgarse
                                                aot = ('id_percepcion', 'valormercado', 'precioalotorgarse')
                                                aotdt = dict.fromkeys(aot)
                                                aotdt.update(detperc[dt__])
                                                aotdt['id_percepcion'] = percepcion['id']
                                                aotdt['valormercado'] = convtipo(aotdt['valormercado'], 'float')
                                                aotdt['precioalotorgarse'] = convtipo(aotdt['precioalotorgarse'],
                                                                                      'float')
                                                reg = (aotdt['id_percepcion'], aotdt['valormercado'],
                                                       aotdt['precioalotorgarse'])
                                                cur.execute("INSERT INTO AccionesOTitulos VALUES(?,?,?)", reg)
                                            elif not dt__.find('horasextra') == -1:
                                                # dias | tipohoras | horasextra | importepagado
                                                he = ('id_percepcion', 'dias', 'tipohoras', 'horasextra',
                                                      'importepagado')
                                                hedt = dict.fromkeys(he)
                                                hedt.update(detperc[dt__])
                                                hedt['id_percepcion'] = percepcion['id']
                                                hedt['dias'] = convtipo(hedt['dias'], 'int')
                                                hedt['horasextra'] = convtipo(hedt['horasextra'], 'float')
                                                hedt['importepagado'] = convtipo(hedt['importepagado'], 'float')
                                                reg = (hedt['id_percepcion'], hedt['dias'], hedt['tipohoras'],
                                                       hedt['horasextra'], hedt['importepagado'])
                                                cur.execute("INSERT INTO HorasExtra VALUES(?,?,?,?,?)", reg)
                                    reg = (percepcion['id_percepciones'], percepcion['id'],
                                           percepcion['tipopercepcion'], percepcion['clave'], percepcion['concepto'],
                                           percepcion['importegravado'], percepcion['importeexento'])
                                    cur.execute("INSERT INTO Percepcion VALUES(?,?,?,?,?,?,?)", reg)
                                elif not dt_.find('jubilacionpensionretiro') == -1:
                                    # totalunaexhibicion | totalparcialidad | montodiario | ingresoacumulable |
                                    # ingresonoacumulable
                                    juboret = perc[dt_]
                                    jor = ('id_percepciones', 'id', 'totalunaexhibicion', 'totalparcialidad',
                                           'montodiario', 'ingresoacumulable', 'ingresonoacumulable')
                                    jubiret = dict.fromkeys(jor)
                                    jubiret['id_percepciones'] = percepciones['id']
                                    jubiret['id'] = idcomp(c, 'JubilacionPensionRetiro', 'id', 5)
                                    jubiret.update(juboret)
                                    jubiret['totalunaexhibicion'] = convtipo(jubiret['totalunaexhibicion'], 'float')
                                    jubiret['totalparcialidad'] = convtipo(jubiret['totalparcialidad'], 'float')
                                    jubiret['montodiario'] = convtipo(jubiret['montodiario'], 'float')
                                    jubiret['ingresoacumulable'] = convtipo(jubiret['ingresoacumulable'], 'float')
                                    jubiret['ingresonoacumulable'] = convtipo(jubiret['ingresonoacumulable'], 'float')
                                    reg = (jubiret['id_percepciones'], jubiret['id'], jubiret['totalunaexhibicion'],
                                           jubiret['totalparcialidad'], jubiret['montodiario'],
                                           jubiret['ingresoacumulable'], jubiret['ingresonoacumulable'])
                                    cur.execute("INSERT INTO JubilacionPensionRetiro VALUES(?,?,?,?,?,?,?)", reg)
                                elif not dt_.find('separacionindemnizacion') == -1:
                                    # totalpagado | numañosservicio | ultimosueldomensord | ingresoacumulable |
                                    # ingresonoacumulable
                                    sepind = perc[dt_]
                                    spd = ('id_percepciones', 'id', 'totalpagado', 'numañosservicio',
                                           'ultimosueldomensord', 'ingresoacumulable', 'ingresonoacumulable')
                                    sep_ind = dict.fromkeys(spd)
                                    sep_ind['id_percepciones'] = percepciones['id']
                                    sep_ind['id'] = idcomp(c, 'SeparacionIndemnizacion', 'id', 5)
                                    sep_ind.update(sepind)
                                    sep_ind['totalpagado'] = convtipo(sep_ind['totalpagado'], 'float')
                                    sep_ind['numañosservicio'] = convtipo(sep_ind['numañosservicio'], 'int')
                                    sep_ind['ultimosueldomensord'] = convtipo(sep_ind['ultimosueldomensord'], 'float')
                                    sep_ind['ingresoacumulable'] = convtipo(sep_ind['ingresoacumulable'], 'float')
                                    sep_ind['ingresonoacumulable'] = convtipo(sep_ind['ingresonoacumulable'], 'float')
                                    reg = (sep_ind['id_percepciones'], sep_ind['id'], sep_ind['totalpagado'],
                                           sep_ind['numañosservicio'], sep_ind['ultimosueldomensord'],
                                           sep_ind['ingresoacumulable'], sep_ind['ingresonoacumulable'])
                                    cur.execute("INSERT INTO SeparacionIndemnizacion VALUES(?,?,?,?,?,?,?)", reg)
                        reg = (percepciones['id_nomina'], percepciones['id'], percepciones['totalsueldos'],
                               percepciones['totalseparacionindemnizacion'],
                               percepciones['totaljubilacionpensionretiro'], percepciones['totalgravado'],
                               percepciones['totalexento'])
                        cur.execute("INSERT INTO Percepciones VALUES(?,?,?,?,?,?,?)", reg)
                    elif not dt.find('deducciones') == -1:
                        # totalotrasdeducciones | totalimpuestosretenidos
                        ded = nomdata[dt]
                        d = ('id_nomina', 'id', 'totalotrasdeducciones', 'totalimpuestosretenidos')
                        deducciones = dict.fromkeys(d)
                        deducciones['id_nomina'] = detallecompl['id']
                        deducciones['id'] = idcomp(c, 'Deducciones', 'id', 5)
                        for dt_ in ded:
                            if dt_ in deducciones:
                                deducciones[dt_] = ded[dt_]
                            else:
                                if not dt_.find('deduccion') == -1:
                                    # tipodeduccion | clave | concepto | importe
                                    detded = ded[dt_]
                                    deddt = ('id_deducciones', 'tipodeduccion', 'clave', 'concepto', 'importe')
                                    deduccion = dict.fromkeys(deddt)
                                    deduccion['id_deducciones'] = deducciones['id']
                                    deduccion.update(detded)
                                    deduccion['importe'] = convtipo(deduccion['importe'], 'float')
                                    reg = (deduccion['id_deducciones'], deduccion['tipodeduccion'], deduccion['clave'],
                                           deduccion['concepto'], deduccion['importe'])
                                    cur.execute("INSERT INTO Deduccion VALUES(?,?,?,?,?)", reg)
                        reg = (deducciones['id_nomina'], deducciones['id'], deducciones['totalotrasdeducciones'],
                               deducciones['totalimpuestosretenidos'])
                        cur.execute("INSERT INTO Deducciones VALUES(?,?,?,?)", reg)
                    elif not dt.find('otrospagos') == -1:
                        otpa = nomdata[dt]
                        op = ('id_nomina', 'id', 'tipootropago', 'clave', 'concepto', 'importe')
                        for dt_ in otpa:
                            if not dt_.find('otropago') == -1:
                                # tipootropago | clave | concepto | importe
                                dtotpa = otpa[dt_]
                                otropago = dict.fromkeys(op)
                                otropago['id_nomina'] = detallecompl['id']
                                otropago['id'] = idcomp(c, 'OtrosPagos', 'id', 5)
                                for dt__ in dtotpa:
                                    if dt__ in otropago:
                                        otropago[dt__] = dtotpa[dt__]
                                    else:
                                        if not dt__.find('subsidioalempleo') == -1:
                                            # subsidiocausado
                                            sub = ('id_otrospagos', 'subsidiocausado')
                                            subsidio = dict.fromkeys(sub)
                                            subsidio.update(dtotpa[dt__])
                                            subsidio['id_otrospagos'] = otropago['id']
                                            subsidio['subsidiocausado'] = convtipo(subsidio['subsidiocausado'], 'float')
                                            reg = (subsidio['id_otrospagos'], subsidio['subsidiocausado'])
                                            cur.execute("INSERT INTO SubsidioAlEmpleo VALUES(?,?)", reg)
                                        elif not dt__.find('compensacionsaldosafavor') == -1:
                                            # saldoafavor | año | remanentesalfav
                                            csf = ('id_otrospagos', 'saldoafavor', 'año', 'remanentesalfav')
                                            comp = dict.fromkeys(csf)
                                            comp.update(dtotpa[dt__])
                                            comp['id_otrospagos'] = otropago['id']
                                            comp['saldoafavor'] = convtipo(comp['saldoafavor'], 'float')
                                            comp['remanentesalfav'] = convtipo(comp['remanentesalfav'], 'float')
                                            reg = (comp['id_otrospagos'], comp['saldoafavor'], comp['año'],
                                                   comp['remanentesalfav'])
                                            cur.execute("INSERT INTO CompensacionSaldosAFavor VALUES(?,?,?,?)", reg)
                                otropago['importe'] = convtipo(otropago['importe'], 'float')
                                reg = (otropago['id_nomina'], otropago['id'], otropago['tipootropago'],
                                       otropago['clave'], otropago['concepto'], otropago['importe'])
                                cur.execute("INSERT INTO OtrosPagos VALUES(?,?,?,?,?,?)", reg)
                    elif not dt.find('incapacidades') == -1:
                        incas = nomdata[dt]
                        inc = ('id_nomina', 'id', 'diasincapacidad', 'tipoincapacidad', 'importemonetario')
                        for dt_ in incas:
                            if not dt_.find('incapacidad') == -1:
                                # diasincapacidad | tipoincapacidad | importemonetario
                                incap = incas[dt_]
                                incapacidad = dict.fromkeys(inc)
                                incapacidad['id_nomina'] = detallecompl['id']
                                incapacidad['id'] = idcomp(c, 'Incapacidades', 'id', 5)
                                incapacidad.update(incap)
                                incapacidad['diasincapacidad'] = convtipo(incapacidad['diasincapacidad'], 'int')
                                incapacidad['importemonetario'] = convtipo(incapacidad['importemonetario'], 'float')
                                reg = (incapacidad['id_nomina'], incapacidad['id'], incapacidad['diasincapacidad'],
                                       incapacidad['tipoincapacidad'], incapacidad['importemonetario'])
                                cur.execute("INSERT INTO Incapacidades VALUES(?,?,?,?,?)", reg)
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['tiponomina'],
                   detallecompl['fechapago'], detallecompl['fechainicialpago'], detallecompl['fechafinalpago'],
                   detallecompl['numdiaspagados'], detallecompl['totalpercepciones'], detallecompl['totaldeducciones'],
                   detallecompl['totalotrospagos'])
            cur.execute("INSERT INTO Nomina VALUES(?,?,?,?,?,?,?,?,?,?,?)", reg)
    cur.close()
