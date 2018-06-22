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
            r = datetime.strptime(strdta, '%H:%M:%S')
    return r


def dbinsert_cfdi(c, data):
    cur = c.cursor()
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
        return 'NO'


def dbinsert_cfdi_rels(c, data):
    # TipoRelacion(c_TipoRelacion)
    # UUID  --  Es posible que sean varios, data los trae como UUID#
    cur = c.cursor()
    i = 1
    while 'UUID' + str(i) in data:
        reg = (data['UUID'], data['tiporelacion'], data['UUID' + str(i)])
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
    for i in imp:
        impuesto = dict.fromkeys(detalle)
        impuesto.update(imp[i])
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
        insertimp(c, data['retenciones'], detimp['id_timpuestos'], 'RETENCION')
    if 'traslados' in data:
        insertimp(c, data['traslados'], detimp['id_timpuestos'], 'TRASLADO')


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
    detparte = ('id', 'numparte', 'claveprodserv', 'cantidad', 'unidad','noidentificacion', 'descripcion', 'valorunitario','importe', 'descuento', 'id_taduana')
    detalleparte = dict.fromkeys(detparte)
    detalleparte['id'] = idparte
    i = numparte
    detalleparte['numparte'] = '{0:03d}'.format(i)
    for det in parte:
        if str(type(parte[det])) == "<class 'str'>":
            detalleparte[det] = parte[det]
        else:
            detalleparte['id_taduana'] = idcomp(c, 'T_Aduana', 'cve_Aduana', 7)
            inseradd(c, parte['informacionaduanera'], detalleparte['id_taduana'], 'aduana')
            # c.commit()
    detalleparte['cantidad'] = convtipo(detalleparte['cantidad'], 'int')
    detalleparte['valorunitario'] = convtipo(detalleparte['valorunitario'], 'float')
    detalleparte['importe'] = convtipo(detalleparte['importe'], 'float')
    detalleparte['descuento'] = convtipo(detalleparte['descuento'], 'float')
    reg = (detalleparte['id'], detalleparte['numparte'], detalleparte['claveprodserv'], detalleparte['cantidad'], detalleparte['unidad'], detalleparte['noidentificacion'], detalleparte['descripcion'], detalleparte['valorunitario'], detalleparte['importe'], detalleparte['descuento'], detalleparte['id_taduana'])
    c.execute("INSERT INTO C_Parte VALUES(?,?,?,?,?,?,?,?,?,?,?)", reg)


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
                            insertimp(c, {'imp': addondict[imp]}, detalleconcepto['id_impuestos'], tp)
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
                                        detallecomp = {**detallecomp, **addondict}
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
                                        detallecomp = {**detallecomp, **addondict}
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
                                                    inseradd(c, addondict[dt], detallecomp['cve_aduana'], 'aduana')
                                                    # c.commit()
                                            elif not dt1.find('parte') == -1:
                                                detallecomp['cve_partes'] = idcomp(c, 'C_Parte', 'cve_Predial', 5)
                                                i = int(dt.replace('parte', ''))
                                                insertpart(c, addondict[dt], detallecomp['cve_partes'], i)
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
                                                detparte = ('id', 'numparte', 'claveprodserv', 'cantidad', 'unidad',
                                                            'noidentificacion', 'descripcion', 'valorunitario',
                                                            'importe', 'descuento', 'id_taduana')
                                                detalleparte = dict.fromkeys(detparte)
                                                detalleparte['id'] = detalletercero['cve_partes']
                                                detalleparte['numparte'] = '{0:03d}'.format(pte)
                                                for d in addondict[dt_]:
                                                    if str(type(addondict[dt_][d])) == "<class 'str'>":
                                                        detalleparte[d] = addondict[dt_][d]
                                                    else:
                                                        # informacionaduanera: numero | fecha | aduana
                                                        detalleparte['id_taduana'] = idcomp(c, 'T_Aduana', 'cve_Aduana',
                                                                                            7)
                                                        detadu = ('cve_aduana', 'numero', 'fecha', 'aduana')
                                                        detalleadu = dict.fromkeys(detadu)
                                                        detalleadu['cve_aduana'] = detalleparte['id_taduana']
                                                        detalleadu = {**detalleadu, **addondict[dt_][d]}
                                                        if detalleadu['fecha'] is not None:
                                                            # "YYYY-MM-DD"
                                                            detalleadu['fecha'] = datetime.strptime(detalleadu['fecha'],
                                                                                                    '%Y-%m-%d')
                                                        reg = (detalleadu['cve_aduana'], detalleadu['numero'],
                                                               detalleadu['fecha'],
                                                               detalleadu['aduana'])
                                                        cur.execute("INSERT INTO T_Aduana VALUES(?,?,?,?)", reg)
                                                        # c.commit()
                                                if detalleparte['cantidad'] is not None:
                                                    detalleparte['cantidad'] = int(detalleparte['cantidad'])
                                                if detalleparte['valorunitario'] is not None:
                                                    detalleparte['valorunitario'] = float(detalleparte['valorunitario'])
                                                if detalleparte['importe'] is not None:
                                                    detalleparte['importe'] = float(detalleparte['importe'])
                                                if detalleparte['descuento'] is not None:
                                                    detalleparte['descuento'] = float(detalleparte['descuento'])
                                                reg = (detalleparte['id'], detalleparte['numparte'],
                                                       detalleparte['claveprodserv'],
                                                       detalleparte['cantidad'], detalleparte['unidad'],
                                                       detalleparte['noidentificacion'], detalleparte['descripcion'],
                                                       detalleparte['valorunitario'], detalleparte['importe'],
                                                       detalleparte['descuento'], detalleparte['id_taduana'])
                                                cur.execute("INSERT INTO C_Parte VALUES(?,?,?,?,?,?,?,?,?,?,?)", reg)
                                                # c.commit()
                                            elif not str(dt_).find('impuestos') == -1:
                                                if detalletercero['cve_impuestos'] is None:
                                                    detalletercero['cve_impuestos'] = idcomp(c, 'T_Impuestos', 'id', 10)
                                                detimp = ('id', 'tipo', 'base', 'impuesto', 'tipofactor', 'tasaocuota',
                                                          'importe')
                                                for lvl in addondict[dt_]:
                                                    for imp in addondict[dt_][lvl]:
                                                        detalleimp = dict.fromkeys(detimp)
                                                        detalleimp['id'] = detalletercero['cve_impuestos']
                                                        if not str(imp).find("traslado") == -1:
                                                            detalleimp['tipo'] = 'TRASLADO'
                                                        else:
                                                            detalleimp['tipo'] = 'RETENCION'
                                                        detalleimp = {**detalleimp, **addondict[dt_][lvl][imp]}
                                                        if 'tasa' in detalleimp:
                                                            detalleimp['tasaocuota'] = detalleimp.pop('tasa')
                                                        if detalleimp['base'] is not None:
                                                            detalleimp['base'] = float(detalleimp['base'])
                                                        if detalleimp['tasaocuota'] is not None:
                                                            detalleimp['tasaocuota'] = float(detalleimp['tasaocuota'])
                                                        if detalleimp['importe'] is not None:
                                                            detalleimp['importe'] = float(detalleimp['importe'])
                                                        reg = (detalleimp['id'], detalleimp['tipo'], detalleimp['base'],
                                                               detalleimp['impuesto'],
                                                               detalleimp['tipofactor'], detalleimp['tasaocuota'],
                                                               detalleimp['importe'])
                                                        cur.execute("INSERT INTO T_Impuestos VALUES(?,?,?,?,?,?,?)",
                                                                    reg)
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
                                                # detalle_ift = {**detalle_ift, **addondict[dt_]}
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
                                                detadu = ('cve_aduana', 'numero', 'fecha', 'aduana')
                                                detalleadu = dict.fromkeys(detadu)
                                                detalleadu['cve_aduana'] = detalletercero['cve_aduana']
                                                detalleadu = {**detalleadu, **addondict[dt_]}
                                                if detalleadu['fecha'] is not None:
                                                    # "YYYY-MM-DD"
                                                    detalleadu['fecha'] = datetime.strptime(detalleadu['fecha'],
                                                                                            '%Y-%m-%d')
                                                reg = (detalleadu['cve_aduana'], detalleadu['numero'],
                                                       detalleadu['fecha'],
                                                       detalleadu['aduana'])
                                                cur.execute("INSERT INTO T_Aduana VALUES(?,?,?,?)", reg)
                                                # c.commit()
                                            elif not str(dt_).find('predial') == -1:
                                                detalletercero['cve_predial'] = idcomp(c, 'T_Predial', 'cve_Predial', 5)
                                                detpred = ('cve_predial', 'numero')
                                                detallepred = dict.fromkeys(detpred)
                                                detallepred['cve_predial'] = detalletercero['cve_predial']
                                                detallepred['numero'] = addondict[dt_]['numero']
                                                reg = (detallepred['cve_predial'], detallepred['numero'])
                                                cur.execute("INSERT INTO T_Predial VALUES(?,?)", reg)
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
                                    detalleconcepto['cvePartes'] = idcomp(c, 'C_Parte', 'cve_Predial', 5)
                                    detparte = ('id', 'numparte', 'claveprodserv', 'cantidad', 'unidad',
                                                'noidentificacion', 'descripcion', 'valorunitario', 'importe',
                                                'descuento', 'id_taduana')
                                    detalleparte = dict.fromkeys(detparte)
                                    detalleparte['id'] = detalleconcepto['cvePartes']
                                    i = int(addon.replace('parte', ''))
                                    detalleparte['numparte'] = '{0:03d}'.format(i)
                                    for _dt in addondict:
                                        if str(type(addondict[_dt])) == "<class 'str'>":
                                            detalleparte[_dt] = addondict[_dt]
                                        else:
                                            # informacionaduanera: NumeroPedimento
                                            detalleparte['id_taduana'] = idcomp(c, 'T_Aduana', 'cve_Aduana', 7)
                                            detadu = ('cve_aduana', 'numero', 'fecha', 'aduana')
                                            detalleadu = dict.fromkeys(detadu)
                                            detalleadu['cve_aduana'] = detalleparte['id_taduana']
                                            # NumeroPedimento
                                            detalleadu['numero'] = addondict['informacionaduanera']['numeropedimento']
                                            reg = (detalleadu['cve_aduana'], detalleadu['numero'], detalleadu['fecha'],
                                                   detalleadu['aduana'])
                                            cur.execute("INSERT INTO T_Aduana VALUES(?,?,?,?)", reg)
                                            # c.commit()
                                    if detalleparte['cantidad'] is not None:
                                        detalleparte['cantidad'] = int(detalleparte['cantidad'])
                                    if detalleparte['valorunitario'] is not None:
                                        detalleparte['valorunitario'] = float(detalleparte['valorunitario'])
                                    if detalleparte['importe'] is not None:
                                        detalleparte['importe'] = float(detalleparte['importe'])
                                    if detalleparte['descuento'] is not None:
                                        detalleparte['descuento'] = float(detalleparte['descuento'])
                                    reg = (detalleparte['id'], detalleparte['numparte'], detalleparte['claveprodserv'],
                                           detalleparte['cantidad'], detalleparte['unidad'],
                                           detalleparte['noidentificacion'], detalleparte['descripcion'],
                                           detalleparte['valorunitario'], detalleparte['importe'],
                                           detalleparte['descuento'], detalleparte['id_taduana'])
                                    cur.execute("INSERT INTO C_Parte VALUES(?,?,?,?,?,?,?,?,?,?,?)", reg)
                                    # c.commit()
            if detalleconcepto['cantidad'] is not None:
                detalleconcepto['cantidad'] = int(detalleconcepto['cantidad'])
            if detalleconcepto['valorunitario'] is not None:
                detalleconcepto['valorunitario'] = float(detalleconcepto['valorunitario'])
            if detalleconcepto['importe'] is not None:
                detalleconcepto['importe'] = float(detalleconcepto['importe'])
            if detalleconcepto['descuento'] is not None:
                detalleconcepto['descuento'] = float(detalleconcepto['descuento'])
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
            tfd = ('UUID', 'version', 'fechatimbrado', 'rfcprovcertif', 'leyenda', 'sellocfd',
                   'nocertificadosat', 'sellosat')
            detalletfd = dict.fromkeys(tfd)  # Create data dictionary from definition
            detalletfd = {**detalletfd, **data[con]}  # Merge 2 dictionaries, keep data from second one
            if detalletfd['fechatimbrado'] is not None:
                detalletfd['fechatimbrado'] = datetime.strptime(detalletfd['fechatimbrado'], '%Y-%m-%dT%H:%M:%S')
            reg = (detalletfd['UUID'], detalletfd['version'], detalletfd['fechatimbrado'],
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
                            reg = (otcgs[det]['codigocargo'], float(otcgs[det]['codigocargo']), detallecompl['id'])
                            cur.execute("INSERT INTO Aerolineas_OtrosCargos VALUES(?,?,?)", reg)
                            # c.commit()
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            if detallecompl['tua'] is not None:
                detallecompl['tua'] = float(detallecompl['tua'])
            if detallecompl['totalotroscargos'] is not None:
                detallecompl['totalotroscargos'] = float(detallecompl['totalotroscargos'])
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['tua'],
                   detallecompl['totalotroscargos'])
            cur.execute("INSERT INTO Aerolineas VALUES(?,?,?,?,?)", reg)
            # c.commit()
        elif con == 'donatarias':
            # version | noautorizacion | fechaautorizacion | leyenda
            compl = ('UUID', 'id', 'version', 'noautorizacion', 'fechaautorizacion', 'leyenda')
            detallecompl = dict.fromkeys(compl)
            detallecompl.update(data[con])
            # detallecompl = {**detallecompl, **data[con]}
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'Donatarias')
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            if detallecompl['fechaautorizacion'] is not None:
                detallecompl['fechaautorizacion'] = datetime.strptime(detallecompl['fechaautorizacion'], '%Y-%m-%d')
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['noautorizacion'],
                   detallecompl['fechaautorizacion'], detallecompl['leyenda'])
            cur.execute("INSERT INTO Donatarias VALUES(?,?,?,?,?,?)", reg)
            # c.commit()
        elif con == 'divisas':
            # version | tipooperacion
            compl = ('UUID', 'id', 'version', 'tipooperacion')
            detallecompl = dict.fromkeys(compl)
            detallecompl.update(data[con])
            # detallecompl = {**detallecompl, **data[con]}
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'Divisas')
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['tipooperacion'])
            cur.execute("INSERT INTO Divisas VALUES(?,?,?,?)", reg)
            # c.commit()
        elif con == 'pfintegrantecoordinado':
            # version | clavevehicular | placa | rfcpf
            compl = ('UUID', 'id', 'version', 'clavevehicular', 'placa', 'rfcpf')
            detallecompl = dict.fromkeys(compl)
            detallecompl.update(data[con])
            # detallecompl = {**detallecompl, **data[con]}
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'PFintegranteCoordinado')
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['clavevehicular'],
                   detallecompl['placa'], detallecompl['rfcpf'])
            cur.execute("INSERT INTO PFintegranteCoordinado VALUES(?,?,?,?,?,?)", reg)
            # c.commit()
        elif con == 'cfdiregistrofiscal':
            # version | folio
            compl = ('UUID', 'id', 'version', 'folio')
            detallecompl = dict.fromkeys(compl)
            detallecompl.update(data[con])
            # detallecompl = {**detallecompl, **data[con]}
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'CFDIRegistroFiscal')
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['folio'])
            cur.execute("INSERT INTO CFDIRegistroFiscal VALUES(?,?,?,?)", reg)
            # c.commit()
        elif con == 'pagoenespecie':
            # version | cvepic | foliosoldon | pzaartnombre | pzaarttecn | pzaartaprod | pzaartdim
            compl = ('UUID', 'id', 'version', 'cvepic', 'foliosoldon', 'pzaartnombre', 'pzaarttecn', 'pzaartaprod',
                     'pzaartdim')
            detallecompl = dict.fromkeys(compl)
            detallecompl.update(data[con])
            # detallecompl = {**detallecompl, **data[con]}
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'PagoEnEspecie')
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
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
            # detallecompl = {**detallecompl, **data[con]}
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'obrasarteantiguedades')
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            if detallecompl['subtotal'] is not None:
                detallecompl['subtotal'] = float(detallecompl['subtotal'])
            if detallecompl['iva'] is not None:
                detallecompl['iva'] = float(detallecompl['iva'])
            if detallecompl['fechaadquisicion'] is not None:
                detallecompl['fechaadquisicion'] = datetime.strptime(detallecompl['fechaadquisicion'], '%Y-%m-%d')
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
                    # detallecompl = {**detallecompl, **ley}
                    detallecompl['UUID'] = uuid
                    detallecompl['id'] = idcomp(c, 'LeyendasFiscales')
                    detallecompl['version'] = vers
                    if detallecompl['version'] is not None:
                        detallecompl['version'] = float(detallecompl['version'])
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
                    # detallesubcompl = {**detallesubcompl, **dtstr}
                    detallesubcompl['id'] = detallecompl['id']
                    reg = (detallesubcompl['id'], detallesubcompl['via'], detallesubcompl['tipoid'],
                           detallesubcompl['numeroid'], detallesubcompl['nacionalidad'],
                           detallesubcompl['empresatransporte'], detallesubcompl['idtransporte'])
                    cur.execute("INSERT INTO datosTransito VALUES(?,?,?,?,?,?,?)", reg)
                    # c.commit()
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            if detallecompl['fechadetransito'] is not None:
                detallecompl['fechadetransito'] = datetime.strptime(detallecompl['fechadetransito'],
                                                                    '%Y-%m-%dT%H:%M:%S')
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
                        # detallesubcompl = {**detallesubcompl, **dtstr}
                        detallesubcompl['id'] = detallecompl['id']
                        if detallesubcompl['fecha'] is not None:
                            detallesubcompl['fecha'] = datetime.strptime(detallesubcompl['fecha'], '%Y-%m-%dT%H:%M:%S')
                        if detallesubcompl['importe'] is not None:
                            detallesubcompl['importe'] = float(detallesubcompl['importe'])
                        reg = (detallesubcompl['id'], detallesubcompl['identificador'], detallesubcompl['fecha'],
                               detallesubcompl['rfc'], detallesubcompl['curp'], detallesubcompl['nombre'],
                               detallesubcompl['numseguridadsocial'], detallesubcompl['importe'])
                        cur.execute("INSERT INTO Conceptos_ValesDeDespensa VALUES(?,?,?,?,?,?,?,?)", reg)
                        # c.commit()
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            if detallecompl['total'] is not None:
                detallecompl['total'] = float(detallecompl['total'])
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
                    detallecompl['cve_aduana'] = idcomp(c, 'T_Aduana', 'cve_Aduana', 7)
                    # numero | fecha | aduana
                    subcompl = ('cve_aduana', 'numero', 'fecha', 'aduana')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl.update(dtstr)
                    # detallesubcompl = {**detallesubcompl, **dtstr}
                    detallesubcompl['cve_aduana'] = detallecompl['cve_aduana']
                    if detallesubcompl['fecha'] is not None:
                        detallesubcompl['fecha'] = datetime.strptime(detallesubcompl['fecha'], '%Y-%m-%d')
                    reg = (detallesubcompl['cve_aduana'], detallesubcompl['numero'], detallesubcompl['fecha'],
                           detallesubcompl['aduana'])
                    cur.execute("INSERT INTO T_Aduana VALUES(?,?,?,?)", reg)
                    # c.commit()
        elif con == 'parcialesconstruccion':
            # version | numperlicoaut
            compl = ('UUID', 'id', 'version', 'numperlicoaut')
            detallecompl = dict.fromkeys(compl)
            detallecompl['UUID'] = uuid
            detallecompl['id'] = idcomp(c, 'parcialesconstruccion')
            for dt in data[con]:
                if dt in detallecompl:
                    detallecompl[dt] = data[con][dt]
                elif not dt.find('informacionaduanera') == -1:
                    dtstr = data[con][dt]
                    # calle | noexterior | nointerior | colonia | localidad | referencia | municipio | estado |
                    # codigopostal
                    subcompl = ('id', 'calle', 'noexterior', 'nointerior', 'colonia', 'localidad', 'referencia',
                                'municipio', 'estado', 'codigopostal')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl.update(dtstr)
                    # detallesubcompl = {**detallesubcompl, **dtstr}
                    detallesubcompl['id'] = detallecompl['id']
                    reg = (detallesubcompl['id'], detallesubcompl['calle'], detallesubcompl['noexterior'],
                           detallesubcompl['nointerior'], detallesubcompl['colonia'], detallesubcompl['localidad'],
                           detallesubcompl['referencia'], detallesubcompl['municipio'], detallesubcompl['estado'],
                           detallesubcompl['codigopostal'])
                    cur.execute("INSERT INTO Inmueble_parcialesconstruccion VALUES(?,?,?,?,?,?,?,?,?,?)", reg)
                    # c.commit()
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
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
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
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
                                    impuesto = ('id', 'tipo', 'base', 'impuesto', 'tipofactor', 'tasaocuota', 'importe')
                                    detimp = dict.fromkeys(impuesto)
                                    detimp.update(trld[dis])
                                    detimp['tipo'] = 'TRASLADO'
                                    detimp['id'] = detallesubcompl['id_impuestos']
                                if detimp['tasaocuota'] is not None:
                                    detimp['tasaocuota'] = float(detimp['tasaocuota'])
                                if detimp['importe'] is not None:
                                    detimp['importe'] = float(detimp['importe'])
                                reg = (detimp['id'], detimp['tipo'], detimp['base'], detimp['impuesto'],
                                       detimp['tipofactor'], detimp['tasaocuota'], detimp['importe'])
                                cur.execute("INSERT INTO T_Impuestos VALUES(?,?,?,?,?,?,?)", reg)
                                # c.commit()
                        if detallesubcompl['cantidad'] is not None:
                            detallesubcompl['cantidad'] = float(detallesubcompl['cantidad'])
                        if detallesubcompl['valorunitario'] is not None:
                            detallesubcompl['valorunitario'] = float(detallesubcompl['valorunitario'])
                        if detallesubcompl['importe'] is not None:
                            detallesubcompl['importe'] = float(detallesubcompl['importe'])
                        if detallesubcompl['fecha'] is not None:
                            detallesubcompl['fecha'] = datetime.strptime(detallesubcompl['fecha'], '%Y-%m-%dT%H:%M:%S')
                        reg = (detallesubcompl['id'], detallesubcompl['id_impuestos'], detallesubcompl['identificador'],
                               detallesubcompl['fecha'], detallesubcompl['rfc'], detallesubcompl['claveestacion'],
                               detallesubcompl['cantidad'], detallesubcompl['tipocombustible'],
                               detallesubcompl['unidad'], detallesubcompl['nombrecombustible'],
                               detallesubcompl['foliooperacion'], detallesubcompl['valorunitario'],
                               detallesubcompl['importe'])
                        cur.execute("INSERT INTO Concepto_EstadoDeCuentaCombustible VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    reg)
                        # c.commit()
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            if detallecompl['subtotal'] is not None:
                detallecompl['subtotal'] = float(detallecompl['subtotal'])
            if detallecompl['total'] is not None:
                detallecompl['total'] = float(detallecompl['total'])
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
                                    impuesto = ('id', 'tipo', 'base', 'impuesto', 'tipofactor', 'tasaocuota', 'importe')
                                    detimp = dict.fromkeys(impuesto)
                                    detimp.update(trld[dis])
                                    detimp['tipo'] = 'DETERMINADO'
                                    detimp['id'] = detallesubcompl['id_impuestos']
                                if detimp['tasaocuota'] is not None:
                                    detimp['tasaocuota'] = float(detimp['tasaocuota'])
                                if detimp['importe'] is not None:
                                    detimp['importe'] = float(detimp['importe'])
                                reg = (detimp['id'], detimp['tipo'], detimp['base'], detimp['impuesto'],
                                       detimp['tipofactor'], detimp['tasaocuota'], detimp['importe'])
                                cur.execute("INSERT INTO T_Impuestos VALUES(?,?,?,?,?,?,?)", reg)
                                # c.commit()
                        if detallesubcompl['cantidad'] is not None:
                            detallesubcompl['cantidad'] = float(detallesubcompl['cantidad'])
                        if detallesubcompl['valorunitario'] is not None:
                            detallesubcompl['valorunitario'] = float(detallesubcompl['valorunitario'])
                        if detallesubcompl['importe'] is not None:
                            detallesubcompl['importe'] = float(detallesubcompl['importe'])
                        if detallesubcompl['fecha'] is not None:
                            detallesubcompl['fecha'] = datetime.strptime(detallesubcompl['fecha'], '%Y-%m-%dT%H:%M:%S')
                        reg = (detallesubcompl['id'], detallesubcompl['id_impuestos'], detallesubcompl['identificador'],
                               detallesubcompl['fecha'], detallesubcompl['rfc'], detallesubcompl['claveestacion'],
                               detallesubcompl['tipocombustible'], detallesubcompl['cantidad'],
                               detallesubcompl['nombrecombustible'], detallesubcompl['foliooperacion'],
                               detallesubcompl['valorunitario'], detallesubcompl['importe'])
                        cur.execute("INSERT INTO Concepto_ConsumoDeCombustibles VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", reg)
                        # c.commit()
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            if detallecompl['subtotal'] is not None:
                detallecompl['subtotal'] = float(detallecompl['subtotal'])
            if detallecompl['total'] is not None:
                detallecompl['total'] = float(detallecompl['total'])
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
                    if detallesubcompl['año'] is not None:
                        detallesubcompl['año'] = int(detallesubcompl['año'])
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
                    subcompl = ('cve_aduana', 'numpedimp', 'fecha', 'aduana')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl.update(dtstr)
                    detallesubcompl['cve_aduana'] = detallecompl['cve_aduana']
                    if detallesubcompl['fecha'] is not None:
                        detallesubcompl['fecha'] = datetime.strptime(detallesubcompl['fecha'], '%Y-%m-%d')
                    reg = (detallesubcompl['cve_aduana'], detallesubcompl['numpedimp'], detallesubcompl['fecha'],
                           detallesubcompl['aduana'])
                    cur.execute("INSERT INTO T_Aduana VALUES(?,?,?,?)", reg)
                    # c.commit()
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'], detallecompl['serie'],
                   detallecompl['numfoldesveh'], detallecompl['cve_aduana'])
            cur.execute("INSERT INTO ConsumoDeCombustibles VALUES(?,?,?,?,?,?)", reg)
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
                        if detallesubcompl['iva'] is not None:
                            detallesubcompl['iva'] = float(detallesubcompl['iva'])
                        if detallesubcompl['montopago'] is not None:
                            detallesubcompl['montopago'] = float(detallesubcompl['montopago'])
                        reg = (detallesubcompl['id'], detallesubcompl['tipo'], detallesubcompl['bancoemisor'],
                               detallesubcompl['nombre'], detallesubcompl['tipocuenta'], detallesubcompl['cuenta'],
                               detallesubcompl['rfc'], detallesubcompl['concepto'], detallesubcompl['iva'],
                               detallesubcompl['montopago'])
                        cur.execute("INSERT INTO DatosBancarios VALUES(?,?,?,?,?,?,?,?,?,?)", reg)
                        # c.commit()
                if detallecompl['version'] is not None:
                    detallecompl['version'] = float(detallecompl['version'])
                if detallecompl['fechaoperacion'] is not None:
                    detallecompl['fechaoperacion'] = datetime.strptime(detallecompl['fechaoperacion'], '%Y-%m-%d')
                if detallecompl['hora'] is not None:
                    detallecompl['hora'] = datetime.strptime(detallecompl['hora'], '%H:%M:%S.%f')
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
            for dt in spter:
                if dt in detallecompl:
                    detallecompl[dt] = data[con][dt]
                elif not dt.find('retenciones') == -1:
                    # implocretenido | tasaderetencion | importe
                    dtstr = data[con][dt]
                    subcompl = ('id', 'tipo', 'base', 'implocretenido', 'tipofactor', 'tasaderetencion', 'importe')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl.update(dtstr)
                    detallesubcompl['tipo'] = 'RETENCION'
                    detallesubcompl['id'] = detallecompl['id_impuestos']
                    if detallesubcompl['tasaderetencion'] is not None:
                        detallesubcompl['tasaderetencion'] = float(detallesubcompl['tasaderetencion'])
                    if detallesubcompl['importe'] is not None:
                        detallesubcompl['importe'] = float(detallesubcompl['importe'])
                    reg = (detallesubcompl['id'], detallesubcompl['tipo'], detallesubcompl['base'],
                           detallesubcompl['implocretenido'], detallesubcompl['tipofactor'],
                           detallesubcompl['tasaderetencion'], detallesubcompl['importe'])
                    cur.execute("INSERT INTO T_Impuestos VALUES(?,?,?,?,?,?,?)", reg)
                    # c.commit()
                elif not dt.find('traslados') == -1:
                    # imploctrasladado | tasadetraslado | importe
                    dtstr = data[con][dt]
                    subcompl = ('id', 'tipo', 'base', 'imploctrasladado', 'tipofactor', 'tasadetraslado', 'importe')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl.update(dtstr)
                    detallesubcompl['tipo'] = 'RETENCION'
                    detallesubcompl['id'] = detallecompl['id_impuestos']
                    if detallesubcompl['tasaderetencion'] is not None:
                        detallesubcompl['tasaderetencion'] = float(detallesubcompl['tasaderetencion'])
                    if detallesubcompl['importe'] is not None:
                        detallesubcompl['importe'] = float(detallesubcompl['importe'])
                    reg = (detallesubcompl['id'], detallesubcompl['tipo'], detallesubcompl['base'],
                           detallesubcompl['imploctrasladado'], detallesubcompl['tipofactor'],
                           detallesubcompl['tasadetraslado'], detallesubcompl['importe'])
                    cur.execute("INSERT INTO T_Impuestos VALUES(?,?,?,?,?,?,?)", reg)
                    # c.commit()
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            if detallecompl['totalderetenciones'] is not None:
                detallecompl['totalderetenciones'] = float(detallecompl['totalderetenciones'])
            if detallecompl['totaldetraslados'] is not None:
                detallecompl['totaldetraslados'] = float(detallecompl['totaldetraslados'])
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'],
                   detallecompl['totalderetenciones'], detallecompl['totaldetraslados'], detallecompl['id_impuestos'])
            cur.execute("INSERT INTO Complemento_SPEI VALUES(?,?,?,?,?,?,?,?,?)", reg)
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
                            if detallesubsubcompl['año'] is not None:
                                detallesubsubcompl['año'] = int(detallesubsubcompl['año'])
                            if detallesubsubcompl['fecharegulveh'] is not None:
                                detallesubsubcompl['fecharegulveh'] = datetime.strptime(data['fecharegulveh'],
                                                                                        '%Y-%m-%d')
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
                            if detallesubsubcompl['año'] is not None:
                                detallesubsubcompl['año'] = int(detallesubsubcompl['año'])
                            reg = (detallesubsubcompl['id'], detallesubsubcompl['año'],
                                   detallesubsubcompl['modelo'], detallesubsubcompl['numplacas'],
                                   detallesubsubcompl['rfc'])
                            cur.execute("INSERT INTO VehiculoNuvoSemEnajenadoFabAlPerm " +
                                        "VALUES(?,?,?,?,?)", reg)
                            # c.commit()
                    reg = (detallesubcompl['id_srv'], detallesubcompl['id'],
                           detallesubcompl['tipo'], detallesubcompl['vehenaj'])
                    cur.execute("INSERT INTO DecretoRenovSustitVehicular VALUES(?,?,?,?)", reg)
                    # c.commit()
                elif not dt.find('decretosustitvehicular') == -1:
                    # vehenaj
                    dtstr = data[con][dt]
                    subcompl = ('id_srv', 'id', 'tipo', 'vehenaj')
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
                            if detallesubsubcompl['año'] is not None:
                                detallesubsubcompl['año'] = int(detallesubsubcompl['año'])
                            if detallesubsubcompl['fecharegulveh'] is not None:
                                detallesubsubcompl['fecharegulveh'] = datetime.strptime(data['fecharegulveh'],
                                                                                        '%Y-%m-%d')
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
                            if detallesubsubcompl['año'] is not None:
                                detallesubsubcompl['año'] = int(detallesubsubcompl['año'])
                            cur.execute("INSERT INTO VehiculoNuvoSemEnajenadoFabAlPerm " +
                                        "VALUES(?,?,?,?,?)", reg)
                            # c.commit()
                    reg = (detallesubcompl['id_srv'], detallesubcompl['id'],
                           detallesubcompl['tipo'], detallesubcompl['vehenaj'])
                    cur.execute("INSERT INTO DecretoRenovSustitVehicular VALUES(?,?,?,?)", reg)
                    # c.commit()
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
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
                    if detallesubcompl['fechainstnotarial'] is not None:
                        detallesubcompl['fechainstnotarial'] = datetime.strptime(detallesubcompl['fechainstnotarial'],
                                                                                 '%Y-%m-%d')
                    if detallesubcompl['montooperacion'] is not None:
                        detallesubcompl['montooperacion'] = float(detallesubcompl['montooperacion'])
                    if detallesubcompl['subtotal'] is not None:
                        detallesubcompl['subtotal'] = float(detallesubcompl['subtotal'])
                    if detallesubcompl['iva'] is not None:
                        detallesubcompl['iva'] = float(detallesubcompl['iva'])
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
                    cur.execute("INSERT INTO DatosOperacion VALUES(?,?,?,?,?)", reg)
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
                            if detallesubcompl['porcentaje'] is not None:
                                detallesubcompl['porcentaje'] = float(detallesubcompl['porcentaje'])
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
                                if detallesubcompl['porcentaje'] is not None:
                                    detallesubcompl['porcentaje'] = float(detallesubcompl['porcentaje'])
                                reg = (detallesubcompl['id'], detallesubcompl['tipo'],
                                       detallesubcompl['coprosocconyugale'], detallesubcompl['nombre'],
                                       detallesubcompl['apellidopaterno'], detallesubcompl['apellidomaterno'],
                                       detallesubcompl['rfc'], detallesubcompl['curp'], detallesubcompl['porcentaje'])
                                cur.execute("INSERT INTO DatosEnajenanteAdquiriente VALUES(?,?,?,?,?,?,?,?,?)", reg)
                                # c.commit()
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            reg = (detallecompl['UUID'], detallecompl['id'], detallecompl['version'])
            cur.execute("INSERT INTO NotariosPublicos VALUES(?,?,?)", reg)
            # c.commit()
    cur.close()
