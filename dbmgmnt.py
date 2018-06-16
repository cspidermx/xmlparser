import sqlite3
from datetime import datetime
import random
import string


def dbopen(db):
    try:
        con = sqlite3.connect(db)
    except:
        return None
    return con


def dbclose(con):
    try:
        con.close
    except:
        return False
    return True

def dbinsertCFDI(c, data):
    # Version(3.3) | *Serie | *Folio | Fecha(AAAA-MM-DDThh:mm:ss) | Sello | *FormaPago(c_FormaPago) |
    # NoCertificado | Certificado | *CondicionesDePago(Texto Libre) | SubTotal | *Descuento |
    # Moneda(c_Moneda) | *TipoCambio | Total | TipoDeComprobante(c_TipoDeComprobante) |
    # *MetodoPago | LugarExpedicion(Codigo Postal) | *Confirmacion
    cur = c.cursor()
    sql = 'Select UUID from CFDI where UUID="' + data['UUID'] + '"'
    if len(cur.execute(sql).fetchall()) == 0:
        cur.close()
        cur = c.cursor()
        data['version'] = float(data['version'])
        data['fecha'] = datetime.strptime(data['fecha'], '%Y-%m-%dT%H:%M:%S')
        data['subtotal'] = float(data['subtotal'])
        if data['descuento'] is not None:
            data['descuento'] = float(data['descuento'])
        if data['tipocambio'] is not None:
            data['tipocambio'] = float(data['tipocambio'])
        data['total'] = float(data['total'])
        reg = (data['UUID'], data['version'], data['serie'], data['folio'], data['fecha'], data['sello'], data['formapago'],
                data['nocertificado'], data['certificado'], data['condicionesdepago'], data['subtotal'], data['descuento'],
                data['moneda'], data['tipocambio'], data['total'], data['tipodecomprobante'], data['metodopago'],
                data['lugarexpedicion'], data['confirmacion'], 'verde', 'x:\\cliente\\CFDIs')
        # valida - indicara si está valida en el SAT
        # path - link a donde está guardado el archivo
        cur.execute("INSERT INTO CFDI VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", reg)
        c.commit()
        cur.close()


def dbinsertCFDIrels(c, data):
    # TipoRelacion(c_TipoRelacion)
    # UUID  --  Es posible que sean varios data los trae como UUID#
    cur = c.cursor()
    i = 1
    while 'uuid' + str(i) in data:
        reg = (data['UUID'], data['tiporelacion'], data['uuid' + str(i)])
        cur.execute("INSERT INTO CFDIrelacionados VALUES(?,?,?)", reg)
        c.commit()
        i += 1
    cur.close()


def dbinsertemisor(c, data):
    # RFC | *Nombre | RegimenFiscal(c_RegimenFiscal)
    cur = c.cursor()
    reg = (data['UUID'], data['rfc'], data['nombre'], data['regimenfiscal'])
    cur.execute("INSERT INTO Emisor VALUES(?,?,?,?)", reg)
    c.commit()
    cur.close()


def dbinsertreceptor(c, data):
    # RFC | *Nombre | *ResidenciaFiscal | *NumRegIdTrib | UsoCFDI(c_UsoCFDI)
    cur = c.cursor()
    reg = (data['UUID'], data['rfc'], data['nombre'], data['residenciafiscal'], data['numregidtrib'], data['usocfdi'])
    cur.execute("INSERT INTO Receptor VALUES(?,?,?,?,?,?)", reg)
    c.commit()
    cur.close()


def dbinsertimpuestos(c, data):
    # *TotalImpuestosRetenidos | *TotalImpuestosTrasladados
    cur = c.cursor()
    id_Timpuestos = idtimpuestos(c)  # Identificador para insertar el detalle
    if data['totalimpuestosretenidos'] is not None:
        data['totalimpuestosretenidos'] = float(data['totalimpuestosretenidos'])
    if data['totalimpuestostrasladados'] is not None:
        data['totalimpuestostrasladados'] = float(data['totalimpuestostrasladados'])
    reg = (data['UUID'], data['totalimpuestosretenidos'], data['totalimpuestostrasladados'], id_Timpuestos)
    cur.execute("INSERT INTO Impuestos VALUES(?,?,?,?)", reg)
    c.commit()
    cur.close()
    detalle = ('id', 'tipo', 'base', 'impuesto', 'tipofactor', 'tasaocuota', 'importe')
    if data['retenciones'] is not None:
        # Impuesto(c_Impuesto) | Importe
        rets = data['retenciones']
        for r in rets:
            cur = c.cursor()
            retencion = dict.fromkeys(detalle)  # Create data dictionary from definition
            retencion = {**retencion, **rets[r]}  # Merge 2 dictionaries, keep data from second one
            retencion['id'] = id_Timpuestos
            retencion['tipo'] = 'RETENCION'
            retencion['importe'] = float(retencion['importe'])
            reg = (retencion['id'], retencion['tipo'], retencion['base'], retencion['impuesto'],
                   retencion['tipofactor'], retencion['tasaocuota'], retencion['importe'])
            cur.execute("INSERT INTO T_Impuestos VALUES(?,?,?,?,?,?,?)", reg)
            c.commit()
            cur.close()
            retencion = None
    if data['traslados'] is not None:
        # Impuesto(c_Impuesto) | TipoFactor | TasaOCuota(c_TasaOCuota) | Importe
        tras = data['traslados']
        for r in tras:
            cur = c.cursor()
            traslado = dict.fromkeys(detalle)  # Create data dictionary from definition
            traslado = {**traslado, **tras[r]}  # Merge 2 dictionaries, keep data from second one
            traslado['id'] = id_Timpuestos
            traslado['tipo'] = 'TRASLADO'
            traslado['tasaocuota'] = float(traslado['tasaocuota'])
            traslado['importe'] = float(traslado['importe'])
            reg = (traslado['id'], traslado['tipo'], traslado['base'], traslado['impuesto'],
                   traslado['tipofactor'], traslado['tasaocuota'], traslado['importe'])
            cur.execute("INSERT INTO T_Impuestos VALUES(?,?,?,?,?,?,?)", reg)
            c.commit()
            cur.close()
            traslado = None


def numconcepto (c):
    cur = c.cursor()
    numcpto = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(15))
    sql = 'Select numcpto from Conceptos where numcpto="' + numcpto + '"'
    while not len(cur.execute(sql).fetchall()) == 0:
        cur.close
        numcpto = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(15))
        sql = 'Select numcpto from Conceptos where numcpto="' + numcpto + '"'
        cur = c.cursor()
    return numcpto


def idtimpuestos (c):
    cur = c.cursor()
    id_Timpuestos = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(10))
    sql = 'Select id from T_Impuestos where id="' + id_Timpuestos + '"'
    while not len(cur.execute(sql).fetchall()) == 0:
        cur.close
        id_Timpuestos = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(10))
        sql = 'Select id from T_Impuestos where id="' + id_Timpuestos + '"'
        cur = c.cursor()
    return id_Timpuestos


def idaduana (c):
    cur = c.cursor()
    id_aduana = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(7))
    sql = 'Select id from T_Aduana where cve_Aduana="' + id_aduana + '"'
    while not len(cur.execute(sql).fetchall()) == 0:
        cur.close
        id_aduana = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(7))
        sql = 'Select id from T_Aduana where cve_Aduana="' + id_aduana + '"'
        cur = c.cursor()
    return id_aduana


def idpredial (c):
    cur = c.cursor()
    id_predial = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(5))
    sql = 'Select id from T_Predial where cve_Predial="' + id_predial + '"'
    while not len(cur.execute(sql).fetchall()) == 0:
        cur.close
        id_predial = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(5))
        sql = 'Select id from T_Predial where cve_Predial="' + id_predial + '"'
        cur = c.cursor()
    return id_predial


def idpartes (c):
    cur = c.cursor()
    id_partes = ''.join(random.SystemRandom().choice(string.ascii_lowercase + string.digits) for _ in range(5))
    sql = 'Select id from C_Parte where cve_Predial="' + id_partes + '"'
    while not len(cur.execute(sql).fetchall()) == 0:
        cur.close
        id_partes = ''.join(random.SystemRandom().choice(string.ascii_lowercase + string.digits) for _ in range(5))
        sql = 'Select id from C_Parte where cve_Predial="' + id_partes + '"'
        cur = c.cursor()
    return id_partes


def idift (c):
    cur = c.cursor()
    id_ift = ''.join(random.SystemRandom().choice(string.ascii_lowercase + string.digits) for _ in range(5))
    sql = 'Select id from cC_PCdT_InformacionFiscalTercero where id="' + id_ift + '"'
    while not len(cur.execute(sql).fetchall()) == 0:
        cur.close
        id_ift = ''.join(random.SystemRandom().choice(string.ascii_lowercase + string.digits) for _ in range(5))
        sql = 'Select id from cC_PCdT_InformacionFiscalTercero where id="' + id_ift + '"'
        cur = c.cursor()
    return id_ift


def idcomp(c, tbla):
    cur = c.cursor()
    id_comp = ''.join(
        random.SystemRandom().choice(string.ascii_lowercase + string.digits + string.ascii_uppercase) for _ in range(5))
    sql = 'Select id from ' + tbla + ' where id="' + id_comp + '"'
    while not len(cur.execute(sql).fetchall()) == 0:
        cur.close
        id_comp = ''.join(
            random.SystemRandom().choice(string.ascii_lowercase + string.digits + string.ascii_uppercase) for _ in
            range(5))
        sql = 'Select id from ' + tbla + ' where id="' + id_comp + '"'
        cur = c.cursor()
    return id_comp


def dbinsertconceptos (c, data):
    # ClaveProdServ(c_ClaveProdServ) | *NoIdentificacion | Cantidad | ClaveUnidad(c_ClaveUnidad)
    # *Unidad | Descripcion | ValorUnitario | Importe | *Descuento |
    cur = c.cursor()
    concepto = ('UUID', 'numcpto', 'claveprodserv', 'noidentificacion', 'cantidad', 'claveunidad', 'unidad', 'descripcion',
                'valorunitario', 'importe', 'descuento', 'id_aduana', 'id_predial', 'id_impuestos', 'cvePartes')
    for con in data:
        if not con.find('concepto') == -1:
            detalleconcepto = dict.fromkeys(concepto)
            conc = data[con]
            detalleconcepto['UUID'] = data['UUID']
            detalleconcepto['numcpto'] = numconcepto(c)
            for dt in conc:
                if str(type(conc[dt])) == "<class 'str'>":
                    detalleconcepto[dt] = conc[dt]
                else:
                    # impuestos  |  informacionaduanera  |  cuentapredial  |  complementoconcepto  |  parte#
                    addon = dt
                    addondict = conc[dt]
                    if addon == 'impuestos':
                        detalleconcepto['id_impuestos'] = idtimpuestos(c)  # Identificador para insertar el detalle
                        detimp = ('id', 'tipo', 'base', 'impuesto', 'tipofactor', 'tasaocuota', 'importe')
                        for imp in addondict:
                            detalleimp = dict.fromkeys(detimp)
                            detalleimp['id'] = detalleconcepto['id_impuestos']
                            if not str(imp).find("traslado") == -1:
                                detalleimp['tipo'] = 'TRASLADO'
                            else:
                                detalleimp['tipo'] = 'RETENCION'
                            detalleimp = {**detalleimp, **addondict[imp]}
                            if detalleimp['base'] is not None:
                                detalleimp['base'] = float(detalleimp['base'])
                            if detalleimp['tasaocuota'] is not None:
                                detalleimp['tasaocuota'] = float(detalleimp['tasaocuota'])
                            if detalleimp['importe'] is not None:
                                detalleimp['importe'] = float(detalleimp['importe'])
                            reg = (detalleimp['id'], detalleimp['tipo'], detalleimp['base'], detalleimp['impuesto'],
                                   detalleimp['tipofactor'], detalleimp['tasaocuota'], detalleimp['importe'])
                            cur.execute("INSERT INTO T_Impuestos VALUES(?,?,?,?,?,?,?)", reg)
                            c.commit()
                            cur.close()
                            cur = c.cursor()
                    else:
                        if addon == 'informacionaduanera':
                            detalleconcepto['id_aduana'] = idaduana(c)  # Identificador para insertar el detalle
                            detadu = ('cve_aduana', 'numero', 'fecha', 'aduana')
                            detalleadu = dict.fromkeys(detadu)
                            detalleadu['cve_aduana'] = detalleconcepto['id_aduana']
                            # NumeroPedimento
                            detalleadu['numero'] = addondict['numeropedimento']
                            reg = (detalleadu['cve_aduana'], detalleadu['numero'], detalleadu['fecha'],
                                   detalleadu['aduana'])
                            cur.execute("INSERT INTO T_Aduana VALUES(?,?,?,?)", reg)
                            c.commit()
                            cur.close()
                            cur = c.cursor()
                        else:
                            if addon == 'cuentapredial':
                                detalleconcepto['id_predial'] = idpredial(c)  # Identificador para insertar el detalle
                                detpred = ('cve_predial', 'numero')
                                detallepred = dict.fromkeys(detpred)
                                detallepred['cve_predial'] = detalleconcepto['id_predial']
                                # Numero
                                detallepred['numero'] = addondict['numero']
                                reg = (detallepred['cve_predial'], detallepred['numero'])
                                cur.execute("INSERT INTO T_Predial VALUES(?,?)", reg)
                                c.commit()
                                cur.close()
                                cur = c.cursor()
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
                                        c.commit()
                                        cur.close()
                                        cur = c.cursor()
                                    elif addondict['schema'] == 'acreditamientoieps':
                                        # version | TAR
                                        detcomp = ('id', 'version', 'tar')
                                        detallecomp = dict.fromkeys(detcomp)
                                        detallecomp['id'] = detalleconcepto['numcpto']
                                        detallecomp = {**detallecomp, **addondict}
                                        detallecomp.pop('schema', None)
                                        reg = (detallecomp['id'], detallecomp['version'], detallecomp['tar'])
                                        cur.execute("INSERT INTO compC_acreditamientoIEPS VALUES(?,?,?)", reg)
                                        c.commit()
                                        cur.close()
                                        cur = c.cursor()
                                    elif addondict['schema'] == 'ventavehiculos':
                                        # version | clavevehicular | NIV
                                        detcomp = ('id', 'version', 'clavevehicular', 'niv', 'cve_aduana', 'cve_partes')
                                        detallecomp = dict.fromkeys(detcomp)
                                        detallecomp['id'] = detalleconcepto['numcpto']
                                        for dt in addondict:
                                            if str(type(addondict[dt])) == "<class 'str'>":
                                                detallecomp[dt] = addondict[dt]
                                            else:
                                                if not dt.find('aduanera') == -1:
                                                    detallecomp['cve_aduana'] = idaduana(c)
                                                    # numero | fecha | aduana
                                                    detsubcomp = ('cve_aduana', 'numero', 'fecha', 'aduana')
                                                    detallesubcomp = dict.fromkeys(detsubcomp)
                                                    detallesubcomp['cve_aduana'] = detallecomp['cve_aduana']
                                                    detallesubcomp = {**detallesubcomp, **addondict[dt]}
                                                    if detallesubcomp['fecha'] is not None:
                                                        # "YYYY-MM-DD"
                                                        detallesubcomp['fecha'] = datetime.strptime(
                                                            detallesubcomp['fecha'], '%Y-%m-%d')
                                                    reg = (detallesubcomp['cve_aduana'], detallesubcomp['numero'],
                                                           detallesubcompu['fecha'], detallesubcomp['aduana'])
                                                    cur.execute("INSERT INTO T_Aduana VALUES(?,?,?,?)", reg)
                                                    c.commit()
                                                    cur.close()
                                                    cur = c.cursor()
                                                elif not dt.find('parte') == -1:
                                                    detallecomp['cve_partes'] = idpartes(c)
                                                    detparte = ('id', 'numparte', 'claveprodserv', 'cantidad', 'unidad',
                                                                'noidentificacion', 'descripcion', 'valorunitario',
                                                                'importe',
                                                                'descuento', 'id_taduana')
                                                    detalleparte = dict.fromkeys(detparte)
                                                    detalleparte['id'] = detallecomp['cve_partes']
                                                    i = int(dt.replace('parte', ''))
                                                    detalleparte['numparte'] = '{0:03d}'.format(i)
                                                    for det in addondict[dt]:
                                                        if str(type(addondict[dt][det])) == "<class 'str'>":
                                                            detalleparte[det] = addondict[dt][det]
                                                        else:
                                                            # informacionaduanera: NumeroPedimento
                                                            detalleparte['id_taduana'] = idaduana(c)
                                                            detadu = ('cve_aduana', 'numero', 'fecha', 'aduana')
                                                            detalleadu = dict.fromkeys(detadu)
                                                            detalleadu['cve_aduana'] = detalleparte['id_taduana']
                                                            # numero | fecha | aduana
                                                            detalleadu = {**detalleadu,
                                                                          **addondict[dt]['informacionaduanera']}
                                                            if detalleadu['fecha'] is not None:
                                                                # "YYYY-MM-DD"
                                                                detalleadu['fecha'] = datetime.strptime(
                                                                    detalleadu['fecha'], '%Y-%m-%d')
                                                            reg = (detalleadu['cve_aduana'], detalleadu['numero'],
                                                                   detalleadu['fecha'],
                                                                   detalleadu['aduana'])
                                                            cur.execute("INSERT INTO T_Aduana VALUES(?,?,?,?)", reg)
                                                            c.commit()
                                                            cur.close()
                                                            cur = c.cursor()
                                                    if detalleparte['cantidad'] is not None:
                                                        detalleparte['cantidad'] = int(detalleparte['cantidad'])
                                                    if detalleparte['valorunitario'] is not None:
                                                        detalleparte['valorunitario'] = float(
                                                            detalleparte['valorunitario'])
                                                    if detalleparte['importe'] is not None:
                                                        detalleparte['importe'] = float(detalleparte['importe'])
                                                    if detalleparte['descuento'] is not None:
                                                        detalleparte['descuento'] = floa(detalleparte['descuento'])
                                                    reg = (detalleparte['id'], detalleparte['numparte'],
                                                           detalleparte['claveprodserv'],
                                                           detalleparte['cantidad'], detalleparte['unidad'],
                                                           detalleparte['noidentificacion'],
                                                           detalleparte['descripcion'],
                                                           detalleparte['valorunitario'], detalleparte['importe'],
                                                           detalleparte['descuento'], detalleparte['id_taduana'])
                                                    cur.execute("INSERT INTO C_Parte VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                                                                reg)
                                                    c.commit()
                                                    cur.close()
                                                    cur = c.cursor()
                                        detallecomp.pop('schema', None)
                                        # detcomp = ('id', 'version', 'clavevehicular', 'niv', 'cve_aduana', 'cve_partes')'
                                        reg = (detallecomp['id'], detallecomp['version'], detallecomp['clavevehicular'],
                                               detallecomp['niv'], detallecomp['cve_aduana'], detallecomp['cve_partes'])
                                        cur.execute("INSERT INTO compC_VentaVehiculos VALUES(?,?,?,?,?,?)", reg)
                                        c.commit()
                                        cur.close()
                                        cur = c.cursor()
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
                                        for dt in addondict:
                                            if not str(dt).find('parte') == -1 :
                                                pte += 1
                                                if detalletercero['cve_partes'] is None:
                                                    detalletercero['cve_partes'] = idpartes(c)
                                                detparte = ('id', 'numparte', 'claveprodserv', 'cantidad', 'unidad',
                                                            'noidentificacion', 'descripcion', 'valorunitario',
                                                            'importe', 'descuento', 'id_taduana')
                                                detalleparte = dict.fromkeys(detparte)
                                                detalleparte['id'] = detalletercero['cve_partes']
                                                detalleparte['numparte'] = '{0:03d}'.format(pte)
                                                for d in addondict[dt]:
                                                    if str(type(addondict[dt][d])) == "<class 'str'>":
                                                        detalleparte[d] = addondict[dt][d]
                                                    else:
                                                        # informacionaduanera: numero | fecha | aduana
                                                        detalleparte['id_taduana'] = idaduana(c)
                                                        detadu = ('cve_aduana', 'numero', 'fecha', 'aduana')
                                                        detalleadu = dict.fromkeys(detadu)
                                                        detalleadu['cve_aduana'] = detalleparte['id_taduana']
                                                        detalleadu = {**detalleadu, **addondict[dt][d]}
                                                        if detalleadu['fecha'] is not None:
                                                            # "YYYY-MM-DD"
                                                            detalleadu['fecha'] = datetime.strptime(detalleadu['fecha'],
                                                                                                    '%Y-%m-%d')
                                                        reg = (detalleadu['cve_aduana'], detalleadu['numero'],
                                                               detalleadu['fecha'],
                                                               detalleadu['aduana'])
                                                        cur.execute("INSERT INTO T_Aduana VALUES(?,?,?,?)", reg)
                                                        c.commit()
                                                        cur.close()
                                                        cur = c.cursor()
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
                                                c.commit()
                                                cur.close()
                                                cur = c.cursor()
                                            elif not str(dt).find('impuestos') == -1 :
                                                if detalletercero['cve_impuestos'] is None:
                                                    detalletercero['cve_impuestos'] = idtimpuestos(c)
                                                detimp = ('id', 'tipo', 'base', 'impuesto', 'tipofactor', 'tasaocuota',
                                                          'importe')
                                                for lvl in addondict[dt]:
                                                    for imp in addondict[dt][lvl]:
                                                        detalleimp = dict.fromkeys(detimp)
                                                        detalleimp['id'] = detalletercero['cve_impuestos']
                                                        if not str(imp).find("traslado") == -1:
                                                            detalleimp['tipo'] = 'TRASLADO'
                                                        else:
                                                            detalleimp['tipo'] = 'RETENCION'
                                                        detalleimp = {**detalleimp, **addondict[dt][lvl][imp]}
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
                                                        c.commit()
                                                        cur.close()
                                                        cur = c.cursor()
                                            elif not str(dt).find('fiscal') == -1 :
                                                detalletercero['id_ift'] = idift(c)
                                                detIFT = ('id', 'rfc', 'calle', 'noexterior', 'nointerior', 'colonia',
                                                          'localidad', 'referencia', 'municipio', 'estado', 'pais',
                                                          'codigopostal')
                                                detalleIFT = dict.fromkeys(detIFT)
                                                detalleIFT['id'] = detalletercero['id_ift']
                                                detalleIFT = {**detalleIFT, **addondict[dt]}
                                                detalleIFT['rfc'] = addondict['rfc']
                                                reg = (detalleIFT['id'], detalleIFT['rfc'], detalleIFT['calle'],
                                                       detalleIFT['noexterior'], detalleIFT['nointerior'],
                                                       detalleIFT['colonia'], detalleIFT['localidad'],
                                                       detalleIFT['referencia'], detalleIFT['municipio'],
                                                       detalleIFT['estado'], detalleIFT['pais'],
                                                       detalleIFT['codigopostal'])
                                                cur.execute(
                                                    "INSERT INTO cC_PCdT_InformacionFiscalTercero VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                                                    reg)
                                                c.commit()
                                                cur.close()
                                                cur = c.cursor()
                                            elif not str(dt).find('aduanera') == -1:
                                                detalletercero['cve_aduana'] = idaduana(c)
                                                detadu = ('cve_aduana', 'numero', 'fecha', 'aduana')
                                                detalleadu = dict.fromkeys(detadu)
                                                detalleadu['cve_aduana'] = detalletercero['cve_aduana']
                                                detalleadu = {**detalleadu, **addondict[dt]}
                                                if detalleadu['fecha'] is not None:
                                                    # "YYYY-MM-DD"
                                                    detalleadu['fecha'] = datetime.strptime(detalleadu['fecha'],
                                                                                            '%Y-%m-%d')
                                                reg = (detalleadu['cve_aduana'], detalleadu['numero'],
                                                       detalleadu['fecha'],
                                                       detalleadu['aduana'])
                                                cur.execute("INSERT INTO T_Aduana VALUES(?,?,?,?)", reg)
                                                c.commit()
                                                cur.close()
                                                cur = c.cursor()
                                            elif not str(dt).find('predial') == -1:
                                                detalletercero['cve_predial'] = idpredial(c)
                                                detpred = ('cve_predial', 'numero')
                                                detallepred = dict.fromkeys(detpred)
                                                detallepred['cve_predial'] = detalletercero['cve_predial']
                                                detallepred['numero'] = addondict[dt]['numero']
                                                reg = (detallepred['cve_predial'], detallepred['numero'])
                                                cur.execute("INSERT INTO T_Predial VALUES(?,?)", reg)
                                                c.commit()
                                                cur.close()
                                                cur = c.cursor()
                                        reg = (detalletercero['id'], detalletercero['cve_partes'],
                                               detalletercero['cve_impuestos'], detalletercero['version'],
                                               detalletercero['id_ift'], detalletercero['nombre'],
                                               detalletercero['cve_aduana'], detalletercero['cve_predial'])
                                        cur.execute("INSERT INTO compC_PorCuentadeTerceros VALUES(?,?,?,?,?,?,?,?)",
                                                    reg)
                                        c.commit()
                                        cur.close()
                                        cur = c.cursor()
                                else:
                                    # el addon es 'parte#'
                                    detalleconcepto['cvePartes'] = idpartes(c)  # Identificador para insertar el detalle
                                    detparte = ('id', 'numparte', 'claveprodserv', 'cantidad', 'unidad',
                                                'noidentificacion', 'descripcion', 'valorunitario', 'importe',
                                                'descuento', 'id_taduana')
                                    detalleparte = dict.fromkeys(detparte)
                                    detalleparte['id'] = detalleconcepto['cvePartes']
                                    i = int(addon.replace('parte', ''))
                                    detalleparte['numparte'] = '{0:03d}'.format(i)
                                    for dt in addondict:
                                        if str(type(addondict[dt])) == "<class 'str'>":
                                            detalleparte[dt] = addondict[dt]
                                        else:
                                            #informacionaduanera: NumeroPedimento
                                            detalleparte['id_taduana'] = idaduana(c)
                                            detadu = ('cve_aduana', 'numero', 'fecha', 'aduana')
                                            detalleadu = dict.fromkeys(detadu)
                                            detalleadu['cve_aduana'] = detalleparte['id_taduana']
                                            # NumeroPedimento
                                            detalleadu['numero'] = addondict['informacionaduanera']['numeropedimento']
                                            reg = (detalleadu['cve_aduana'], detalleadu['numero'], detalleadu['fecha'],
                                                   detalleadu['aduana'])
                                            cur.execute("INSERT INTO T_Aduana VALUES(?,?,?,?)", reg)
                                            c.commit()
                                            cur.close()
                                            cur = c.cursor()
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
                                    c.commit()
                                    cur.close()
                                    cur = c.cursor()
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
            c.commit()
            cur.close()
            cur = c.cursor()


def dbinsertcomplementos (c, data):
    UUID = data['UUID']
    cur = c.cursor()
    for con in data:
        if con == 'timbrefiscaldigital':
            # Version | UUID | FechaTimbrado | RfcProvCertif | *Leyenda | SelloCFD |
            # NoCertificadoSAT | SelloSAT
            tfd = ('uuid', 'version', 'fechatimbrado', 'rfcprovcertif', 'leyenda', 'sellocfd',
                   'nocertificadosat', 'sellosat')
            detalletfd = dict.fromkeys(tfd)  # Create data dictionary from definition
            detalletfd = {**detalletfd, **data[con]}  # Merge 2 dictionaries, keep data from second one
            if detalletfd['fechatimbrado'] is not None:
                detalletfd['fechatimbrado'] = datetime.strptime(detalletfd['fechatimbrado'], '%Y-%m-%dT%H:%M:%S')
            reg = (detalletfd['uuid'], detalletfd['version'], detalletfd['fechatimbrado'],
                   detalletfd['rfcprovcertif'], detalletfd['leyenda'], detalletfd['sellocfd'],
                   detalletfd['nocertificadosat'], detalletfd['sellosat'])
            cur.execute("INSERT INTO TimbreFiscalDigital VALUES(?,?,?,?,?,?,?,?)", reg)
            c.commit()
            cur.close()
            cur = c.cursor()
        elif con == 'aerolineas':
            # Version | TUA | TotalOtrosCargos
            compl = ('uuid', 'id', 'version', 'tua', 'totalotroscargos')
            detallecompl = dict.fromkeys(compl)
            detallecompl['uuid'] = UUID
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
                            c.commit()
                            cur.close()
                            cur = c.cursor()
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            if detallecompl['tua'] is not None:
                detallecompl['tua'] = float(detallecompl['tua'])
            if detallecompl['totalotroscargos'] is not None:
                detallecompl['totalotroscargos'] = float(detallecompl['totalotroscargos'])
            reg = (detallecompl['uuid'], detallecompl['id'], detallecompl['version'], detallecompl['tua'],
                   detallecompl['totalotroscargos'])
            cur.execute("INSERT INTO Aerolineas VALUES(?,?,?,?,?)", reg)
            c.commit()
            cur.close()
            cur = c.cursor()
        elif con == 'donatarias':
            # version | noautorizacion | fechaautorizacion | leyenda
            compl = ('uuid', 'id', 'version', 'noautorizacion', 'fechaautorizacion', 'leyenda')
            detallecompl = dict.fromkeys(compl)
            detallecompl = {**detallecompl, **data[con]}
            detallecompl['uuid'] = UUID
            detallecompl['id'] = idcomp(c, 'Donatarias')
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            if detallecompl['fechaautorizacion'] is not None:
                detallecompl['fechaautorizacion'] = datetime.strptime(detallecompl['fechaautorizacion'], '%Y-%m-%d')
            reg = (detallecompl['uuid'], detallecompl['id'], detallecompl['version'], detallecompl['noautorizacion'],
                   detallecompl['fechaautorizacion'], detallecompl['leyenda'])
            cur.execute("INSERT INTO Donatarias VALUES(?,?,?,?,?,?)", reg)
            c.commit()
            cur.close()
            cur = c.cursor()
        elif con == 'divisas':
            # version | tipooperacion
            compl = ('uuid', 'id', 'version', 'tipooperacion')
            detallecompl = dict.fromkeys(compl)
            detallecompl = {**detallecompl, **data[con]}
            detallecompl['uuid'] = UUID
            detallecompl['id'] = idcomp(c, 'Divisas')
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            reg = (detallecompl['uuid'], detallecompl['id'], detallecompl['version'], detallecompl['tipooperacion'])
            cur.execute("INSERT INTO Divisas VALUES(?,?,?,?)", reg)
            c.commit()
            cur.close()
            cur = c.cursor()
        elif con == 'pfintegrantecoordinado':
            # version | clavevehicular | placa | rfcpf
            compl = ('uuid', 'id', 'version', 'clavevehicular', 'placa', 'rfcpf')
            detallecompl = dict.fromkeys(compl)
            detallecompl = {**detallecompl, **data[con]}
            detallecompl['uuid'] = UUID
            detallecompl['id'] = idcomp(c, 'PFintegranteCoordinado')
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            reg = (detallecompl['uuid'], detallecompl['id'], detallecompl['version'], detallecompl['clavevehicular'],
                   detallecompl['placa'], detallecompl['rfcpf'])
            cur.execute("INSERT INTO PFintegranteCoordinado VALUES(?,?,?,?,?,?)", reg)
            c.commit()
            cur.close()
            cur = c.cursor()
        elif con == 'cfdiregistrofiscal':
            # version | folio
            compl = ('uuid', 'id', 'version', 'folio')
            detallecompl = dict.fromkeys(compl)
            detallecompl = {**detallecompl, **data[con]}
            detallecompl['uuid'] = UUID
            detallecompl['id'] = idcomp(c, 'CFDIRegistroFiscal')
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            reg = (detallecompl['uuid'], detallecompl['id'], detallecompl['version'], detallecompl['folio'])
            cur.execute("INSERT INTO CFDIRegistroFiscal VALUES(?,?,?,?)", reg)
            c.commit()
            cur.close()
            cur = c.cursor()
        elif con == 'pagoenespecie':
            # version | cvepic | foliosoldon | pzaartnombre | pzaarttecn | pzaartaprod | pzaartdim
            compl = ('uuid', 'id', 'version', 'cvepic', 'foliosoldon', 'pzaartnombre', 'pzaarttecn', 'pzaartaprod',
                     'pzaartdim')
            detallecompl = dict.fromkeys(compl)
            detallecompl = {**detallecompl, **data[con]}
            detallecompl['uuid'] = UUID
            detallecompl['id'] = idcomp(c, 'PagoEnEspecie')
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            reg = (detallecompl['uuid'], detallecompl['id'], detallecompl['version'], detallecompl['cvepic'],
                   detallecompl['foliosoldon'], detallecompl['pzaartnombre'], detallecompl['pzaarttecn'],
                   detallecompl['pzaartaprod'], detallecompl['pzaartdim'])
            cur.execute("INSERT INTO PagoEnEspecie VALUES(?,?,?,?,?,?,?,?,?)", reg)
            c.commit()
            cur.close()
            cur = c.cursor()
        elif con == 'obrasarteantiguedades':
            # version | tipobien | otrostipobien | tituloadquirido | otrostituloadquirido | subtotal | iva |
            # fechaadquisicion | característicasdeobraopieza
            compl = ('uuid', 'id', 'version', 'tipobien', 'otrostipobien', 'tituloadquirido', 'otrostituloadquirido',
                     'subtotal', 'iva', 'fechaadquisicion', 'característicasdeobraopieza')
            detallecompl = dict.fromkeys(compl)
            detallecompl = {**detallecompl, **data[con]}
            detallecompl['uuid'] = UUID
            detallecompl['id'] = idcomp(c, 'obrasarteantiguedades')
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            if detallecompl['subtotal'] is not None:
                detallecompl['subtotal'] = float(detallecompl['subtotal'])
            if detallecompl['iva'] is not None:
                detallecompl['iva'] = float(detallecompl['iva'])
            if detallecompl['fechaadquisicion'] is not None:
                detallecompl['fechaadquisicion'] = datetime.strptime(detallecompl['fechaadquisicion'], '%Y-%m-%d')
            reg = (detallecompl['uuid'], detallecompl['id'], detallecompl['version'], detallecompl['tipobien'],
                   detallecompl['otrostipobien'], detallecompl['tituloadquirido'], detallecompl['otrostituloadquirido'],
                   detallecompl['subtotal'], detallecompl['iva'], detallecompl['fechaadquisicion'],
                   detallecompl['característicasdeobraopieza'])
            cur.execute("INSERT INTO obrasarteantiguedades VALUES(?,?,?,?,?,?,?,?,?,?,?)", reg)
            c.commit()
            cur.close()
            cur = c.cursor()
        elif con == 'leyendasfiscales':
            # version
            compl = ('uuid', 'id', 'version', 'disposicionfiscal', 'norma', 'textoleyenda')
            if 'version' in data[con]:
                vers = data[con]['version']
            else:
                vers = '0.0'
            for dt in data[con]:
                if not dt.find('leyenda') == -1:
                    #  disposicionfiscal | norma | textoleyenda
                    ley = data[con][dt]
                    detallecompl = dict.fromkeys(compl)
                    detallecompl = {**detallecompl, **ley}
                    detallecompl['uuid'] = UUID
                    detallecompl['id'] = idcomp(c, 'LeyendasFiscales')
                    detallecompl['version'] = vers
                    if detallecompl['version'] is not None:
                        detallecompl['version'] = float(detallecompl['version'])
                    reg = (detallecompl['uuid'], detallecompl['id'], detallecompl['version'],
                           detallecompl['disposicionfiscal'], detallecompl['norma'], detallecompl['textoleyenda'])
                    cur.execute("INSERT INTO LeyendasFiscales VALUES(?,?,?,?,?,?)", reg)
                    c.commit()
                    cur.close()
                    cur = c.cursor()
        elif con == 'turistapasajeroextranjero':
            # version | fechadetransito | tipotransito
            compl = ('uuid', 'id', 'version', 'fechadetransito', 'tipotransito')
            detallecompl = dict.fromkeys(compl)
            detallecompl['uuid'] = UUID
            detallecompl['id'] = idcomp(c, 'TuristaPasajeroExtranjero')
            for dt in data[con]:
                if dt in detallecompl:
                    detallecompl[dt] = data[con][dt]
                elif not dt.find('datostransito') == -1:
                    dtstr = data[con][dt]
                    # via | tipoid | numeroid | nacionalidad | empresatransporte | idtransporte
                    subcompl = ('id', 'via', 'tipoid', 'numeroid', 'nacionalidad', 'empresatransporte', 'idtransporte')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl = {**detallesubcompl, **dtstr}
                    detallesubcompl['id'] = detallecompl['id']
                    reg = (detallesubcompl['id'], detallesubcompl['via'], detallesubcompl['tipoid'],
                           detallesubcompl['numeroid'], detallesubcompl['nacionalidad'],
                           detallesubcompl['empresatransporte'], detallesubcompl['idtransporte'])
                    cur.execute("INSERT INTO datosTransito VALUES(?,?,?,?,?,?,?)", reg)
                    c.commit()
                    cur.close()
                    cur = c.cursor()
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            if detallecompl['fechadetransito'] is not None:
                detallecompl['fechadetransito'] = datetime.strptime(detallecompl['fechadetransito'],
                                                                    '%Y-%m-%dT%H:%M:%S')
            reg = (detallecompl['uuid'], detallecompl['id'], detallecompl['version'], detallecompl['fechadetransito'],
                   detallecompl['tipotransito'])
            cur.execute("INSERT INTO TuristaPasajeroExtranjero VALUES(?,?,?,?,?)", reg)
            c.commit()
            cur.close()
            cur = c.cursor()
        elif con == 'valesdedespensa':
            # version | tipooperacion | registropatronal | numerodecuenta | total
            compl = ('uuid', 'id', 'version', 'tipooperacion', 'registropatronal', 'numerodecuenta', 'total')
            detallecompl = dict.fromkeys(compl)
            detallecompl['uuid'] = UUID
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
                        detallesubcompl = {**detallesubcompl, **dtstr}
                        detallesubcompl['id'] = detallecompl['id']
                        if detallesubcompl['fecha'] is not None:
                            detallesubcompl['fecha'] = datetime.strptime(detallesubcompl['fecha'], '%Y-%m-%dT%H:%M:%S')
                        if detallesubcompl['importe'] is not None:
                            detallesubcompl['importe'] = float(detallesubcompl['importe'])
                        reg = (detallesubcompl['id'], detallesubcompl['identificador'], detallesubcompl['fecha'],
                               detallesubcompl['rfc'], detallesubcompl['curp'], detallesubcompl['nombre'],
                               detallesubcompl['numseguridadsocial'], detallesubcompl['importe'])
                        cur.execute("INSERT INTO Conceptos_ValesDeDespensa VALUES(?,?,?,?,?,?,?,?)", reg)
                        c.commit()
                        cur.close()
                        cur = c.cursor()
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            if detallecompl['total'] is not None:
                detallecompl['total'] = float(detallecompl['total'])
            reg = (detallecompl['uuid'], detallecompl['id'], detallecompl['version'], detallecompl['tipooperacion'],
                   detallecompl['registropatronal'], detallecompl['numerodecuenta'], detallecompl['total'])
            cur.execute("INSERT INTO ValesDeDespensa VALUES(?,?,?,?,?,?,?)", reg)
            c.commit()
            cur.close()
            cur = c.cursor()
        elif con == 'vehiculousado':
            # version | montoadquisicion | montoenajenacion | clavevehicular | marca | tipo | modelo | numeromotor |
            # numeroserie | niv | valor
            compl = ('uuid', 'id', 'version', 'montoadquisicion', 'montoenajenacion', 'clavevehicular', 'marca', 'tipo',
                     'modelo', 'numeromotor', 'numeroserie', 'niv', 'valor', 'id_aduana')
            detallecompl = dict.fromkeys(compl)
            detallecompl['uuid'] = UUID
            detallecompl['id'] = idcomp(c, 'VehiculoUsado')
            for dt in data[con]:
                if dt in detallecompl:
                    detallecompl[dt] = data[con][dt]
                elif not dt.find('informacionaduanera') == -1:
                    dtstr = data[con][dt]
                    detallecompl['cve_aduana'] = idaduana(c)
                    # numero | fecha | aduana
                    subcompl = ('cve_aduana', 'numero', 'fecha', 'aduana')
                    detallesubcompl = dict.fromkeys(subcompl)
                    detallesubcompl = {**detallesubcompl, **dtstr}
                    detallesubcompl['cve_aduana'] = detallecompl['cve_aduana']
                    if detallesubcompl['fecha'] is not None:
                        detallesubcompl['fecha'] = datetime.strptime(detallesubcompl['fecha'], '%Y-%m-%d')
                    reg = (detallesubcompl['cve_aduana'], detallesubcompl['numero'], detallesubcompl['fecha'],
                           detallesubcompl['aduana'])
                    cur.execute("INSERT INTO T_Aduana VALUES(?,?,?,?)", reg)
                    c.commit()
                    cur.close()
                    cur = c.cursor()
        elif con == 'parcialesconstruccion':
            # version | numperlicoaut
            compl = ('uuid', 'id', 'version', 'numperlicoaut')
            detallecompl = dict.fromkeys(compl)
            detallecompl['uuid'] = UUID
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
                    detallesubcompl = {**detallesubcompl, **dtstr}
                    detallesubcompl['id'] = detallecompl['id']
                    reg = (detallesubcompl['id'], detallesubcompl['calle'], detallesubcompl['noexterior'],
                           detallesubcompl['nointerior'], detallesubcompl['colonia'], detallesubcompl['localidad'],
                           detallesubcompl['referencia'], detallesubcompl['municipio'], detallesubcompl['estado'],
                           detallesubcompl['codigopostal'])
                    cur.execute("INSERT INTO Inmueble_parcialesconstruccion VALUES(?,?,?,?,?,?,?,?,?,?)", reg)
                    c.commit()
                    cur.close()
                    cur = c.cursor()
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            reg = (detallecompl['uuid'], detallecompl['id'], detallecompl['version'], detallecompl['numperlicoaut'])
            cur.execute("INSERT INTO parcialesconstruccion VALUES(?,?,?,?)", reg)
            c.commit()
            cur.close()
            cur = c.cursor()
        elif con == 'ine':
            # version | tipoproceso | tipocomite | idcontabilidad
            compl = ('uuid', 'id', 'version', 'tipoproceso', 'tipocomite', 'idcontabilidad')
            detallecompl = dict.fromkeys(compl)
            detallecompl['uuid'] = UUID
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
                    c.commit()
                    cur.close()
                    cur = c.cursor()
            if detallecompl['version'] is not None:
                detallecompl['version'] = float(detallecompl['version'])
            reg = (detallecompl['uuid'], detallecompl['id'], detallecompl['version'], detallecompl['tipoproceso'],
                   detallecompl['tipocomite'], detallecompl['idcontabilidad'])
            cur.execute("INSERT INTO INE VALUES(?,?,?,?,?,?)", reg)
            c.commit()
            cur.close()
            cur = c.cursor()
