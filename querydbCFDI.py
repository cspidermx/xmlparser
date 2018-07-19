from dbmgmnt import dbopen, dbclose


db = 'C:\\Users\\Charly\\Dropbox\\Work\\CFDIs\\CFDIs.sqlite'
# db = 'E:\\Dropbox\\Dropbox\\Work\\CFDIs\\CFDIs.sqlite'
dbcon = dbopen(db)

c = dbcon.cursor()
sql = 'SELECT UUID from CFDI'
allcfdis = c.execute(sql).fetchall()
# Donatarias | LeyendasFiscales | Complemento_SPEI | ValesDeDespensa | EstadoDeCuentaCombustible | ConsumoDeCombustibles
# Divisas | PFIntegranteCoordinado | TuristaPasajeroExtranjero | PagoEnEspecie | CFDIRegistroFiscal | INE | Aerolineas
# obrasarteantiguedades | ImpuestosLocales | Vehiculousado | NotariosPublicos | parcialesconstruccion
# renovacionysustitucionvehiculos | certificadodedestruccion | ComercioExterior | Pagos | Nomina
comps = ('Donatarias', 'LeyendasFiscales', 'Complemento_SPEI', 'ValesDeDespensa', 'EstadoDeCuentaCombustible',
         'ConsumoDeCombustibles', 'Divisas', 'PFIntegranteCoordinado', 'TuristaPasajeroExtranjero', 'PagoEnEspecie',
         'CFDIRegistroFiscal', 'INE', 'Aerolineas', 'obrasarteantiguedades', 'ImpuestosLocales', 'Vehiculousado',
         'NotariosPublicos', 'parcialesconstruccion', 'renovacionysustitucionvehiculos', 'certificadodedestruccion',
         'ComercioExterior', 'Pagos', 'Nomina')
# compC_acreditamientoIEPS | compC_instEducativas | compC_VentaVehiculos | compC_PorCuentaDeTerceros
compsconc = ('compC_acreditamientoIEPS', 'compC_instEducativas', 'compC_VentaVehiculos', 'compC_PorCuentaDeTerceros')
for fact in allcfdis:
    complementos = ""
    complementosC = ""
    otros = ""
    for tbl in comps:
        curr = dbcon.cursor()
        sql = 'Select UUID from {} where UUID = "{}"'.format(tbl, fact[0])
        if len(curr.execute(sql).fetchall()) > 0:
            complementos = complementos + tbl + ", "
        curr.close()
    if complementos != "":
        complementos = complementos[: -2]
    curr = dbcon.cursor()
    sql = 'Select numcpto from Conceptos where UUID = "{}"'.format(fact[0])
    allcc = curr.execute(sql).fetchall()
    compC = dict.fromkeys(compsconc)
    for cc in allcc:
        idcpto = cc[0]
        for compcon in compsconc:
            ccon = dbcon.cursor()
            sql = 'Select id from {} where id="{}"'.format(compcon, idcpto)
            if len(ccon.execute(sql).fetchall()) > 0:
                compC[compcon] = 1
            ccon.close()
    for llave in compC:
        if compC[llave] is not None:
            complementosC = complementosC + llave + ", "
    if complementosC != "":
        complementosC = complementosC[: -2]
    curr.close()
    curr = dbcon.cursor()
    sql = 'Select UUID from CFDIrelacionados where UUID = "{}"'.format(fact[0])
    if len(curr.execute(sql).fetchall()) > 0:
        otros = otros + "CFDIrelacionados, "
    if otros != "":
        otros = otros[: -2]
    curr.close()
    print(fact[0], otros, "|", complementos, "|", complementosC)
dbclose(dbcon)
