# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 09:01:18 2023

@author: jcgarciam
"""
# Este codigo toma las facturas de Gestion Documental que llega todos los dias por correo
# de parte de RGC y las unifica con las facturas devueltas o anuladas que estan en el 
# Maestro y no aparecen en Gestion Documental

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

def CambioFormato(df, a = 'a'):
    df[a] = df[a].astype(str).str.strip('\x02').str.strip('').str.strip()
    df[a] = np.where(df[a].str[-2::] == '.0', df[a].str[0:-2], df[a])
    df[a] = np.where(df[a] == 'nan', np.nan, df[a])
    return df[a]   

def Devoluciones(devoluciones_entrada, Maestro, Glosas):
    
    devoluciones_entrada['Origen'] = 'Entrada'
    devoluciones_entrada['Orden'] = 2 
    
    # Facturas anuladas segun los estos estados
    lista = ['AUDITADA: Factura Anulada','AUDITADA: Devuelta sin posibilidad de re-ingreso.','EN AUDITORIA: Factura Anulada']
    
    Maestro2 = Maestro.copy()
    Maestro2 = Maestro2[Maestro2['Estado_actual'].isin(lista) == True]

    Maestro2 = Maestro2.rename(columns = {'Razon_Social':'RAZON SOCIAL','Numero_Factura':'NUMERO DE FACTURA',
                                      'Cod_Barra':'CODIGO_BARRA','Fecha_Radicacion':'FECHA DE RADICADO',
                                      'Fecha_Comentario':'FECHA DE DEVOLUCION','Regimen':'REGIMEN',
                                      'Estado_actual':'ESTADO'})

    Maestro2['Origen'] = 'Auditoria'
    Maestro2['Orden'] = 3

    # Como las facturas de Maestro no tienen un campo del motivo por el que se devolivieron
    # lo cruzamos con el archivo de Glosas para asociarle los codigos de glosa que pueda 
    # tener la factura
    
    Glosas = Glosas[['Numero_Interno','Codigo_Glosa']]
    Glosas = Glosas.rename(columns = {'Numero_Interno':'Fac_Norint','Codigo_Glosa':'RUBRO-CAUSAL'})
    Glosas['Fac_Norint'] = CambioFormato(Glosas, a = 'Fac_Norint')

    Glosas['Llave'] = Glosas['Fac_Norint'].astype(str) + Glosas['RUBRO-CAUSAL'].astype(str)
    Glosas = Glosas.drop_duplicates('Llave')

    Maestro3 = Maestro2.copy()
    Maestro3['Fac_Norint'] = CambioFormato(Maestro3, a = 'Fac_Norint')
    Maestro3 = Maestro3.merge(Glosas, how = 'left', on = 'Fac_Norint')
        
    devoluciones = pd.concat([devoluciones_entrada,Maestro3]).reset_index(drop = True)
    devoluciones['CODIGO_BARRA'] = CambioFormato(devoluciones, a = 'CODIGO_BARRA')

    devoluciones2 = devoluciones.copy()
    devoluciones2 = devoluciones2[devoluciones2['FECHA DE RADICADO'].astype(str).str.contains('/') == True]
    devoluciones2['FECHA DE RADICADO'] = pd.to_datetime(devoluciones2['FECHA DE RADICADO'], format = '%d/%m/%Y')

    devoluciones3 = devoluciones.copy()
    devoluciones3 = devoluciones3[devoluciones3['FECHA DE RADICADO'].astype(str).str.contains('/') == False]
    devoluciones3['FECHA DE RADICADO'] = pd.to_datetime(devoluciones3['FECHA DE RADICADO'], format = '%Y-%m-%d')
    devoluciones = pd.concat([devoluciones2,devoluciones3])

    del devoluciones2, devoluciones3

    devoluciones.loc[devoluciones['REGIMEN'].astype(str).str.upper().str.contains('MPP') == True,'REGIMEN'] = 'MPP'
    devoluciones.loc[devoluciones['REGIMEN'].astype(str).str.upper().str.contains('HYC') == True,'REGIMEN'] = 'HYC'
    devoluciones.loc[devoluciones['REGIMEN'].astype(str).str.upper().str.contains('ARL') == True,'REGIMEN'] = 'ARL'

    def Cantidad(df, a):
        df2 = df.copy()
        df2['Cantidad de '+ a] = df2[a].copy()
        df2 = df2.groupby(a, as_index = False)['Cantidad de ' + a].count()
        df = df.merge(df2, how = 'left', on = a)
        df = df.sort_values(['Cantidad de ' + a, a], ascending = False)
        return df

    devoluciones['llave'] = devoluciones['CODIGO_BARRA'].astype(str) + devoluciones['Origen'].astype(str) + devoluciones['FECHA DE RADICADO'].astype(str)
    devoluciones = Cantidad(devoluciones, a = 'llave')

    devoluciones = devoluciones.drop(columns = ['llave'])
    devoluciones = devoluciones.rename({'Cantidad de CODIGO_BARRA':'Cantidad de facturas'})
    devoluciones = devoluciones.sort_values(['FECHA DE RADICADO','Orden'], ascending = False)
    devoluciones['Llave'] = devoluciones['CODIGO_BARRA'].astype(str) + devoluciones['FECHA DE RADICADO'].astype(str)

    devoluciones = devoluciones.drop_duplicates('Llave', keep = 'first')

    devoluciones['NUMERO DE FACTURA'] = CambioFormato(devoluciones, a = 'NUMERO DE FACTURA')
    devoluciones['NUMERO DE ACTA DEVOLUCION'] = CambioFormato(devoluciones, a = 'NUMERO DE ACTA DEVOLUCION')
    devoluciones['FECHA DE DEVOLUCION'] = CambioFormato(devoluciones, a = 'FECHA DE DEVOLUCION')
    devoluciones['NIT'] = CambioFormato(devoluciones, a = 'NIT')

    devoluciones = Cantidad(devoluciones, a = 'CODIGO_BARRA')

    devoluciones = devoluciones[['NIT', 'RAZON SOCIAL', 'REGIMEN', 'FECHA DE RADICADO', 'CODIGO_BARRA',
                                 'NUMERO DE FACTURA', 'ESTADO', 'RUBRO-CAUSAL',
                                 'NUMERO DE ACTA DEVOLUCION', 'FECHA DE DEVOLUCION','Zona','Origen']]
    
    devoluciones_arl = devoluciones[devoluciones['REGIMEN'].astype(str).str.upper().str.contains('ARL') == True]
    devoluciones_salud = devoluciones[devoluciones['REGIMEN'].astype(str).str.upper().str.contains('ARL') == False]
    
    return devoluciones_arl, devoluciones_salud

def RadicadoVsDevolucionesSalud(devoluciones_salud, Maestro):
        
    Maestro2 = Maestro.copy()
    Maestro2 = Maestro2[Maestro2['Regimen'].astype(str).str.upper().str.contains('ARL') == False]
    Maestro2 = Maestro2[['Razon_Social','Numero_Factura','Cod_Barra','Fecha_Radicacion',
                         'Fecha_Comentario','Regimen','Estado_actual','NIT','Zona']]
    
    Maestro2.loc[Maestro2['Regimen'] == 'AXA Colpatria-Medicina prepagada MPP','Regimen'] = 'MPP'
    Maestro2.loc[Maestro2['Regimen'] == 'AXA Colpatria-Hospitalizacion y cirugia HYC','Regimen'] = 'HYC'
    
    Maestro2['Fecha_Radicacion'] = pd.to_datetime(Maestro2['Fecha_Radicacion'], format = '%d/%m/%Y')

    Maestro2 = Maestro2.rename(columns = {'Razon_Social':'RAZON SOCIAL','Numero_Factura':'NUMERO DE FACTURA',
                                      'Cod_Barra':'CODIGO_BARRA','Fecha_Radicacion':'FECHA DE RADICADO',
                                      'Fecha_Comentario':'FECHA DE DEVOLUCION','Regimen':'REGIMEN',
                                      'Estado_actual':'ESTADO'})
    
    Maestro2['CODIGO_BARRA'] = CambioFormato(Maestro2, a = 'CODIGO_BARRA')
    
    devoluciones_salud['CODIGO_BARRA'] = CambioFormato(devoluciones_salud, a = 'CODIGO_BARRA')
    
    Maestro2 = Maestro2[Maestro2['CODIGO_BARRA'].isin(devoluciones_salud['CODIGO_BARRA']) == False]
    Maestro2['DEVOLUCION'] = 'No'
    devoluciones_salud['DEVOLUCION'] = 'Si'
    
    fecha_fin = datetime.today().date() - timedelta(datetime.today().day)
    fecha_ini = fecha_fin - timedelta(fecha_fin.day - 1)
    
    
    DevVsRad = pd.concat([devoluciones_salud, Maestro2]).reset_index(drop = True)
    DevVsRad = DevVsRad[DevVsRad['FECHA DE RADICADO'].between(str(fecha_ini), str(fecha_fin)) == True]
    DevVsRad['NIT'] = CambioFormato(DevVsRad, a = 'NIT')
    
    Tipo_Documento = Maestro[['NIT','Tipo_Doc_Prestador']].copy()
    Tipo_Documento['NIT'] = CambioFormato(Tipo_Documento, a = 'NIT')
    Tipo_Documento = Tipo_Documento.drop_duplicates('NIT')
    Tipo_Documento['Tipo_Doc_Prestador'] = Tipo_Documento['Tipo_Doc_Prestador'].astype(str).str.upper()
    Tipo_Documento = Tipo_Documento.rename(columns = {'Tipo_Doc_Prestador':'TIPO DE DOCUMENTO'})
    
    DevVsRad = DevVsRad.merge(Tipo_Documento, how = 'left', on = 'NIT')
    
    FacturasXPrestador = DevVsRad.copy()
    FacturasXPrestador['NUMERO DEVUELTAS'] = np.where(FacturasXPrestador['DEVOLUCION'] == 'Si',1,0)
    FacturasXPrestador['NUMERO RADICADAS'] = FacturasXPrestador['CODIGO_BARRA'].copy()
    FacturasXPrestador = FacturasXPrestador.groupby(['NIT','RAZON SOCIAL']).agg({'NUMERO RADICADAS':'count','NUMERO DEVUELTAS':'sum'})
    FacturasXPrestador = FacturasXPrestador.sort_values('NUMERO RADICADAS', ascending = False)
    FacturasXPrestador['PORC DEVOLUCIONES'] = (FacturasXPrestador['NUMERO DEVUELTAS']/FacturasXPrestador['NUMERO RADICADAS'])*100
    FacturasXPrestador['PORC DEVOLUCIONES'] = '%' + FacturasXPrestador['PORC DEVOLUCIONES'].map('{:,.2f}'.format)
    
    FacturasXCedula = DevVsRad.copy()
    FacturasXCedula = FacturasXCedula[FacturasXCedula['TIPO DE DOCUMENTO'] != 'NI']
    FacturasXCedula['NUMERO DEVUELTAS'] = np.where(FacturasXCedula['DEVOLUCION'] == 'Si',1,0)
    FacturasXCedula['NUMERO RADICADAS'] = FacturasXCedula['CODIGO_BARRA'].copy()
    FacturasXCedula = FacturasXCedula.groupby(['NIT','RAZON SOCIAL']).agg({'NUMERO RADICADAS':'count','NUMERO DEVUELTAS':'sum'})
    FacturasXCedula = FacturasXCedula.sort_values('NUMERO DEVUELTAS', ascending = False)
    FacturasXCedula['PORC DEVOLUCIONES'] = (FacturasXCedula['NUMERO DEVUELTAS']/FacturasXCedula['NUMERO RADICADAS'])*100
    FacturasXCedula['PORC DEVOLUCIONES'] = '%' + FacturasXCedula['PORC DEVOLUCIONES'].map('{:,.2f}'.format)
    
    MotivoXCedula = DevVsRad.copy()
    MotivoXCedula = MotivoXCedula[(MotivoXCedula['TIPO DE DOCUMENTO'] != 'NI') & (MotivoXCedula['DEVOLUCION'] == 'Si')]
    MotivoXCedula['NUMERO DEVUELTAS'] = 1
    MotivoXCedula = MotivoXCedula.groupby(['RAZON SOCIAL','RUBRO-CAUSAL'])['NUMERO DEVUELTAS'].sum()
    #MotivoXCedula = MotivoXCedula.sort_values('NUMERO DEVUELTAS', ascending = False)
    
    FacturasXNIT = DevVsRad.copy()
    FacturasXNIT = FacturasXNIT[FacturasXNIT['TIPO DE DOCUMENTO'] == 'NI']
    FacturasXNIT['NUMERO DEVUELTAS'] = np.where(FacturasXNIT['DEVOLUCION'] == 'Si',1,0)
    FacturasXNIT['NUMERO RADICADAS'] = FacturasXNIT['CODIGO_BARRA'].copy()
    FacturasXNIT = FacturasXNIT.groupby(['NIT','RAZON SOCIAL']).agg({'NUMERO RADICADAS':'count','NUMERO DEVUELTAS':'sum'})
    FacturasXNIT = FacturasXNIT.sort_values('NUMERO DEVUELTAS', ascending = False)
    FacturasXNIT['PORC DEVOLUCIONES'] = (FacturasXNIT['NUMERO DEVUELTAS']/FacturasXNIT['NUMERO RADICADAS'])*100
    FacturasXNIT['PORC DEVOLUCIONES'] = '%' + FacturasXNIT['PORC DEVOLUCIONES'].map('{:,.2f}'.format)
    
    MotivoXNIT = DevVsRad.copy()
    MotivoXNIT = MotivoXNIT[(MotivoXNIT['TIPO DE DOCUMENTO'] == 'NI') & (MotivoXNIT['DEVOLUCION'] == 'Si')]
    MotivoXNIT['NUMERO DEVUELTAS'] = 1
    MotivoXNIT = MotivoXNIT.groupby(['RAZON SOCIAL','RUBRO-CAUSAL'])['NUMERO DEVUELTAS'].sum()
    #MotivoXNIT = MotivoXNIT.sort_values('NUMERO DEVUELTAS', ascending = False)
    
    return DevVsRad, FacturasXPrestador, FacturasXCedula, FacturasXNIT, MotivoXCedula, MotivoXNIT
        
    

