# -*- coding: utf-8 -*-
"""
Created on Thu Aug 25 14:49:06 2022

@author: jcgarciam
"""

import Transformaciones as Tr
import Extraccion as Ex
import numpy as np
import pandas as pd


def CambioFormato(df, a = 'a'):
    df[a] = df[a].astype(str)
    df[a] = np.where(df[a].str[-2::] == '.0', df[a].str[0:-2], df[a])
    df[a] = np.where(df[a] == 'nan', np.nan, df[a])
    return df[a]

def CreacionSISCO_ARL(Maestro_Consolidado, Pagos, Glosas, Conciliaciones, path_entrada2, path_entrada5):
    
    Glosas = Glosas[Glosas['Origen_Glosa'] == 'ARL']
    Glosas = Tr.GlosasFinal(Glosas)
    
    Rpt_arl = Ex.ExtraccionRadicadosARL(path_entrada5)
    Rpt_arl = Tr.RPT_Radicados_ARL_Final(Rpt_arl)
    
    Asistenciales = Ex.ExtraccionAistenciales(path_entrada2)
    Asistenciales = Tr.AsistencialesFinal(Asistenciales)
    
    Pagos_ARL = Pagos[Pagos['CODFUE'] == 8]
    Pagos_ARL = Pagos_ARL.drop_duplicates('NUREFE', keep = 'last')
    
    Conciliaciones = Conciliaciones[Conciliaciones['Origen'].astype(str).str.upper().str.contains('ARL') == True]
    Conciliaciones = Conciliaciones[Conciliaciones['Codigo_barras'].isnull() == False]
    Conciliaciones['Codigo_barras']  = CambioFormato(Conciliaciones, a = 'Codigo_barras')
    Conciliaciones = Conciliaciones.drop_duplicates('Codigo_barras', keep = 'last')
    Conciliaciones = Conciliaciones.rename(columns = {'Codigo_barras':'Cod_Barra'})
    del(Conciliaciones['Origen'])
    
    Maestro_ARL = Maestro_Consolidado[Maestro_Consolidado['Regimen'].str.upper().str.contains('ARL')]
    
    Maestro_ARL['Cod_Barra_Pago_Parcial'] = CambioFormato(Maestro_ARL, a = 'Cod_Barra_Pago_Parcial')
    Maestro_ARL['Cod_Barra'] = CambioFormato(Maestro_ARL, a = 'Cod_Barra')
    Rpt_arl['CODIGO_BARRAS'] = CambioFormato(Rpt_arl, a = 'CODIGO_BARRAS')
    
    Maestro_ARL = Maestro_ARL.merge(Rpt_arl, how = 'left', left_on = 'Cod_Barra_Pago_Parcial', right_on = 'CODIGO_BARRAS')
    Maestro_ARL = Maestro_ARL.merge(Rpt_arl, how = 'left', left_on = 'Cod_Barra', right_on = 'CODIGO_BARRAS')
    
    Maestro_ARL = Tr.UnificacionCampos(Maestro_ARL, campo = 'RADICADO')
    
    Maestro_ARL['llave'] = Maestro_ARL['Cod_Barra_Pago_Parcial'].astype(str) + Maestro_ARL['Cod_Barra'].astype(str) + Maestro_ARL['Radicado'].astype(str)
    Maestro_ARL = Maestro_ARL.drop_duplicates('llave')
    
    Maestro_ARL['Radicado'] = CambioFormato(Maestro_ARL, a = 'Radicado')
    Asistenciales['RADICADO'] = CambioFormato(Asistenciales, a = 'RADICADO')
    Maestro_ARL = Maestro_ARL.merge(Asistenciales, how = 'left', left_on = 'Radicado', right_on = 'RADICADO')
    
    
    Maestro_ARL['Referencia_Egresos'] = CambioFormato(Maestro_ARL, a = 'Referencia_Egresos')
    Pagos_ARL['NUREFE'] = CambioFormato(Pagos_ARL, a = 'NUREFE')
    Maestro_ARL = Maestro_ARL.merge(Pagos_ARL, how = 'left', left_on = 'Referencia_Egresos', right_on = 'NUREFE')
    
    Maestro_ARL['Fac_Norint'] = CambioFormato(Maestro_ARL, a = 'Fac_Norint')
    Glosas['Numero_Interno'] = CambioFormato(Glosas, a = 'Numero_Interno')
    
    Maestro_ARL = Maestro_ARL.merge(Glosas, how = 'left', left_on = 'Fac_Norint', right_on = 'Numero_Interno')
    
    Maestro_ARL = Tr.UnificacionCampos(Maestro_ARL, campo = 'CODIGO_BARRAS')
    
    Maestro_ARL['Marcacion_RPT'] = np.where(Maestro_ARL['Codigo_Barras'] != 'Sin CODIGO_BARRAS', 'Si', 'No')
    
    Maestro_ARL['RADICADO'] = Maestro_ARL['RADICADO'].fillna('Sin Radicado')    
    Maestro_ARL['Marcacion_Asistenciales'] = np.where(Maestro_ARL['RADICADO'] != 'Sin Radicado', 'Si', 'No')
    
    Maestro_ARL['NUREFE'] = Maestro_ARL['NUREFE'].fillna('Sin NUREFE')    
    Maestro_ARL['Marcacion_Egresos'] = np.where(Maestro_ARL['NUREFE'] != 'Sin NUREFE', 'Si', 'No')
    
    Maestro_ARL['Numero_Interno'] = Maestro_ARL['Numero_Interno'].fillna('Sin Numero_Interno')    
    Maestro_ARL['Marcacion_F.Glosas'] = np.where(Maestro_ARL['Numero_Interno'] != 'Sin Numero_Interno', 'Si', 'No')
    
    lista = ['Numero_Factura_Original','NIT','Razon_Social','Fecha_Radicacion','Valor_Bruto','Valor_Neto','Valor_Glosa',
             'Numero_OP','Fecha_OP','Valor_Pagado_Egresos','Valor_OP','Valor_Iva','Rete_Fuente','Rete_ICA','Rete_IVA',
             'Estado_Pago','Radicado','Regimen','Numero_Factura_Virtual','Fecha_Lote','Siniestro','Auditor_conciliación',
             'Cod_Barra','Estado_actual','Fecha_Ultimo_estado','Fac_Norint','Valor_Descuento','Codigo_Forma_Pago',
             'Fecha_Auditoria','Fecha_Envio','Nro_Guia','Codigo_Glosa','Estado_Egresos','Estado_Asitenciales','Origen',
             'Cod_Barra_Pago_Parcial','Tipo_Cuenta_Medica','Numero_Interno','Departamento','Municipio','Zona',
             'Usuario_Inicia_Radicacion','Valor_Copago','Valor_Cuota_Moderadora','IVA','Marcacion_RPT',
             'Marcacion_Asistenciales','Marcacion_Egresos','Marcacion_F.Glosas','Observacion','Nota_crédito',
             'Nombre_Estado_Actual','Fecha_Ultimo_estado_glosa','Fecha_Ingreso','Valor_Neto_Pago_Parcial','Descuento_Pronto_Pago',
             'Tipo_Doc_Prestador','Auditor','Fecha_Factura','TipoCuentaMedica','Fecha de registro','Fecha de causacion']

    Maestro_ARL = Maestro_ARL[lista]    
    
    
    Maestro_ARL['Numero_OP'] = CambioFormato(Maestro_ARL, a = 'Numero_OP')
    
    Maestro_ARL = Maestro_ARL.merge(Conciliaciones, how = 'left', on = 'Cod_Barra')
    
    Campos_A_Cambiar = ['Valor_Bruto','Valor_Neto','Valor_Glosa','Valor_Pagado_Egresos','Valor_OP','IVA','Rete_Fuente',
                        'Rete_ICA','Rete_IVA','Valor_Descuento','Valor_Copago','Valor_Cuota_Moderadora',
                        'Valor_Iva']
    
    for i in Campos_A_Cambiar:
        Maestro_ARL[i] = Maestro_ARL[i].astype(float)
    
    return Maestro_ARL


def ARL_No_Cruzados(Maestro_ARL):
    
    Maestro_ARL = Maestro_ARL.drop(columns = ['Valor_Glosa','Numero_OP','Fecha_OP','Valor_Pagado_Egresos','Valor_OP',
                                              'IVA','Rete_Fuente','Rete_ICA','Rete_IVA','Estado_Pago','Numero_Interno',
                                              'Codigo_Glosa','Observacion','Fecha_Envio','Nro_Guia','Auditor_conciliación',
                                              'Tipo_Cuenta_Medica','Marcacion_F.Glosas'])
    
    ARL_No_Registra_RPT = Maestro_ARL[Maestro_ARL['Marcacion_RPT'] == 'No']
    ARL_No_Registra_RPT = ARL_No_Registra_RPT.drop( columns = ['Marcacion_Asistenciales','Marcacion_Egresos'])
    
    ARL_No_Registra_Asistenciales = Maestro_ARL[Maestro_ARL['Marcacion_Asistenciales'] == 'No']
    ARL_No_Registra_Asistenciales = ARL_No_Registra_Asistenciales.drop( columns = ['Marcacion_RPT','Marcacion_Egresos'])
    
    ARL_No_Registra_Egresos = Maestro_ARL[Maestro_ARL['Marcacion_Egresos'] == 'No']
    ARL_No_Registra_Egresos = ARL_No_Registra_Egresos.drop( columns = ['Marcacion_RPT','Marcacion_Asistenciales'])
    
    return ARL_No_Registra_RPT, ARL_No_Registra_Asistenciales, ARL_No_Registra_Egresos

def Tablero_Glosas(Maestro_ARL):

    columnas = ['Fecha_OP','Numero_OP','Valor_Pagado_Egresos','Estado_Pago',
                'Rete_ICA','Rete_Fuente','Cod_Barra','Rete_IVA']

    sisco_pagos = Maestro_ARL[columnas].copy()
                
    sisco_pagos['Fecha_OP'] = pd.to_datetime(sisco_pagos['Fecha_OP'], format = '%d/%m/%Y', dayfirst = True)
    sisco_pagos['Numero_OP'] = CambioFormato(sisco_pagos, a = 'Numero_OP')
    sisco_pagos = sisco_pagos[sisco_pagos['Numero_OP'].isnull() == False]
    sisco_pagos['Valor_Pagado_Egresos'] = sisco_pagos['Valor_Pagado_Egresos'].astype(str).str.replace(',','.').astype(float)
    sisco_pagos['Rete_ICA'] = sisco_pagos['Rete_ICA'].astype(str).str.replace(',','.').astype(float)
    sisco_pagos['Rete_Fuente'] = sisco_pagos['Rete_Fuente'].astype(str).str.replace(',','.').astype(float)
    sisco_pagos['Rete_IVA'] = sisco_pagos['Rete_IVA'].astype(str).str.replace(',','.').astype(float)

    sisco_pagos['Total Valor Pagado'] = sisco_pagos['Valor_Pagado_Egresos'] + sisco_pagos['Rete_ICA'] + sisco_pagos['Rete_Fuente'] + sisco_pagos['Rete_IVA']

    sisco_pagos['Cantidad de OPs'] = sisco_pagos['Numero_OP'].copy()

    sisco_pagos = sisco_pagos.sort_values('Fecha_OP', ascending = False)

    sisco_pagos = sisco_pagos.groupby('Cod_Barra', as_index = False).agg({
                    'Total Valor Pagado':sum,'Fecha_OP':'first','Cantidad de OPs':'count',
                    'Valor_Pagado_Egresos':'sum','Estado_Pago':'last','Rete_ICA':'sum',
                    'Rete_Fuente':'sum'})

    sisco_pagos = sisco_pagos.rename(columns = {'Fecha_OP':'Ultima Fecha de Pago'})
    sisco_pagos['Total Valor Pagado'] = sisco_pagos['Total Valor Pagado'].astype(str).str.replace('.',',')
    sisco_pagos['Valor_Pagado_Egresos'] = sisco_pagos['Valor_Pagado_Egresos'].astype(str).str.replace('.',',')
    sisco_pagos['Rete_ICA'] = sisco_pagos['Rete_ICA'].astype(str).str.replace('.',',')
    sisco_pagos['Rete_Fuente'] = sisco_pagos['Rete_Fuente'].astype(str).str.replace('.',',')
    sisco_pagos['Cantidad de OPs'] = sisco_pagos['Cantidad de OPs'].astype(int)
    
    columnas = ['Regimen','NIT','Razon_Social','Tipo_Doc_Prestador','Fecha_Radicacion','Fecha_Ingreso',
                'Numero_Factura_Original','Valor_Neto','Estado_actual','Fecha_Ultimo_estado','Fecha_Auditoria',
                'Auditor','Fecha_Factura','Valor_Glosa','Cod_Barra','TipoCuentaMedica',
                'Fac_Norint','Valor_Bruto','Valor_Copago','Valor_Cuota_Moderadora','Valor_Descuento',
                'Valor_Iva','Descuento_Pronto_Pago','Auditor_conciliación','Departamento','Municipio',
                'Zona','Nota_crédito','Fecha de registro','Fecha de causacion','Numero_conciliacion']
    
    Maestro2 = Maestro_ARL[columnas].copy()
    
    Maestro2 = Maestro2.rename( columns = {'Numero_Factura_Original':'Numero_Factura'})

    Maestro2['Valor ultima Glosa'] = Maestro2['Valor_Glosa'].copy()    
    
    Maestro2['Cod_Barra'] = CambioFormato(Maestro2, a = 'Cod_Barra')
    
    total_ops = Maestro_ARL[['Cod_Barra','Numero_OP']].copy()
    
    total_ops['Cod_Barra'] = CambioFormato(total_ops, a = 'Cod_Barra')
    
    total_ops['Numero_OP'] = CambioFormato(total_ops, a = 'Numero_OP')
    
    total_ops['llave'] = total_ops['Cod_Barra'].astype(str) + total_ops['Numero_OP'].astype(str)
    
    total_ops['Numero_OP'] = total_ops['Numero_OP'].astype(str) + '-'
    
    total_ops = total_ops.drop_duplicates('llave')
    
    total_ops = total_ops.drop(columns = ['llave'])
    
    total_ops = total_ops.groupby('Cod_Barra', as_index = False)['Numero_OP'].sum()
    
    total_ops['Numero_OP'] = total_ops['Numero_OP'].replace({'nan-',''})
    
    total_ops['Numero_OP'] = np.where(total_ops['Numero_OP'].str[-1] == '-',total_ops['Numero_OP'].str[0:-1], total_ops['Numero_OP'])

    total_ops = total_ops.rename(columns = {'Numero_OP':'Ordenes de pago'})
    
    Maestro2['Fecha_Ultimo_estado'] = pd.to_datetime(Maestro2['Fecha_Ultimo_estado'], format = '%d/%m/%Y', dayfirst = True)
    
    Maestro2['Fecha_Radicacion'] = pd.to_datetime(Maestro2['Fecha_Radicacion'], format = '%d/%m/%Y', dayfirst = True)
    
    Maestro2 = Maestro2.sort_values('Fecha_Radicacion', ascending = False)
    
    Maestro2 = Maestro2.drop_duplicates('Cod_Barra', keep = 'first')    
    
    Maestro2 = Maestro2.merge(total_ops, how = 'left', on = 'Cod_Barra')
    
    del total_ops
    
    Maestro2 = Maestro2[Maestro2['Fecha_Radicacion'].dt.year >= 2021]

    columnas = ['Valor_Neto','Valor_Copago','Valor_Cuota_Moderadora','Valor_Descuento','Valor_Iva',
                'Valor_Glosa','Valor_Bruto','Descuento_Pronto_Pago']

    for i in columnas:
        print(i)
        Maestro2[i] = Maestro2[i].astype(str).str.replace('.',',')
        
    Maestro2 = Maestro2.merge(sisco_pagos, how = 'left', on = 'Cod_Barra')

    return Maestro2