# -*- coding: utf-8 -*-
"""
Created on Thu Sep 15 10:08:51 2022

@author: jcgarciam
"""

import pandas as pd
import csv
import zipfile
from datetime import datetime, timedelta
import glob

Tiempo_Total = datetime.now()
anio = Tiempo_Total.year

path_entrada = r'D:\DATOS\Users\jcgarciam\OneDrive - AXA Colpatria Seguros\Documentos\Informes\Reporte Cartera\Input'
path_entrada2 = r'\\dc1pcadfrs1\Reportes_Activa\axa'

path_salida = r'D:\DATOS\Users\jcgarciam\AXA Colpatria Seguros\SHAREPOINT ARL Y SALUD - SISCO\Devoluciones'
#%%
xls = pd.ExcelFile(glob.glob(path_entrada + '\Devoluciones Gestión Documental AXA  *.xlsx')[0])
sheets = xls.sheet_names
#%%
dic = {}

#columnas = ['NIT_IPS','IPS','Factura','Codigo_barra','Fecha_Radicado','Fecha de envio','RUBRO','Generacion_Numero_Acta']
for i in sheets:
    if (str(anio) in i) | (str(anio - 1) in i):
        print(i)
        df = pd.read_excel(glob.glob(path_entrada + '\Devoluciones Gestión Documental AXA  *.xlsx')[0], header = 0, sheet_name =  i)#, usecols  = columnas)
        df = df.rename(columns = {'NIT_IPS':'NIT','IPS':'RAZON SOCIAL',
                            'Factura':'NUMERO DE FACTURA','Codigo_barra':'CODIGO_BARRA','Fecha_Radicado':'FECHA DE RADICADO',
                            'Fecha de envio':'FECHA DE DEVOLUCION','RUBRO':'RUBRO-CAUSAL',
                            'Generacion_Numero_Acta':'NUMERO DE ACTA DEVOLUCION'})
        dic[i] = df
    
devoluciones_entrada = pd.concat(dic).reset_index(drop = True)

devoluciones_entrada = devoluciones_entrada.rename(columns = {'NIT_IPS':'NIT','IPS':'RAZON SOCIAL',
                        'Factura':'NUMERO DE FACTURA','Codigo_barra':'CODIGO_BARRA','Fecha_Radicado':'FECHA DE RADICADO',
                        'Fecha de envio':'FECHA DE DEVOLUCION','RUBRO':'RUBRO-CAUSAL',
                        'Generacion_Numero_Acta':'NUMERO DE ACTA DEVOLUCION'})


#%%
columnas = ['NIT_IPS','RAZON_SOCIAL','NUMERO_FACTURA','CODIGO_BARRA','FECHA_LOTE_RADICADO','FECHA_DEVOLUCION','RUBRO','NRO_ACTA_DEVOLUCION']

dic = {}

for i in glob.glob(path_entrada + '/*Devoluciones (AXA Colpatria-*.xlsx'):
    print(i)
    df = pd.read_excel(i, header = 0, usecols = columnas)
    dic[i] = df

devoluciones_activa = pd.concat(dic).reset_index(drop = True)
devoluciones_activa = devoluciones_activa.rename(columns = {'NIT_IPS':'NIT','RAZON_SOCIAL':'RAZON SOCIAL',
                        'NUMERO_FACTURA':'NUMERO DE FACTURA','FECHA_LOTE_RADICADO':'FECHA DE RADICADO',
                        'FECHA_DEVOLUCION':'FECHA DE DEVOLUCION','RUBRO':'RUBRO-CAUSAL',
                        'NRO_ACTA_DEVOLUCION':'NUMERO DE ACTA DEVOLUCION'})
#%%


Current_Date = datetime.today().date()

Current_Date = Current_Date.strftime('%Y')+Current_Date.strftime('%m')+ Current_Date.strftime('%d')
dic = {}

#columnas = ['NIT','Razon_Social','Numero_Factura','Cod_Barra','Fecha_Radicacion','Fecha_Ultimo_estado']

with zipfile.ZipFile(path_entrada2 + '/'+Current_Date+'.zip', mode = 'r') as z:
    lista = z.namelist()
    for i in lista:
        with z.open(i) as f:
            df = pd.read_csv(f, sep=';', header=0, encoding='ANSI', engine='python', quoting=csv.QUOTE_NONE, index_col =  False)
            dic[i] = df
        
Maestro = pd.concat(dic).reset_index(drop = True)

#%%

Maestro2 = Maestro.copy()
lista = ['AUDITADA: Factura Anulada','AUDITADA: Devuelta sin posibilidad de re-ingreso.','EN AUDITORIA: Factura Anulada']
Maestro2 = Maestro2[Maestro2['Estado_actual'].isin(lista) == True]

#%%

devoluciones_entrada2 = devoluciones_entrada.copy()
devoluciones_entrada2 = devoluciones_entrada2[devoluciones_entrada2['FECHA DE RADICADO'].astype(str).str.contains('/') == True]
devoluciones_entrada2['FECHA DE RADICADO'] = pd.to_datetime(devoluciones_entrada2['FECHA DE RADICADO'], format = '%d/%m/%Y')
devoluciones_entrada3 = devoluciones_entrada.copy()
devoluciones_entrada3 = devoluciones_entrada3[devoluciones_entrada3['FECHA DE RADICADO'].astype(str).str.contains('/') == False]
devoluciones_entrada3['FECHA DE RADICADO'] = pd.to_datetime(devoluciones_entrada3['FECHA DE RADICADO'], format = '%Y-%m-%d')
devoluciones_entrada4 = pd.concat([devoluciones_entrada2,devoluciones_entrada3])

#%%

devoluciones = pd.concat([devoluciones_entrada4,devoluciones_activa]).reset_index(drop = True)

date_now = datetime.now().date()

if date_now.day < 27:
    a = date_now - timedelta(date_now.day)
    fecha_fin = a - timedelta(a.day) + timedelta(27)
    fecha_ini = a - timedelta(a.day)
    fecha_ini = fecha_ini - timedelta(fecha_ini.day) + timedelta(28)
    print('La fecha inicial de corte es: ',str(fecha_ini))
    print('La fecha final de corte es: ',str(fecha_fin))

else:
    fecha_fin = date_now - timedelta(date_now.day) + timedelta(27)
    fecha_ini = date_now - timedelta(date_now.day)
    fecha_ini = fecha_ini - timedelta(fecha_ini.day) + timedelta(28)
    print('La fecha inicial de corte es: ',str(fecha_ini))
    print('La fecha final de corte es: ',str(fecha_fin))
    
devoluciones = devoluciones[devoluciones['FECHA DE RADICADO'].between(str(fecha_ini),str(fecha_fin)) == True]



#%%

def Cantidad(df, a):
    df2 = df.copy()
    df2['Cantidad de '+ a] = df2[a].copy()
    df2 = df2.groupby(a, as_index = False)['Cantidad de ' + a].count()
    df = df.merge(df2, how = 'left', on = a)
    df = df.sort_values(['Cantidad de ' + a, a], ascending = False)
    return df
#%%

devoluciones_activa = Cantidad(devoluciones_activa, a = 'CODIGO_BARRA')
devoluciones_entrada4 = Cantidad(devoluciones_entrada4, a = 'CODIGO_BARRA')

#%%
import numpy as np

def CambioFormato(df, a = 'a'):
    df[a] = df[a].astype(str).str.strip().str.strip('\x02').str.strip('')
    df[a] = np.where(df[a].str[-2::] == '.0', df[a].str[0:-2], df[a])
    df[a] = np.where(df[a] == 'nan', np.nan, df[a])
    return df[a]

#%%

devoluciones_activa['CODIGO_BARRA'] = CambioFormato(devoluciones_activa, a = 'CODIGO_BARRA')
devoluciones_entrada['CODIGO_BARRA'] = CambioFormato(devoluciones_entrada, a = 'CODIGO_BARRA')
Maestro2['Cod_Barra'] = CambioFormato(Maestro2, a = 'Cod_Barra')
Maestro['Cod_Barra'] = CambioFormato(Maestro, a = 'Cod_Barra')

    
a = devoluciones_activa[devoluciones_activa['CODIGO_BARRA'].isin(devoluciones_entrada['CODIGO_BARRA']) == False]
b = devoluciones_activa[devoluciones_activa['CODIGO_BARRA'].isin(Maestro2['Cod_Barra']) == True]
c = devoluciones_activa[devoluciones_activa['CODIGO_BARRA'].isin(Maestro['Cod_Barra']) == True]
d = devoluciones_activa[devoluciones_entrada['CODIGO_BARRA'].isin(Maestro['Cod_Barra']) == True]
e = Maestro2[Maestro2['Cod_Barra'].isin(devoluciones_entrada['CODIGO_BARRA']) == False]
f = Maestro[Maestro['Cod_Barra'].isin(a['CODIGO_BARRA']) == True]


#%%

devoluciones['FECHA_LOTE_RADICADO'] = pd.to_datetime(devoluciones['FECHA_LOTE_RADICADO'], format = '%d/%m/%Y')

#%%

Maestro_salud = Maestro[(Maestro['Regimen'].astype(str).str.upper().str.contains('MPP') == True) | (Maestro['Regimen'].astype(str).str.upper().str.contains('HYC') == True)]
estados = ['EN AUDITORIA: En espera de ser asignada a Auditor','AUDITADA: Factura Anulada','EN RADICACION: Radicado, pendiente de completar carga','EN RADICACION: Revision Calidad de Radicación']
Maestro_salud = Maestro_salud[Maestro_salud['Estado_actual'].isin(estados) == False]
Maestro_salud = Maestro_salud.drop_duplicates('Cod_Barra')
Maestro_salud['Año_Radicacion'] = pd.to_datetime(Maestro_salud['Fecha_Radicacion'], format = '%d/%m/%Y')
Maestro_salud['Año_Radicacion'] = Maestro_salud['Año_Radicacion'].dt.year
Maestro_salud = Maestro_salud[Maestro_salud['Año_Radicacion'].isin([2020,2021,2022,2023]) == True]
#%%

lista_NITS = list(devoluciones['NIT_IPS'].astype(str).unique())

print('Existen ', len(lista_NITS), ' NITs diferentes en devoluciones')

c = 1
b = len(lista_NITS)
for i in lista_NITS:
    d = str(c) + '.'
    print(d, ' Filtranddo información para el NIT: ',i)
    df = devoluciones[devoluciones['NIT_IPS'].astype(str) == str(i)]
    print('    Guardando información para el NIT: ',i)
    df.to_excel(path_salida + '/' + str(i) + '_d.xlsx', index = False)
    print('    Información guardada para el NIT: ',i, '\n')
    b -=1
    print(' Quedan ',b, ' archivos por guardar \n')
    c += 1
