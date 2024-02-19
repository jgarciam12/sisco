

# -*- coding: utf-8 -*-
"""
Created on Fri Nov 17 15:05:35 2023

@author: jcgarciam
"""

import pandas as pd
import glob
import numpy as np
from datetime import datetime

now = datetime.now()

path_int1 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\SALUD'
path_int2 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\ARL'

path_int3 = r'D:\DATOS\Users\jcgarciam\OneDrive - AXA Colpatria Seguros\Documentos\Informes\SISCO\Notificaciones DIAN\Input'
path_salida = r'D:\DATOS\Users\jcgarciam\OneDrive - AXA Colpatria Seguros\Documentos\Informes\SISCO\Notificaciones DIAN\Output'

#%%
columnas = ['Fecha_Radicacion','Valor_Neto','Total Valor Pagado','NIT','Numero_Factura','Regimen']

print('Cargando Maestro Salud')
Maestro_salud = pd.read_csv(path_int1 + '/Maestro_Salud.csv', sep = '|', usecols = columnas, encoding = 'ANSI')
print('Maestro Saludo cargado \n')

print('Cargando Maestro ARL')
Maestro_arl = pd.read_csv(path_int2 + '/Maestro_ARL.csv', sep = '|', usecols = columnas, encoding = 'ANSI')
print('Maestro ARLL cargado \n')

sisco_agrupado = pd.concat([Maestro_salud,Maestro_arl]).reset_index(drop = True)
#%%

dic = {}

for i in glob.glob(path_int3 + '/Acuses_*'):
    print('Leyendo el archivo: ', i[len(path_int3) + 1::])
    df = pd.read_csv(i, sep = ',', usecols = ['CUFE'])
    dic[i] = df
    print('Archivo ', i[len(path_int3) + 1::], ' leído\n')
    
acuses = pd.concat(dic).reset_index(drop = True)
#%%
dic = {}

for i in glob.glob(path_int3 + '/Recibidas_*'):
    print('Leyendo el archivo: ', i[len(path_int3) + 1::])
    df = pd.read_csv(i, sep = ',')
    df['Origen'] = i[len(path_int3) + 1::]
    dic[i] = df
    print('Archivo ', i[len(path_int3) + 1::], ' leído\n')
    
recibidas = pd.concat(dic).reset_index(drop = True)

#%%

acuses['Acuses'] = 'Si'
acuses = acuses.rename(columns = {'CUFE':'Cufe'})
acuses['Cufe'] = acuses['Cufe'].astype(str)
recibidas['Cufe'] = recibidas['Cufe'].astype(str)

recibidas2 = recibidas.merge(acuses, how = 'inner', on = 'Cufe')
#%%
sisco_agrupado['Valor_Neto'] = sisco_agrupado['Valor_Neto'].astype(str).str.replace(',','.').astype(float)
sisco_agrupado['Total Valor Pagado'] = sisco_agrupado['Total Valor Pagado'].astype(str).str.replace(',','.').astype(float)

sisco_agrupado['Porc Diferencias'] = (sisco_agrupado['Valor_Neto'] - sisco_agrupado['Total Valor Pagado'])/sisco_agrupado['Valor_Neto']

#%%

sisco_agrupado2 = sisco_agrupado.copy()
sisco_agrupado2['Fecha_Radicacion'] =  pd.to_datetime(sisco_agrupado2['Fecha_Radicacion'], format = '%Y/%m/%d')
sisco_agrupado2 = sisco_agrupado2[sisco_agrupado2['Fecha_Radicacion'].dt.year >= 2023]
sisco_agrupado2 = sisco_agrupado2[sisco_agrupado2['Valor_Neto'].abs() > 2]
#%%
sisco_agrupado2['Facturas Pagas Total'] = np.nan

sisco_agrupado2['Facturas Pagas Total'] = np.where(sisco_agrupado2['Porc Diferencias'].abs() <= 0.01, 'Si', 'No')


#%%
def CambioFormato(df, a = 'a'):
    df[a] = df[a].astype(str)
    df[a] = np.where(df[a].str[-2::] == '.0', df[a].str[0:-2], df[a])
    df.loc[(df[a].str.contains('nan') == True),a] = np.nan

    return df[a]

sisco_agrupado3 = sisco_agrupado2[sisco_agrupado2['Facturas Pagas Total'] == 'Si']

sisco_agrupado3 = sisco_agrupado3[['NIT','Numero_Factura','Facturas Pagas Total','Regimen','Total Valor Pagado']]

sisco_agrupado3['NIT'] = CambioFormato(sisco_agrupado3, a = 'NIT')
sisco_agrupado3['Numero_Factura'] = CambioFormato(sisco_agrupado3, a = 'Numero_Factura')

sisco_agrupado3 = sisco_agrupado3[sisco_agrupado3['NIT'].isnull() == False]
sisco_agrupado3 = sisco_agrupado3[sisco_agrupado3['Numero_Factura'].isnull() == False]

sisco_agrupado3 = sisco_agrupado3.rename(columns = {'NIT':'ID Proveedor','Numero_Factura':'Número Documento'})

recibidas2['ID Proveedor'] = CambioFormato(recibidas2, a = 'ID Proveedor')
recibidas2['Número Documento'] = CambioFormato(recibidas2, a = 'Número Documento')

#%%
recibidas3 = recibidas2.merge(sisco_agrupado3, how = 'inner', on = ['ID Proveedor','Número Documento'], validate  = 'many_to_one')
recibidas3 = recibidas3.rename(columns = {'Número Documento':'Numero Documento'})
recibidas3['ID Cliente'] = CambioFormato(recibidas3, a = 'ID Cliente')
recibidas3['ID Cliente'] = recibidas3['ID Cliente'].str.strip('\ufeff').str.strip('"')
recibidas3['Comentario'] = np.nan
recibidas3['Estado documento'] = 'Aceptado'
#%%

for i in list(recibidas3['Origen'].unique()):
    print('\nGuardando archivo para ',i)
    df = recibidas3[recibidas3['Origen'] == i].reset_index(drop = True)
    print('\n    La base',i,'tiene', str(len(df)),'registros')
    
    cant = 2448
    a = len(df) % cant
    b = int((len(df) - a) / cant)
    if a > 0:
        b = b + 1
    print('    Se van a guardar',b,'archivos para',i,'\n')

    k = 0
    cant2 = cant
    
    for j in range(b):
            
        df2  = df.loc[k:cant2].copy()
        print('    Archivo',j,':','empieza en', k, 'y termina en',str(df2.index.max()) + '.','Tamaño:', df2.shape[0])
        k = cant2 + 1
        cant2 = cant + k

        df2 = df2.reset_index(drop = True)
        df2 = df2.reset_index()
        df2['index'] = df2['index'] + 1
        df2 = df2.rename(columns = {'index':'identificador'})
        df2 = df2[['identificador','ID Cliente','ID Proveedor','Tipo Documento','Numero Documento','Cufe','Comentario','Estado documento','Origen']]
        df2 = df2.drop(columns = ['Origen'])     
        df2.to_csv(path_salida + '/Salida ' + i[0:-4] + '_' + str(j) + '.csv', index = False, sep = ';', encoding = 'ANSI')

print('\n')
print('Proceso finalizado')
print('Tiempo de duración: ', datetime.now() - now)
