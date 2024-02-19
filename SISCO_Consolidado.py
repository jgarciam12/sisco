# -*- coding: utf-8 -*-
"""
Created on Tue Sep 20 11:08:20 2022

@author: jcgarciam
"""

import pandas as pd
import glob
from datetime import datetime

Tiempo_Total = datetime.now()

path_entrada1 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\SALUD\CONSOLIDADO_SISCO'
path_entrada2 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\ARL\CONSOLIDADO_SISCO'
path_salida = r'D:\DATOS\Users\jcgarciam\AXA Colpatria Seguros\SHAREPOINT ARL Y SALUD - SISCO\Consolidado_facturas'

dic = {}

columnas = ['Numero_Factura_Original','NIT','Razon_Social','Numero_OP','Fecha_OP','Valor_Pagado_Egresos',
             'Valor_Bruto','Valor_OP','IVA','Rete_Fuente','Rete_ICA','Rete_IVA','Regimen','Codigo_Glosa',
             'Observacion','Estado_actual','Valor_Glosa'] #'Estado_Egresos',

print('\n Inicio de carga de archivos SISCO Salud \n')
for Archivo in glob.glob(path_entrada1 + '\*csv'):
    print('\n Cargando archivo ', Archivo[-20:-4])
    df = pd.read_csv(Archivo, sep = '|', header = 0, usecols = columnas)
    df['Origen'] = 'Salud'
    print('\n Archivo ', Archivo[-20:-4], ' cargado \n')
    dic[Archivo] = df
    
for Archivo in glob.glob(path_entrada2 + '\*csv'):
    print('\n Cargando archivo ', Archivo[-18:-4])
    df = pd.read_csv(Archivo, sep = '|', header = 0, usecols = columnas)
    df['Origen'] = 'ARL'
    print('\n Archivo ', Archivo[-18:-4], ' cargado \n')
    dic[Archivo] = df
    
#%%
   

SISCO = pd.concat(dic).reset_index(drop = True)


#%%
print('\n SISCO Salud cargado completamente')
print("\n Tiempo del proceso de carga: " , datetime.now()-Tiempo_Total)

SISCO['NIT'] = SISCO['NIT'].astype(str)

SISCO['Fecha_OP'] = pd.to_datetime(SISCO['Fecha_OP'], format = '%d/%m/%Y')
SISCO['Fecha_OP'] = SISCO['Fecha_OP'].dt.strftime('%d/%m/%Y')

SISCO = SISCO.rename( columns = {'Valor_Pagado_Egresos':'Valor_Pagado','Numero_Factura_Original':'Numero_Factura'})


columnas_numericas =['Valor_Bruto','Valor_Pagado','IVA','Rete_Fuente','Rete_ICA', 'Rete_IVA','Numero_OP','Valor_OP']

for i in columnas_numericas:
    SISCO[i] = SISCO[i].str.replace(',','.')
    SISCO[i] = SISCO[i].astype(float)

#%%


lista_NITS = list(SISCO['NIT'].astype(str).unique())

#%%

print('Existen ', len(lista_NITS), ' NITs diferentes en devoluciones')

c = 1
b = len(lista_NITS)

for i in lista_NITS:
    d = str(c) + '.'
    print(d, ' Filtranddo información para el NIT: ',i)
    df = SISCO[SISCO['NIT'].astype(str) == str(i)]
    print('    Guardando información para el NIT: ',i)
    df.to_excel(path_salida + '/' + str(i) + '.xlsx', index = False)
    print('    Información guardada para el NIT: ',i, '\n')
    b -= 1
    print('  Quedan ', b, ' archivos por guardar \n')
    c += 1
    
    
print('fin del proceso')    
print(len(lista_NITS), ' NITs diferentes guardados')
print("Tiempo final del Proceso: " , datetime.now()-Tiempo_Total)