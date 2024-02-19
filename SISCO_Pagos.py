# -*- coding: utf-8 -*-
"""
Created on Fri Sep 16 08:12:56 2022

@author: jcgarciam
"""

import pandas as pd
import glob
from datetime import datetime
import numpy as np

Tiempo_Total = datetime.now()

path_entrada1 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\SALUD\CONSOLIDADO_SISCO'
path_entrada2 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\ARL\CONSOLIDADO_SISCO'
path_salida = r'D:\DATOS\Users\jcgarciam\OneDrive - AXA Colpatria Seguros\Documentos\Informes\SISCO\Output'

dic = {}

columnas = ['Numero_Factura_Original','NIT','Razon_Social','Numero_OP','Fecha_OP','Valor_Pagado_Egresos',
             'Valor_Bruto','Valor_OP','IVA','Rete_Fuente','Rete_ICA','Rete_IVA','Regimen']

print('\n Inicio de carga de archivos SISCO Salud \n')
for Archivo in glob.glob(path_entrada1 + '\*csv'):
    print('Cargando archivo ', Archivo[-20:-4])
    df = pd.read_csv(Archivo, sep = '|', header = 0)#, usecols = columnas)
    df['Origen'] = 'Salud'
    print('Archivo ', Archivo[-20:-4], ' cargado \n')
    dic[Archivo] = df
    
for Archivo in glob.glob(path_entrada2 + '\*csv'):
    print('Cargando archivo ', Archivo[-18:-4])
    df = pd.read_csv(Archivo, sep = '|', header = 0)#, usecols = columnas)
    df['Origen'] = 'ARL'
    print('Archivo ', Archivo[-18:-4], ' cargado \n')
    dic[Archivo] = df
    

sisco = pd.concat(dic).reset_index(drop = True)
#%%   

print('\n SISCO cargado completamente')
print("\n Tiempo del proceso de carga: " , datetime.now()-Tiempo_Total)

sisco['Numero_OP'] = sisco['Numero_OP'].astype(str)
sisco['Numero_OP'] = np.where(sisco['Numero_OP'].str[-2::] == '.0', sisco['Numero_OP'].str[0:-2], sisco['Numero_OP'])

sisco = sisco[sisco['Fecha_OP'].isnull() == False]
sisco['Fecha_OP'] = pd.to_datetime(sisco['Fecha_OP'], format = '%d/%m/%Y')


#%%

Fecha_Inicial = input('\n Ingrese la Fecha OP incial de descarga, yyyy-mm-dd: ')
Fecha_Final = input('\n Ingrese la Fecha OP final de descarga, yyyy-mm-dd: ')

sisco = sisco[sisco['Fecha_OP'].between(Fecha_Inicial, Fecha_Final) == True]

sisco['NIT'] = sisco['NIT'].astype(str)
sisco = sisco.rename( columns = {'Valor_Pagado_Egresos':'Valor_Pagado','Numero_Factura_Original':'Numero_Factura'})

#%%
sisco['Numero_Factura'] = sisco['Numero_Factura'].str.strip('\x02')

columnas_numericas =['Valor_Bruto','Valor_Pagado','IVA','Rete_Fuente','Rete_ICA', 'Rete_IVA','Valor_OP']

for i in columnas_numericas:
    sisco[i] = sisco[i].astype(str)
    sisco[i] = sisco[i].str.replace(',','.')
    sisco[i] = sisco[i].astype(float)
    
sisco['Fecha_OP'] = sisco['Fecha_OP'].dt.strftime('%d/%m/%Y')

#%%

lista_NITS = list(sisco['NIT'].astype(str).unique())

print('Existen ', len(lista_NITS), ' NITs diferentes en Pagos')

c = 1
b = len(lista_NITS)

dic = {}
for i in lista_NITS:
    d = str(c) + '.'
    print(d, ' filtranddo información para el NIT: ',i)
    df = sisco[sisco['NIT'].astype(str) == str(i)]
    dic[i] = df
    print('    Guardando información para el NIT: ',i)
    df.to_excel(path_salida + '/' + str(i) + '.xlsx', index = False)
    print('    Información guardada para el NIT: ',i, '\n')
    b -= 1
    print(' Quedan ',b, ' archivos por guardar \n')
    c += 1
    
nits = {'NIT':lista_NITS}
nits = pd.DataFrame(nits)
nits.to_excel(path_salida + '/NITs.xlsx', index = False)
    
    
print('fin del proceso')    
print(len(lista_NITS), ' NITs diferentes guardados')
print("Tiempo final del Proceso: " , datetime.now()-Tiempo_Total)

