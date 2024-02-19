# -*- coding: utf-8 -*-
"""
Created on Thu Sep 15 14:58:57 2022

@author: jcgarciam

"""

import pandas as pd
import glob
from datetime import datetime
import numpy as np

Tiempo_Total = datetime.now()

path_entrada1 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\SALUD\CONSOLIDADO_SISCO'
path_entrada2 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\ARL\CONSOLIDADO_SISCO'
path_salida_arl = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\ARL\INTEGRIDAD'
path_salida_salud = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\SALUD\INTEGRIDAD'


dic_salud = {}
dic_ARL = {}
  
print('\n Inicio de carga de archivos SISCO Salud \n')
for Archivo in glob.glob(path_entrada1 + '\*csv'):
    if int(Archivo[-8:-4]) >= 2020:
        print('\n Cargando archivo ', Archivo[-20:-4])
        df = pd.read_csv(Archivo, sep = '|', header = 0)
        print('\n Archivo ', Archivo[-20:-4], ' cargado \n')
        dic_salud[Archivo] = df

print('\n Inicio de carga de archivos SISCO ARL \n')    
for Archivo in glob.glob(path_entrada2 + '\*csv'):
    if int(Archivo[-8:-4]) >= 2020:
        print('\n Cargando archivo ', Archivo[-18:-4])
        df = pd.read_csv(Archivo, sep = '|', header = 0)
        print('\n Archivo ', Archivo[-18:-4], ' cargado \n')
        dic_ARL[Archivo] = df
    
  
#%%

print('Concatenando archivos de SISCO Salud ')
salud = pd.concat(dic_salud).reset_index(drop = True)
print('Archisvos de SISCO Salud concatenados \n')

print('Concatenando archivos de SISCO ARL ')
arl = pd.concat(dic_ARL).reset_index(drop = True)
print('Archisvos de SISCO ARL concatenados \n')


#%%
lista_salud = ['Numero_Factura_Original','Estado_actual','Cod_Barra','Valor_Glosa','Fac_Norint','Folio_BH','Estado_RR',
         'Numero_OP','Fecha_OP','Valor_Pagado_Egresos','Valor_OP','IVA','Rete_Fuente','Rete_ICA','Rete_IVA',
         'Estado_Pago','Regimen','Fecha_Radicacion','Valor_Bruto','Valor_Neto','NIT','Razon_Social','Fecha_Ultimo_estado']


nuevos_nombres_salud = ['Numero_Factura','Estado_actual','PrimeroDeCod_Barra','Valor_Glosa','PrimeroDeFac_Norint',	
                  'PrimeroDeCONSECUTIVO','ESTADO','PrimeroDeNUORPA','FEESOP','SumaDeVANEPG','SumaDeVAORPA',	
                  'SumaDeIVA','SumaDeRTE FUENTE','SumaDeRTE ICA','SumaDeIMP IVA','PrimeroDeESTOPG','Regimen',	
                  'Fecha_Radicacion','Valor_Bruto','Valor_Neto','NIT','Razon_Social','Fecha_Ultimo_estado']

lista_arl = ['Numero_Factura_Original','Estado_actual','Cod_Barra','Valor_Glosa','Fac_Norint','Radicado','Estado_Asitenciales',
         'Numero_OP','Fecha_OP','Valor_Pagado_Egresos','Valor_OP','IVA','Rete_Fuente','Rete_ICA','Rete_IVA',
         'Estado_Pago','Regimen','Fecha_Radicacion','Valor_Bruto','Valor_Neto','NIT','Razon_Social','Fecha_Ultimo_estado']


nuevos_nombres_arl = ['Numero_Factura','Estado_actual','PrimeroDeCod_Barra','Valor_Glosa','PrimeroDeFac_Norint',
                        'PrimeroDeRDA','PrimeroDeRAAEST','PrimeroDeNUORPA','FEESOP','SumaDeVANEPG','SumaDeVAORPA',	
                        'SumaDeIVA','SumaDeRTE FUENTE','SumaDeRTE ICA','SumaDeIMP IVA','PrimeroDeESTOPG','Regimen',	
                        'Fecha_Radicacion','Valor_Bruto','Valor_Neto','NIT','Razon_Social','Fecha_Ultimo_estado']
#%%

def tablasfinales(df, lista, nombres):
    lista_2 = ['Valor_Glosa','Numero_OP','Valor_Pagado_Egresos','Valor_OP','IVA','Rete_Fuente',
             'Rete_ICA','Rete_IVA','Estado_Pago','Valor_Bruto','Valor_Neto']
    for i in lista_2:
        print('Cambiando formato del campo ', i, 'a formato entero')
        df[i] = df[i].astype(str)
        df[i] = df[i].str.replace(',','.')
        df[i] = df[i].fillna(0.0)
        df[i] = np.where(df[i].str[-2::] == '.0', df[i].str[0:-2], df[i])
        print('Formato cambiado para el campo ', i)

        
    df = df[lista]
    df.columns = nombres
    
    return df
    

salud = tablasfinales(salud, lista = lista_salud, nombres = nuevos_nombres_salud)
arl = tablasfinales(arl, lista = lista_arl, nombres = nuevos_nombres_arl)
#%%
arl['PrimeroDeRAAEST'] = arl['PrimeroDeRAAEST'].astype(str)
print('\n Cambiando formato del campo PrimeroDeRAAEST a formato entero')
arl['PrimeroDeRAAEST'] = np.where(arl['PrimeroDeRAAEST'].str[-2::] == ',0',arl['PrimeroDeRAAEST'].str[0:-2],arl['PrimeroDeRAAEST'])
print('Formato cambiado para el campo PrimeroDeRAAEST')

salud['PrimeroDeCod_Barra'] = salud['PrimeroDeCod_Barra'].astype(str)
print('\n Cambiando formato del campo PrimeroDeCod_Barra a formato entero')
salud['PrimeroDeCod_Barra'] = np.where(salud['PrimeroDeCod_Barra'].str[-2::] == '.0',salud['PrimeroDeCod_Barra'].str[0:-2],salud['PrimeroDeCod_Barra'])
print('Formato cambiado para el campo PrimeroDeCod_Barra \n')


#%%

print('Guardando archivo de integridad para ARL')
arl.to_csv(path_salida_arl + '/arl.csv', index = False, sep = '\t')
print('Archivo guardado integridad para ARL \n')

print('Guardando archivo de integridad para Salud')
salud.to_csv(path_salida_salud + '\salud.csv', index = False, sep = '\t')
print('Archivo guardado integridad para Salud \n')


print('Proceso finalizado')
print("Tiempo del Proceso: " , datetime.now()-Tiempo_Total)
