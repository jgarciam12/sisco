# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 15:32:59 2023

@author: jcgarciam
"""

import pandas as pd
import os
from datetime import datetime

fecha = datetime.now().date()

ruth = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\SALUD\CONSOLIDADO_SISCO'
ruth2 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\ARL\CONSOLIDADO_SISCO'
path_salida = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\25-Historico_Pagos'

dic = {}
texto = {'Numero_Factura_Original':str,'NIT':str,'Numero_OP':str,'Cod_Barra':str,
         'Cod_Barra_Pago_Parcial':str,'Fac_Norint':str}
for i in os.listdir(ruth):
    print('Leyendo archivo: ', i)
    df = pd.read_csv(ruth + '/' + i, sep = '|', dtype = texto, encoding = 'ANSI')
    df['Oriden_S'] = 'Salud'
    dic[i] = df
    
    
for i in os.listdir(ruth2):
    print('Leyendo archivo: ', i)
    df = pd.read_csv(ruth2 + '/' + i, sep = '|', dtype = texto, encoding = 'ANSI')
    df['Oriden_S'] = 'ARL'
    dic[i] = df
    
#%%

sisco = pd.concat(dic).reset_index(drop = True)

#%%

sisco = sisco[sisco['Estado_Pago'].astype(str).str.replace(',0','').str.replace('.0','') == '7']
#%%
import numpy as np

def CambioFormato(df, a = 'a'):
    df[a] = df[a].astype(str).str.strip().str.strip('\x02').str.strip('')
    df[a] = np.where(df[a].str[-2::] == '.0', df[a].str[0:-2], df[a])
    df[a] = np.where(df[a] == 'nan', np.nan, df[a])
    return df[a]
#%%
sisco['Cod_Barra'] = CambioFormato(sisco, a = 'Cod_Barra')
sisco['Cod_Barra_Pago_Parcial'] = CambioFormato(sisco, a = 'Cod_Barra_Pago_Parcial')
sisco['Numero_OP'] = CambioFormato(sisco, a = 'Numero_OP')
sisco['NIT'] = CambioFormato(sisco, a = 'NIT')
sisco['Fac_Norint'] = CambioFormato(sisco, a = 'Fac_Norint')
sisco['Fecha_Radicacion'] = pd.to_datetime(sisco['Fecha_Radicacion'], format = '%d/%m/%Y')
sisco['Fecha_OP'] = pd.to_datetime(sisco['Fecha_OP'], format = '%d/%m/%Y')
sisco['Fecha de causacion'] = pd.to_datetime(sisco['Fecha de causacion'], format = '%d/%m/%Y')

valores = ['Valor_OP','Valor_Bruto','Valor_Pagado_Egresos','IVA','Rete_ICA','Rete_IVA',
           'Rete_Fuente','Valor_Copago','Valor_Iva']

for i in valores:
    sisco[i] = sisco[i].astype(str).str.replace(',','.').astype(float)

#%%
sisco_salud = sisco[sisco['Oriden_S'] == 'Salud']
sisco_salud['IVA'] = np.where(((sisco_salud['Valor_Iva'] == 0) &
                              (sisco_salud['Regimen'].str.upper().str.contains('HYC') == True)),
                              0, sisco_salud['IVA'])

sisco_arl = sisco[sisco['Oriden_S'] == 'ARL']
sisco_arl.loc[sisco_arl['Cod_Barra'] == 'RDF00000999066','Fac_Norint'] = '20772958'
sisco_arl.loc[sisco_arl['Cod_Barra'] == 'RDF00001017373','Fac_Norint'] = '20858490'
sisco_arl.loc[sisco_arl['Cod_Barra'] == 'RDF00001018793','Fac_Norint'] = '20857367'

#%%

sisco_arl = sisco_arl.sort_values(['Fecha de causacion','Fecha_OP'], ascending = False)
sisco_arl['Cod_Barra'] = np.where(sisco_arl['Cod_Barra_Pago_Parcial'].isnull() == False,sisco_arl['Cod_Barra_Pago_Parcial'],sisco_arl['Cod_Barra'])
agrupaciones = {'Numero_Factura_Original':'first','NIT':'first','Razon_Social':'first',
                'Valor_OP':'sum','Numero_OP':'first','Fecha_OP':'first','Valor_Bruto':'first',
                'Valor_Pagado_Egresos':'sum','IVA':'sum','Rete_ICA':'sum','Rete_IVA':'sum',
                'Rete_Fuente':'sum','Regimen':'first','Fecha de causacion':'first',
                'Valor_Copago':'last','Fac_Norint':'first','Fecha_Radicacion':'last'}
sisco_arl = sisco_arl.groupby('Cod_Barra', as_index = False).agg(agrupaciones)
#%%

sisco = pd.concat([sisco_salud, sisco_arl])
#%%

sisco = sisco[sisco['Fecha_Radicacion'].dt.year >= 2021]
sisco['CODIGO DE BARRAS'] = ''
sisco['CODIGO DE BARRAS'] = np.where(sisco['Origen'] == 'Pagos Parciales',sisco['Cod_Barra_Pago_Parcial'],sisco['Cod_Barra'])


sisco['RAZON SOCIAL PAGADOR'] = np.where(sisco['Regimen'].astype(str).str.upper().str.contains('MPP') == True,'AXA COLPATRIA MEDICINA PREPAGADA S.A','AXA COLPATRIA SEGUROS DE VIDA S.A')
sisco['RAZON SOCIAL PAGADOR'] = sisco['RAZON SOCIAL PAGADOR'].str[0:30]
sisco['FECHA ENVIO'] = fecha.strftime('%Y') + fecha.strftime('%m') + fecha.strftime('%d')
sisco['FECHA DE PAGO'] = sisco['Fecha_OP'].copy()
sisco2 = sisco[['Numero_Factura_Original','NIT','Razon_Social','RAZON SOCIAL PAGADOR',
                            'Numero_OP','Fecha_OP','Valor_Bruto','Valor_Pagado_Egresos',
                            'IVA','Rete_ICA','Rete_IVA','Rete_Fuente','Regimen','FECHA DE PAGO',
                            'Fecha de causacion','Valor_Copago','CODIGO DE BARRAS','Fac_Norint',
                            'FECHA ENVIO','Fecha_Radicacion']].copy()


fechas = ['FECHA DE PAGO','Fecha de causacion','Fecha_OP']

for i in fechas:
    print(i)
    sisco2[i] = sisco2[i].dt.strftime('%Y%m%d')

sisco2 = sisco2.rename(columns = {'Numero_Factura_Original':'NUMERO DE FACTURA',
            'NIT':'NIT DEL PRESTADOR','Razon_Social':'RAZON SOCIAL PRESTADOR',
            'Numero_OP':'NUMERO DE OP','Fecha_OP':'FECHA DE OP','Valor_Bruto':'VALOR BRUTO',
            'Valor_Pagado_Egresos':'VALOR PAGADO','Rete_ICA':'RETE ICA','Rete_IVA':'RETE IVA',
            'Rete_Fuente':'RETE FUENTE','Regimen':'ORIGEN (SALUD/ARL)','Fecha de causacion':'FECHA DE CAUSACION',
            'Valor_Copago':'VALOR BONOS','Fac_Norint':'FACTNROINT','Valor_OP':'VALOR DE OP'})

sisco2['RAZON SOCIAL PRESTADOR'] = sisco2['RAZON SOCIAL PRESTADOR'].str[0:30]
sisco2['NIT DEL PRESTADOR'] = sisco2['NIT DEL PRESTADOR'].str[0:11]
sisco2['ORIGEN (SALUD/ARL)'] = np.where(sisco2['ORIGEN (SALUD/ARL)'].astype(str).str.upper().str.contains('ARL') == True,'ARL','SALUD')
#%%
lista_numeros = ['VALOR BRUTO','VALOR PAGADO','IVA','RETE ICA',
                 'RETE IVA','RETE FUENTE','VALOR BONOS']

for i in lista_numeros:
    sisco2[i] = CambioFormato(sisco2, a = i)

#%%
sisco2['Mes Radicado'] = sisco2['Fecha_Radicacion'].dt.month
sisco2['Semestre Radicado'] = np.where(sisco2['Mes Radicado'] <= 6,1,2)

#%%
anios = [2021,2022,2023,2024]
periodos = [1,2]

for i in anios:
    print(i)
    for j in periodos:
        print(j)
        df = sisco2[sisco2['Fecha_Radicacion'].dt.year == i]
        df = df[df['Semestre Radicado'] == j]
        df = df[df['FECHA DE CAUSACION'].isnull() == False]
        df = df.drop(columns = ['Fecha_Radicacion','Semestre Radicado','Mes Radicado'])
        df.to_csv(path_salida + r'/Pagos_radicacion_' + str(i) + '-' + str(j)+ '_' +fecha.strftime('%d') + fecha.strftime('%m') + fecha.strftime('%Y') +'.csv', index = False, sep = ';', encoding = 'ANSI')
        
