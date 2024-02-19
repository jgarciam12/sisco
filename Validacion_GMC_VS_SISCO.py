# -*- coding: utf-8 -*-
"""
Created on Tue Aug  1 10:30:12 2023

@author: jcgarciam
"""

import pandas as pd

gm_enero = pd.read_excel(r'\\dc1pvfnas1\Autos\Soat_Y_ARL\Pagos_Arl_Salud\Cierre\Años_anteriores\GM_Activa\GMC Mejorado MPP y HYC 2024/GM HYC PRUEBA ENERO 2024.xlsx')#, sheet_name= 'Gasto médico')

#%%
import glob
path2 = r'C:\SISCO\Cierre Salud Enero 2024'
dic = {}

print('\n Inicio de carga de archivos SISCO Salud \n')

for Archivo in glob.glob(path2 + '\*csv'):
    print('Cargando archivo ', Archivo[-20:-4])
    df = pd.read_csv(Archivo, sep = '|', header = 0)
    df['Origen'] = 'Salud'
    print('Archivo ', Archivo[-20:-4], ' cargado \n')
    dic[Archivo] = df
    
sisco = pd.concat(dic).reset_index(drop = True)

#%%
import numpy as np

def CambioFormato(df, a = 'a'):
    df[a] = df[a].astype(str).str.strip().str.strip('\x02').str.strip('')
    df[a] = np.where(df[a].str[-2::] == '.0', df[a].str[0:-2], df[a])
    df[a] = np.where(df[a] == 'nan', np.nan, df[a])
    return df[a]

#%%

gm_enero['Folio'] = CambioFormato(gm_enero, a = 'Folio')
#%%
gm_enero2 = gm_enero.groupby('Folio', as_index = False)['Valor Hospital','Valor Cheques Hospital'].sum()

#%%

sisco['Cod_Barra'] = CambioFormato(sisco, a = 'Cod_Barra')
sisco['Cod_Barra_Pago_Parcial'] = CambioFormato(sisco, a = 'Cod_Barra_Pago_Parcial')

#%%

sisco_a = sisco[sisco['Cod_Barra'].isin(gm_enero2['Folio']) == True]
sisco_a['Folio'] = sisco_a['Cod_Barra']
sisco_b = sisco[sisco['Cod_Barra_Pago_Parcial'].isin(gm_enero2['Folio'])]
sisco_b['Folio'] = sisco_b['Cod_Barra_Pago_Parcial']
sisco2 = pd.concat([sisco_a,sisco_b]).reset_index(drop = True)
#%%
sisco3 = sisco2.copy()
sisco3['Fecha de causacion'] = pd.to_datetime(sisco3['Fecha de causacion'], format = '%d/%m/%Y')
sisco3['Valor_OP'] = sisco3['Valor_OP'].astype(str).str.replace(',','.').astype(float)
sisco3['Valor_Copago'] = sisco3['Valor_Copago'].astype(str).str.replace(',','.').astype(float)

sisco3 =  sisco3.groupby('Folio', as_index = False).agg({'Valor_OP':'sum','Valor_Copago':'sum','Fecha de causacion':'last'})
sisco3['Cruce'] = 'Si'

gm_enero3 = gm_enero2.merge(sisco3, how = 'left', on = 'Folio')

#%%

gm_enero3['Diferencias1'] = gm_enero3['Valor Hospital'] - gm_enero3['Valor_OP']
gm_enero3['Diferencias2'] = gm_enero3['Valor Cheques Hospital'] - gm_enero3['Valor_Copago']
gm_enero3['Diferencias Total Valor Hospital VS Total Valor_OP'] = np.nan
gm_enero3.loc[(gm_enero3['Diferencias1'] == 0) & (gm_enero3['Cruce'] == 'Si'),'Diferencias Total Valor Hospital VS Total Valor_OP'] = 'No'
gm_enero3.loc[(gm_enero3['Diferencias1'] != 0) & (gm_enero3['Cruce'] == 'Si'),'Diferencias Total Valor Hospital VS Total Valor_OP'] = 'Si'
gm_enero3 = gm_enero3.rename(columns = {'Valor_OP':'Total Valor_OP',
                                                'Valor_Copago':'Total Valor_Copago',
                                                'Valor Hospital':'Total Valor Hospital',
                                                'Valor Cheques Hospital':'Total Valor Cheques Hospital'})


#%%
gm_enero3.to_excel(r'D:\DATOS\Users\jcgarciam\Downloads/comparacion_sisco_vs_gmc_HYC_enero_2024.xlsx', index = False)