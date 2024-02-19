# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 08:12:37 2023

@author: jcgarciam
"""

import pandas as pd

path1 = r'\\dc1pvfnas1\Autos\Soat_Y_ARL\Backup_Brian\Cierre\Años_anteriores\GM_Activa\GMC Mejorado MPP y HYC 2024'

gm_mpp_enro = pd.read_excel(path1 + '/GM MPP PRUEBA ENERO 2024.xlsx')

#%%

path2 = r'\\dc1pvfnas1\Autos\Soat_Y_ARL\Backup_Brian\Cierre\Años_anteriores\GM_Activa'

gm_2023 = pd.read_csv(path2 + '/GM MPP MEJORADO 2023.csv', sep = '|')

#%%

gm_2023['Fecha Contabilizacion'] = pd.to_datetime(gm_2023['Fecha Contabilizacion'], format = '%Y-%m-%d')
gm_2023['Fecha Servicio'] = pd.to_datetime(gm_2023['Fecha Servicio'], format = '%Y-%m-%d')
gm_2023['Fecha Radicacion'] = pd.to_datetime(gm_2023['Fecha Radicacion'], format = '%Y-%m-%d')
gm_2023['FECHA_RAD_FACT'] = pd.to_datetime(gm_2023['FECHA_RAD_FACT'], format = '%Y-%m-%d')

#%%

gm_2023_2 = pd.concat([gm_mpp_mayo,gm_2023]).reset_index(drop = True)

#gm_2023_2['valor neto'] = gm_2023_2['valor neto'].astype(int)

#%%

#gm_2023_2.to_excel(r'D:\DATOS\Users\jcgarciam\Downloads/gmc_mpp_2023.xlsx', index = False)

gm_2023_2['Fecha Contabilizacion'] = gm_2023_2['Fecha Contabilizacion'].dt.strftime('%Y-%m-%d')
gm_2023_2['Fecha Servicio'] = gm_2023_2['Fecha Servicio'].dt.strftime('%Y-%m-%d')
gm_2023_2['Fecha Radicacion'] = gm_2023_2['Fecha Radicacion'].dt.strftime('%Y-%m-%d')
gm_2023_2['FECHA_RAD_FACT'] = gm_2023_2['FECHA_RAD_FACT'].dt.strftime('%Y-%m-%d')

#%%
gm_2023_2.to_csv(path2 + '/GM MPP MEJORADO 2023.csv', sep = '|', index = False)

