# -*- coding: utf-8 -*-
"""
Created on Wed Nov 15 10:46:05 2023

@author: jcgarciam
"""

import pandas as pd
import os

path1  = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\ARL\CONSOLIDADO_SISCO'
path2 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\SALUD\CONSOLIDADO_SISCO'

dic = {}

for i in os.listdir(path1):
    print(i)
    dic[i] = pd.read_csv(path1 + '/' + i, sep = '|')

sisco_arl = pd.concat(dic).reset_index(drop = True)
#%%
dic = {}

for i in os.listdir(path2):
    print(i)
    dic[i] = pd.read_csv(path2 + '/' + i, sep = '|')

sisco_salud = pd.concat(dic).reset_index(drop = True)

#%%

sisco_arl_sin_radicado = sisco_arl.copy()

sisco_arl_sin_radicado = sisco_arl_sin_radicado[sisco_arl_sin_radicado['Marcacion_RPT'] == 'No']

sisco_arl_sin_asistenciales = sisco_arl.copy()

sisco_arl_sin_asistenciales = sisco_arl_sin_asistenciales[sisco_arl_sin_asistenciales['Marcacion_Asistenciales'] == 'No']

sisco_arl_sin_egresos = sisco_arl.copy()

sisco_arl_sin_egresos = sisco_arl_sin_egresos[sisco_arl_sin_egresos['Marcacion_Egresos'] == 'No']

#%%
sisco_salud_sin_radicacion_rapida = sisco_salud.copy()

sisco_salud_sin_radicacion_rapida = sisco_salud_sin_radicacion_rapida[sisco_salud_sin_radicacion_rapida['Marcacion_RR'] == 'No']


sisco_salud_sin_egresos = sisco_salud.copy()

sisco_salud_sin_egresos = sisco_salud_sin_egresos[sisco_salud_sin_egresos['Marcacion_Egresos_Imp'] == 'No']

#%%
path_sal1 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas'

sisco_arl_sin_radicado.to_excel(path_sal1 + r'/sisco_arl_sin_radicado.xlsx', index = False)

sisco_arl_sin_asistenciales.to_excel(path_sal1 + r'/sisco_arl_sin_asistenciales.xlsx', index = False)

sisco_arl_sin_egresos.to_excel(path_sal1 + r'/sisco_arl_sin_egresos.xlsx', index = False)

sisco_salud_sin_radicacion_rapida.to_excel(path_sal1 + r'/sisco_salud_sin_radicacion_rapida.xlsx', index = False)

sisco_salud_sin_egresos.to_excel(path_sal1 + r'/sisco_salud_sin_egresos.xlsx', index = False)