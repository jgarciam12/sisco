# -*- coding: utf-8 -*-
"""
Created on Thu Oct  6 11:13:05 2022

@author: jcgarciam
"""

import pandas as pd
import numpy as np
import zipfile
from datetime import datetime
import os
import glob
import Extraccion as Ex
import Transformaciones as Tr


#%%
Tiempo_Total = datetime.now()

Current_Date = datetime.today().date()

print('La fecha del archivo de extracción es: ', Current_Date)
Current_Date = Current_Date.strftime('%Y')+Current_Date.strftime('%m')+Current_Date.strftime('%d')

path_entrada1 = r'\\dc1pcadfrs1\Reportes_Activa\axa'
path_entrada2 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Entradas\AS400'
path_entrada3 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Entradas\Glosas'
path_entrada4 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Entradas\BH'
path_entrada5 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Entradas\goanywhere'

path_salida1 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\SALUD'
path_salida2 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\ARL'
path_salida3 = r'D:\DATOS\Users\jcgarciam\AXA Colpatria Seguros\SHAREPOINT ARL Y SALUD - SISCO\Consolidado_facturas'


path_temporal = r'D:\DATOS\Users\jcgarciam\OneDrive - AXA Colpatria Seguros\Documentos\Informes\SISCO'

dic = {}
    #columnas = ['NUREFE','FEESOP','NUORPA','VANEPG','VAORPA','IVA','RTE_FUE','RTE_ICA','RTE_IVA','ESTOPG','CODFPG',
     #           'ESTCAU','CODCOM','CODFUE']
    
files = os.listdir(path_entrada2)
    
for file in files:
    if file != 'ASISTENCIALES.txt':
        print(file)
        dic[file] = pd.read_csv(path_entrada2 + "/" + file, sep = '\t', header=0, error_bad_lines = False) 
        
#%%
    
Pagos = pd.concat(dic).reset_index(drop = True)

#%%
Pagos['NUREFE'] = Pagos['NUREFE'].astype(str)
Pagos['NUREFE'] = Pagos['NUREFE'].str.strip()

#%%
Pagos['Llave'] = Pagos['NUREFE'].astype(str) + Pagos['NUORPA'].astype(str) + Pagos['NUMCCT'].astype(str) + Pagos['CODIM1'].astype(str) + Pagos['CODIM2'].astype(str) + Pagos['CODIM3'].astype(str) + Pagos['CODIM4'].astype(str)
Pagos = Pagos.drop_duplicates('Llave', keep = 'last')

#%%
Pagos['IVA'] = np.where(Pagos['CODIM1'] == 1, Pagos['VALIM1'],0)

conditions = [
            (Pagos['CODIM1'] == 2),
            (Pagos['CODIM2'] == 2)
            ]
choices = [Pagos['VALIM1'], Pagos['VALIM2']] 

Pagos['RTE_FUE'] = np.select(conditions,choices)

conditions = [
            (Pagos['CODIM1'] == 3),
            (Pagos['CODIM2'] == 3),
            (Pagos['CODIM3'] == 3)
            ]
choices = [Pagos['VALIM1'], Pagos['VALIM2'], Pagos['VALIM3']] 

Pagos['RTE_ICA'] = np.select(conditions,choices)

conditions = [
            (Pagos['CODIM1'] == 4),
            (Pagos['CODIM2'] == 4),
            (Pagos['CODIM3'] == 4),
            (Pagos['CODIM4'] == 4)
            ]
choices = [Pagos['VALIM1'], Pagos['VALIM2'], Pagos['VALIM3'], Pagos['VALIM4']] 

Pagos['RTE_IVA'] = np.select(conditions,choices)

Pagos.loc[:,['IVA','RTE_FUE','RTE_ICA','RTE_IVA']] = Pagos.loc[:,['IVA','RTE_FUE','RTE_ICA','RTE_IVA']].fillna(0)

#%%

#Pagos['Llave'] = Pagos['NUREFE'].astype(str) + Pagos['NUORPA'].astype(str) + Pagos['NUMCCT'].astype(str)
#Pagos_sin = Pagos.drop_duplicates('Llave', keep = 'last')


Pagos2 = Pagos[['NUREFE','IVA','RTE_FUE','RTE_ICA','RTE_IVA']]

Pagos2 = Pagos2.groupby(['NUREFE'], as_index = False)['IVA','RTE_FUE','RTE_ICA','RTE_IVA'].sum().reset_index(drop = True)
Pagos2 = Pagos2.drop_duplicates('NUREFE')
#%%

Pagos3 = Pagos.drop( columns = ['IVA','RTE_FUE','RTE_ICA','RTE_IVA'])
Pagos3 = Pagos3.drop_duplicates('NUREFE')

#%%

Pagos4 = Pagos3.merge(Pagos2, how = 'inner', on = 'NUREFE')
Pagos4 = Pagos4[['NUREFE','FEESOP','NUORPA','VANEPG','VAORPA','IVA','RTE_FUE','RTE_ICA','RTE_IVA','ESTOPG','CODFPG',
                'ESTCAU','CODCOM','CODFUE']]
"""

Pagos['Llave'] = Pagos['NUREFE'].astype(str) + Pagos['NUORPA'].astype(str) + Pagos['NUMCCT'].astype(str)

#%%

Pagos = Pagos.drop_duplicates('Llave', keep = 'last')

#%%

Pagos2 = Pagos[['NUREFE','VALIM1','VALIM2','VALIM3','VALIM4']]

Pagos2 =  Pagos.groupby(['NUREFE'], as_index = False)['VALIM1','VALIM2','VALIM3','VALIM4'].sum().reset_index(drop = True)

#%%

Pagos3 = Pagos[['NUREFE','FEESOP','NUORPA','VANEPG','VAORPA','ESTOPG','CODFPG',
                'ESTCAU','CODCOM','CODFUE','CODIM1','CODIM2','CODIM3','CODIM4']]

Pagos3 = Pagos3.drop_duplicates('NUREFE', keep = 'last')

#%%

Pagos3 = Pagos3.merge(Pagos2, how = 'left', on = 'NUREFE')

#%%

Pagos3['IVA'] = np.where(Pagos3['CODIM1'] == 1, Pagos3['VALIM1'],0)

conditions = [
            (Pagos3['CODIM1'] == 2),
            (Pagos3['CODIM2'] == 2)
            ]
choices = [Pagos3['VALIM1'], Pagos3['VALIM2']] 

Pagos3['RTE_FUE'] = np.select(conditions,choices)

conditions = [
            (Pagos3['CODIM1'] == 3),
            (Pagos3['CODIM2'] == 3),
            (Pagos3['CODIM3'] == 3)
            ]
choices = [Pagos3['VALIM1'], Pagos3['VALIM2'], Pagos3['VALIM3']] 

Pagos3['RTE_ICA'] = np.select(conditions,choices)

conditions = [
            (Pagos3['CODIM1'] == 4),
            (Pagos3['CODIM2'] == 4),
            (Pagos3['CODIM3'] == 4),
            (Pagos3['CODIM4'] == 4)
            ]
choices = [Pagos3['VALIM1'], Pagos3['VALIM2'], Pagos3['VALIM3'], Pagos3['VALIM4']] 

Pagos3['RTE_IVA'] = np.select(conditions,choices)

#%%

Pagos3.loc[:,['IVA','RTE_FUE','RTE_ICA','RTE_IVA']] = Pagos3.loc[:,['IVA','RTE_FUE','RTE_ICA','RTE_IVA']].fillna(0)

'''
Suma(Si([CODIM1]=2;[VALIM1];Si([CODIM2]=2;[VALIM2];0)))
Suma(Si([CODIM1]=3;[VALIM1];Si([CODIM2]=3;[VALIM2];Si([CODIM3]=3;[VALIM3];0))))
Suma(Si([CODIM1]=4;[VALIM1];Si[CODIM2]=4;[VALIM2];Si([CODIM3]=4;[VALIM3];S([CODIM4]=4;[VALIM4];0)))))
'''

#'IVA','RTE_FUE','RTE_ICA','RTE_IVA',"""


#%%

b.to_