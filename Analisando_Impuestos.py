# -*- coding: utf-8 -*-
"""
Created on Wed Aug 23 08:43:04 2023

@author: jcgarciam
"""

import os
import pandas as pd
path2 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Entradas\AS400'

def ExtraccionPagos(path2):
    #Extraccion de la base Pagos
    dic = {}
    
    files = os.listdir(path2)
    
    for file in files:
        if file != 'ASISTENCIALES.txt':
            print(file)
            dic[file] = pd.read_csv(path2 + "/" + file, sep = '\t', header=0, error_bad_lines = False)    
    
    Pagos = pd.concat(dic).reset_index(drop = True)
    
    return Pagos

pagos = ExtraccionPagos(path2)
#%%
def PagosFinal(Pagos):
    import numpy as np
    #Se quitan NUREFEs vacios 
    Pagos = Pagos[Pagos['NUREFE'].isnull() == False]
    Pagos['NUREFE'] = Pagos['NUREFE'].astype(str).str.strip()

    # Se hace la siguente llave para luego quitar duplicados y calcular los impuestos
    Pagos['Llave'] = Pagos['NUREFE'].astype(str) + Pagos['NUORPA'].astype(str) + Pagos['NUMCCT'].astype(str) + Pagos['CODIM1'].astype(str) + Pagos['CODIM2'].astype(str) + Pagos['CODIM3'].astype(str) + Pagos['CODIM4'].astype(str)
    Pagos = Pagos.drop_duplicates('Llave', keep = 'last')


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

    #Un NUREFE puede tener diferentes NUORPAs porque puede ser una factura multiusuario,
    #Un NUORPA puede tener diferentes NUORPAs porque puede realizarse un pago multiple
    #Por eso la llave unica deberia ser NUREFE-NUORPA
    Pagos2 = Pagos[['NUREFE','NUORPA','IVA','RTE_FUE','RTE_ICA','RTE_IVA']]

    Pagos2 = Pagos2.groupby(['NUREFE','NUORPA'], as_index = False)['IVA','RTE_FUE','RTE_ICA','RTE_IVA'].sum().reset_index(drop = True)

    Pagos3 = Pagos.drop( columns = ['IVA','RTE_FUE','RTE_ICA','RTE_IVA'])
    Pagos3['Llave'] = Pagos3['NUREFE'].astype(str) + Pagos3['NUORPA'].astype(str)
    Pagos3 = Pagos3.drop_duplicates('Llave')
    Pagos3 = Pagos3.drop(columns = ['Llave'])


    Pagos4 = Pagos3.merge(Pagos2, how = 'inner', on = ['NUREFE','NUORPA'])
    Pagos = Pagos4[['NUREFE','FEESOP','NUORPA','VANEPG','VAORPA','IVA','RTE_FUE','RTE_ICA','RTE_IVA','ESTOPG','CODFPG',
                'ESTCAU','CODCOM','CODFUE']]
    
    # Un dato estaba viniendo con la fecha mal
    Pagos['FEESOP'] = Pagos['FEESOP'].replace(50210915,20210915)
    
    Pagos = Pagos.rename(columns = {'FEESOP':'Fecha_OP','NUORPA':'Numero_OP','VAORPA':'Valor_OP','VANEPG':'Valor_Pagado_Egresos',
                                    'ESTOPG':'Estado_Pago','CODFPG':'Codigo_Forma_Pago','ESTCAU':'Estado_Egresos',
                                    'RTE_FUE':'Rete_Fuente','RTE_ICA':'Rete_ICA','RTE_IVA':'Rete_IVA'})
    
       
    Pagos['Fecha_OP'] = np.where(Pagos['Fecha_OP'] != 0, Pagos['Fecha_OP'], np.nan)
    Pagos['Fecha_OP'] = pd.to_datetime(Pagos['Fecha_OP'], format = '%Y%m%d')
    Pagos['Fecha_OP'] = Pagos['Fecha_OP'].dt.strftime('%d/%m/%Y')
    
    Pagos = Pagos[Pagos['NUREFE'].astype(str).str.lower() != 'nan']
    
    Pagos = Pagos[Pagos['NUREFE'].isnull() == False]

    return Pagos

def PagosFinal_Prueba(Pagos):
    import numpy as np
    #Se quitan NUREFEs vacios 
    Pagos = Pagos[Pagos['NUREFE'].isnull() == False]
    Pagos['NUREFE'] = Pagos['NUREFE'].astype(str).str.strip()

    # Se hace la siguente llave para luego quitar duplicados y calcular los impuestos
    Pagos['Llave'] = Pagos['NUREFE'].astype(str) + Pagos['NUORPA'].astype(str) + Pagos['CODIM1'].astype(str) + Pagos['CODIM2'].astype(str) + Pagos['CODIM3'].astype(str) + Pagos['CODIM4'].astype(str) + Pagos['VALIM1'].astype(str) + Pagos['VALIM2'].astype(str) + Pagos['VALIM3'].astype(str) + Pagos['VALIM4'].astype(str)
    Pagos = Pagos.drop_duplicates('Llave', keep = 'last')


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

    #Un NUREFE puede tener diferentes NUORPAs porque puede ser una factura multiusuario,
    #Un NUORPA puede tener diferentes NUORPAs porque puede realizarse un pago multiple
    #Por eso la llave unica deberia ser NUREFE-NUORPA
    Pagos2 = Pagos[['NUREFE','NUORPA','IVA','RTE_FUE','RTE_ICA','RTE_IVA']]

    Pagos2 = Pagos2.groupby(['NUREFE','NUORPA'], as_index = False)['IVA','RTE_FUE','RTE_ICA','RTE_IVA'].sum().reset_index(drop = True)

    Pagos3 = Pagos.drop( columns = ['IVA','RTE_FUE','RTE_ICA','RTE_IVA'])
    Pagos3['Llave'] = Pagos3['NUREFE'].astype(str) + Pagos3['NUORPA'].astype(str)
    Pagos3 = Pagos3.drop_duplicates('Llave')
    Pagos3 = Pagos3.drop(columns = ['Llave'])


    Pagos4 = Pagos3.merge(Pagos2, how = 'inner', on = ['NUREFE','NUORPA'])
    Pagos = Pagos4[['NUREFE','FEESOP','NUORPA','VANEPG','VAORPA','IVA','RTE_FUE','RTE_ICA','RTE_IVA','ESTOPG','CODFPG',
                'ESTCAU','CODCOM','CODFUE']]
    
    # Un dato estaba viniendo con la fecha mal
    Pagos['FEESOP'] = Pagos['FEESOP'].replace(50210915,20210915)
    
    Pagos = Pagos.rename(columns = {'FEESOP':'Fecha_OP','NUORPA':'Numero_OP','VAORPA':'Valor_OP','VANEPG':'Valor_Pagado_Egresos',
                                    'ESTOPG':'Estado_Pago','CODFPG':'Codigo_Forma_Pago','ESTCAU':'Estado_Egresos',
                                    'RTE_FUE':'Rete_Fuente','RTE_ICA':'Rete_ICA','RTE_IVA':'Rete_IVA'})
    
       
    Pagos['Fecha_OP'] = np.where(Pagos['Fecha_OP'] != 0, Pagos['Fecha_OP'], np.nan)
    Pagos['Fecha_OP'] = pd.to_datetime(Pagos['Fecha_OP'], format = '%Y%m%d')
    Pagos['Fecha_OP'] = Pagos['Fecha_OP'].dt.strftime('%d/%m/%Y')
    
    Pagos = Pagos[Pagos['NUREFE'].astype(str).str.lower() != 'nan']
    
    Pagos = Pagos[Pagos['NUREFE'].isnull() == False]

    return Pagos

#%%

Pagos1 = PagosFinal(pagos)
Pagos2 = PagosFinal_Prueba(pagos)

#%%

Pagos2 = Pagos2.rename(columns = {'Rete_Fuente':'Rete_Fuente2','Rete_ICA':'Rete_ICA2'})

#%%

Pagos1 = Pagos1.merge(Pagos2[['Rete_Fuente2','Rete_ICA2','NUREFE','Numero_OP']], on = ['NUREFE','Numero_OP'])

#%%

Pagos1['Dif R Fuente'] = Pagos1['Rete_Fuente'] - Pagos1['Rete_Fuente2']
Pagos1['Dif R ICA'] = Pagos1['Rete_ICA'] - Pagos1['Rete_ICA2']

#%%

Pagos_Prueba = Pagos1[(Pagos1['Dif R Fuente'].abs() > 0) | (Pagos1['Dif R ICA'].abs() > 0)]

#%%

d = pagos[pagos['NUREFE'].astype(str).str.strip().isin(Pagos_Prueba['NUREFE']) == False]




