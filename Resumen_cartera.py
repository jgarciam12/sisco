# -*- coding: utf-8 -*-
"""
Created on Wed May 24 08:45:16 2023

@author: jcgarciam
"""
import pandas as pd
import glob
from datetime import datetime
import numpy as np


Tiempo_Total = datetime.now()

path_entrada1 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\SALUD\CONSOLIDADO_SISCO'

dic = {}


print('\n Inicio de carga de archivos SISCO Salud \n')
for Archivo in glob.glob(path_entrada1 + '\*csv'):
    if int(Archivo[-8:-4]) >= 2017:
        print('Cargando archivo ', Archivo[-20:-4])
        df = pd.read_csv(Archivo, sep = '|', header = 0)
        print('Archivo ', Archivo[-20:-4], ' cargado \n')
        dic[Archivo] = df

#%%   

df_salud = pd.concat(dic).reset_index(drop = True)
#df_arl = Maestro_ARL.reset_index(drop=True)


df_salud['Fecha_Radicacion'] = pd.to_datetime(df_salud['Fecha_Radicacion'], format = '%d/%m/%Y', dayfirst = True)

df_salud = df_salud.sort_values('Fecha_Radicacion', ascending = False)


def CambioFormato(df, a = 'a'):
    df[a] = df[a].astype(str).str.strip('\x02').str.strip('').str.strip()
    df[a] = np.where(df[a].str[-2::] == '.0', df[a].str[0:-2], df[a])
    df[a] = np.where(df[a] == 'nan', np.nan, df[a])
    return df[a]    

df_salud['Cod_Barra'] = CambioFormato(df_salud, a = 'Cod_Barra')
df_salud['Cod_Barra_Pago_Parcial'] = CambioFormato(df_salud, a = 'Cod_Barra_Pago_Parcial')
df_salud['Fac_Norint'] = CambioFormato(df_salud, a = 'Fac_Norint')

df_salud['Numero_OP'] = CambioFormato(df_salud, a = 'Numero_OP')
df_salud['Numero_Interno'] = CambioFormato(df_salud, a = 'Numero_Interno')

df_salud['Numero de Radicados'] = df_salud['Cod_Barra'].astype(str) + df_salud['Cod_Barra_Pago_Parcial'].astype(str)
df_salud['Llave'] = df_salud['Cod_Barra'].astype(str) + df_salud['Cod_Barra_Pago_Parcial'].astype(str) + df_salud['Numero_OP'].astype(str)



def Cantidad(df, a):
    df2 = df.copy()
    df2['Cantidad de '+ a] = df2[a].copy()
    df2 = df2.groupby(a, as_index = False)['Cantidad de ' + a].count()
    df = df.merge(df2, how = 'left', on = a)
    df = df.sort_values(['Cantidad de ' + a, a], ascending = False)
    return df

df_salud = Cantidad(df_salud, a = 'Numero_OP')
df_salud['Cantidad de Numero_OP'] = df_salud['Cantidad de Numero_OP'].fillna(0.0)
df_salud = Cantidad(df_salud, a = 'Llave')

df_prueba = df_salud.drop_duplicates('Numero de Radicados')
df_prueba = df_prueba.groupby('Cod_Barra', as_index = False)['Numero de Radicados'].count()

#%%


#df_salud = df_salud[df_salud['Fecha_Radicacion'].dt.year.isin([2022,2023])]
valores = ['Valor_Pagado_Egresos','Valor_Glosa','Nota_crédito','Rete_Fuente','Rete_ICA']

for i in valores:
    df_salud[i] = df_salud[i].astype(str).str.replace(',','.').astype(float)
    
#%%
df_resumen_a = df_salud.copy()
df_resumen_a = df_resumen_a[df_resumen_a['Cantidad de Numero_OP'] <= 1]
#%%
df_resumen_b = df_salud.copy()
df_resumen_b = df_resumen_b[df_resumen_b['Cantidad de Numero_OP'] > 1]
df_resumen_b['Valor_Pagado_Egresos'] = df_resumen_b['Valor_Pagado_Egresos'] / df_resumen_b['Cantidad de Numero_OP']
df_resumen_b['Rete_Fuente'] = df_resumen_b['Rete_Fuente'] / df_resumen_b['Cantidad de Numero_OP']
df_resumen_b['Rete_ICA'] = df_resumen_b['Rete_ICA'] / df_resumen_b['Cantidad de Numero_OP']
#%%
df_resumen = pd.concat([df_resumen_a,df_resumen_b])
df_resumen = df_resumen.sort_values('Fecha_Radicacion', ascending = False)

df_resumen = df_resumen.groupby('Cod_Barra', as_index = False).agg({'Numero_Factura_Original':'last','Fac_Norint':'last','Valor_Pagado_Egresos':'sum','Valor_Glosa':'last','Nota_crédito':'first','Estado_actual':'first','NIT':'last','Razon_Social':'last','Rete_Fuente':'sum','Rete_ICA':'sum','IVA':'sum','Rete_IVA':'sum','Fecha_Radicacion':'last','Cantidad de Numero_OP':'sum'})
#%%

df_resumen = df_resumen.merge(df_prueba, how = 'left', on = 'Cod_Barra')


from datetime import datetime
import glob

#%%
Tiempo_Total = datetime.now()

Current_Date = datetime.today().date()

print('La fecha del archivo de extracción es: ', Current_Date)
Current_Date = Current_Date.strftime('%Y')+Current_Date.strftime('%m')+ Current_Date.strftime('%d')
path_entrada3 = r'\\dc1pcadfrs1\Reportes_Activa'


def ExtraccionGlosas(path3, Current_Date):
    #Extraccion de la base glosas
    dic = {}
    
    for file in glob.glob(path3 + '/' + Current_Date + '*SPNET_CONTROL_INFORME_MENSUAL_GLOSAS (AXA Colpatria -*'):
        if 'ARL' not in file:
            print('Leyendo el archivo: ',file[len(path3) + 1::])
            dic[file] = pd.read_excel(file, header=0)    
            print('El archivo: ',file[len(path3) + 1::], ' leido')
    Glosas = pd.concat(dic).reset_index(drop = True)
    
    return Glosas

glosas = ExtraccionGlosas(path_entrada3, Current_Date)
#%%
glosas.loc[glosas['Codigo_Glosa'].str[1] == '1','Homo_Rubro'] = '1. Facturación'
glosas.loc[glosas['Codigo_Glosa'].str[1] == '2','Homo_Rubro'] = '2. Tarifas'
glosas.loc[glosas['Codigo_Glosa'].str[1] == '3','Homo_Rubro'] = '3. Soportes'
glosas.loc[glosas['Codigo_Glosa'].str[1] == '4','Homo_Rubro'] = '4. Autorizaciones'
glosas.loc[glosas['Codigo_Glosa'].str[1] == '5','Homo_Rubro'] = '5. Cobertura'
glosas.loc[glosas['Codigo_Glosa'].str[1] == '6','Homo_Rubro'] = '6. Pertinencia'
glosas.loc[glosas['Codigo_Glosa'].str[1] == '8','Homo_Rubro'] = '8. Devolución'

#%%

def CambioFormato(df, a = 'a'):
    df[a] = df[a].astype(str).str.strip().str.strip('\x02').str.strip('')
    df[a] = np.where(df[a].str[-2::] == '.0', df[a].str[0:-2], df[a])
    df[a] = np.where(df[a] == 'nan', np.nan, df[a])
    return df[a]


glosas['Numero_Interno'] = CambioFormato(glosas, a = 'Numero_Interno')

#%%
#glosas2 = glosas.groupby(['Numero_Interno','Homo_Rubro'], as_index = False ).agg({'Valor_Glosa_Detalle':'sum'})
#%%
glosas2 = glosas.pivot_table(index = 'Numero_Interno', 
                    values = ['Valor_Glosa_Detalle'], 
                    columns = 'Homo_Rubro', fill_value = 0.0,
                    aggfunc = 'sum'
                    )

glosas2.columns = glosas2.columns.get_level_values(1)
glosas2 = glosas2.reset_index()
#%%
glosas3 = glosas.pivot_table(index = 'Numero_Interno', 
                    values = ['Valor_Glosa_Detalle'], 
                    columns = 'Homo_Rubro', fill_value = 0.0,
                    aggfunc = 'count'
                    )

glosas3.columns = glosas3.columns.get_level_values(1)
glosas3 = glosas3.reset_index()

#%%

glosas3 = glosas3.rename(columns = {'1. Facturación':'1. Cantidad Facturación',
                                    '2. Tarifas':'2. Cantidad Tarifas',
                                    '3. Soportes':'3. Cantidad Soportes',
                                    '4. Autorizaciones':'4. Cantidad Autorizaciones',
                                    '5. Cobertura':'5. Cantidad Cobertura',
                                    '6. Pertinencia':'6. Cantidad Pertinencia',
                                    '8. Devolución':'8. Cantidad Devolución'})
#%%

df_resumen['Pago con impuestos'] = df_resumen['Valor_Pagado_Egresos'] + df_resumen['Rete_Fuente'] + df_resumen['Rete_ICA']
devoluciones = ['AUDITADA: Devuelta sin posibilidad de re-ingreso.','EN AUDITORIA: Factura Anulada','AUDITADA: Factura Anulada']

df_resumen.loc[df_resumen['Estado_actual'].isin(devoluciones) == True,'Grupo'] = 'Devueltas o Anuladas'
df_resumen.loc[(df_resumen['Grupo'].isnull() == True) & (df_resumen['Cantidad de Numero_OP'] == 0),'Grupo'] = 'Sin pago'
df_resumen.loc[df_resumen['Grupo'].isnull() == True,'Grupo'] = 'Con Pago'
#%%
df_resumen['Valor_Glosa_prueba'] = np.where(df_resumen['Nota_crédito'] == df_resumen['Valor_Glosa'],0,df_resumen['Valor_Glosa'])

#%%
df_resumen = df_resumen[df_resumen['Fecha_Radicacion'].dt.year.isin([2021,2022,2023]) == True]

ipss = pd.read_excel(r'D:\DATOS\Users\jcgarciam\Downloads/33 ips.xlsx', header = 0, sheet_name = 'Hoja1', usecols = ['NIT'])
#%%

df_resumen['NIT'] = CambioFormato(df_resumen, a = 'NIT')
ipss['NIT'] = CambioFormato(ipss, a = 'NIT')

#%%

df_resumen = df_resumen[df_resumen['NIT'].isin(ipss['NIT']) == True]

#%%

df_resumen2 = df_resumen.merge(glosas2, how = 'left', left_on = 'Fac_Norint', right_on = 'Numero_Interno')
df_resumen2 = df_resumen2.merge(glosas3, how = 'left', left_on = 'Fac_Norint', right_on = 'Numero_Interno')

#%%

df_resumen2.to_excel(r'D:\DATOS\Users\jcgarciam\Downloads/Resumen Cartera Salud 30 ips.xlsx')


#%%





