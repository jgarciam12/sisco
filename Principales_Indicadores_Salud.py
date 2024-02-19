# -*- coding: utf-8 -*-
"""
Created on Tue Jun  6 07:40:34 2023

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

#%%

df_salud['Fecha_Radicacion'] = pd.to_datetime(df_salud['Fecha_Radicacion'], format = '%d/%m/%Y', dayfirst = True)

df_salud = df_salud.sort_values('Fecha_Radicacion', ascending = False)

#%%

def CambioFormato(df, a = 'a'):
    df[a] = df[a].astype(str).str.strip('\x02').str.strip('').str.strip()
    df[a] = np.where(df[a].str[-2::] == '.0', df[a].str[0:-2], df[a])
    df[a] = np.where(df[a] == 'nan', np.nan, df[a])
    return df[a]    

df_salud['Cod_Barra'] = CambioFormato(df_salud, a = 'Cod_Barra')
df_salud['Numero_OP'] = CambioFormato(df_salud, a = 'Numero_OP')


df_salud['Llave'] = df_salud['Cod_Barra'].astype(str) + df_salud['Numero_OP'].astype(str)

def Cantidad(df, a):
    df2 = df.copy()
    df2['Cantidad de '+ a] = df2[a].copy()
    df2 = df2.groupby(a, as_index = False)['Cantidad de ' + a].count()
    df = df.merge(df2, how = 'left', on = a)
    df = df.sort_values(['Cantidad de ' + a, a], ascending = False)
    return df

df_salud = Cantidad(df_salud, a = 'Numero_OP')
df_salud['Cantidad de Numero_OP'] = df_salud['Cantidad de Numero_OP'].fillna(0.0)

df_salud = df_salud.drop_duplicates('Llave')

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

df_resumen = df_resumen.groupby('Cod_Barra', as_index = False).agg({'Numero_Factura_Original':'last','Valor_Pagado_Egresos':'sum','Valor_Glosa':'first','Nota_crédito':'first','Estado_actual':'first','NIT':'last','Razon_Social':'last','Rete_Fuente':'sum','Rete_ICA':'sum','IVA':'sum','Rete_IVA':'sum','Fecha_Radicacion':'last','Cantidad de Numero_OP':'sum'})
#%%

dic = {}

columnas = ['Cod_Barra','Valor_Neto','Regimen']
import zipfile
import csv
Current_Date = datetime.today().date()
path_entrada2 = r'\\dc1pcadfrs1\Reportes_Activa\axa'

Current_Date = Current_Date.strftime('%Y')+Current_Date.strftime('%m')+ Current_Date.strftime('%d')


with zipfile.ZipFile(path_entrada2 + '/'+Current_Date+'.zip', mode = 'r') as z:
    lista = z.namelist()
    for i in lista:
        with z.open(i) as f:
            print(i)
            df = pd.read_csv(f, sep=';', header=0, encoding='ANSI', engine='python', quoting=csv.QUOTE_NONE, index_col =  False, usecols = columnas)
            df['Origen'] = i
            dic[i] = df
            
        
Maestro = pd.concat(dic).reset_index(drop = True)
Maestro = Maestro.sort_values('Origen', ascending = True)
Maestro['Cod_Barra'] = CambioFormato(Maestro, a = 'Cod_Barra')
Maestro = Maestro.drop_duplicates('Cod_Barra', keep = 'last')


valor_neto_original = Maestro[['Cod_Barra','Valor_Neto','Regimen']].copy()
valor_neto_original = valor_neto_original[valor_neto_original['Regimen'].astype(str).str.upper().str.contains('ARL') == False]

#%%

df_resumen = df_resumen.merge(valor_neto_original, how = 'inner', on = 'Cod_Barra')
df_resumen['Pago con impuestos'] = df_resumen['Valor_Pagado_Egresos'] + df_resumen['Rete_Fuente'] + df_resumen['Rete_ICA']
devoluciones = ['AUDITADA: Devuelta sin posibilidad de re-ingreso.','EN AUDITORIA: Factura Anulada','AUDITADA: Factura Anulada']

df_resumen.loc[df_resumen['Estado_actual'].isin(devoluciones) == True,'Grupo'] = 'Devueltas o Anuladas'
df_resumen.loc[(df_resumen['Grupo'].isnull() == True) & (df_resumen['Cantidad de Numero_OP'] == 0),'Grupo'] = 'Sin pago'
df_resumen.loc[df_resumen['Grupo'].isnull() == True,'Grupo'] = 'Con Pago'
#%%
df_resumen['Valor_Glosa_prueba'] = np.where(df_resumen['Nota_crédito'] == df_resumen['Valor_Glosa'],0,df_resumen['Valor_Glosa'])

#%%
df_resumen = df_resumen[df_resumen['Fecha_Radicacion'].dt.year.isin([2022,2023]) == True]

#%%
reporte = df_resumen.pivot_table(index = 'Año de Radicación', 
                    values = ['Valor_Neto','Pago con impuestos','Valor_Glosa','Nota_crédito'], 
                    columns = 'Grupo', fill_value = 0.0,
                    aggfunc = {'Valor_Neto':'sum','Pago con impuestos':'sum','Valor_Glosa':'sum','Nota_crédito':'sum'}, 
                    margins = True,margins_name='Total'
                    )

reporte = reporte.reorder_levels([1,0], axis = 1).sort_index(axis = 1)
reporte = reporte.applymap('${:,.0f}'.format)
#order_valores = ['Cod_Barra','Valor_Neto','Pago con impuestos','Valor_Glosa','Nota_crédito']

#reporte = reporte.reindex(order_valores, axis = 1, level = 1)

#reporte = reporte.applymap('${:,.0f}'.format)

#reporte['Con Pago']['Total Facturas'] = reporte['Con Pago']['Total Facturas'].str.replace('$','')

reporte2 = df_resumen.pivot_table(index = 'Año de Radicación', 
                    values = ['Cod_Barra'], 
                    columns = 'Grupo', fill_value = 0.0, aggfunc = {'Cod_Barra':'count'}, 
                    margins = True,margins_name='Total'
                    )
reporte2 = reporte2.reorder_levels([1,0], axis = 1).sort_index(axis = 1)

reporte2 = reporte2.rename(columns = {'Cod_Barra':'Total Facturas'})
reporte2 = reporte2.applymap('{:,.0f}'.format)
#%%
reporte_final = pd.merge(reporte, reporte2, on = ['Año de Radicación'])

order_grupos = ['Con Pago','Sin pago','Devueltas, Anuladas ó sin completar carga']

reporte_final = reporte_final[order_grupos]

order_valores = ['Total Facturas','Valor_Neto','Pago con impuestos','Valor_Glosa','Nota_crédito']

reporte_final = reporte_final.reindex(order_valores, axis = 1, level = 1)