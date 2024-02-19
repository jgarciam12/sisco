# -*- coding: utf-8 -*-
"""
Created on Wed Aug 31 15:41:29 2022

@author: jcgarciam
"""
import pandas as pd
import glob
from datetime import datetime
import numpy as np

Tiempo_Total = datetime.now()

path_entrada1 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\SALUD\CONSOLIDADO_SISCO'
path_entrada2 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\ARL\CONSOLIDADO_SISCO'
path_entrada4 = r'D:\DATOS\Users\jcgarciam\OneDrive - AXA Colpatria Seguros\Documentos\Informes\SISCO\Consulta sisco salida\Input'

path_salida = r'D:\DATOS\Users\jcgarciam\OneDrive - AXA Colpatria Seguros\Documentos\Informes\SISCO\Consulta sisco salida\Output'
#%%
dic = {}

columnas = ['Numero_Factura_Original','NIT','Numero_OP','Fecha_OP','Valor_Pagado_Egresos',
             'Valor_Bruto','Valor_OP','IVA','Rete_Fuente','Rete_ICA','Rete_IVA','Regimen','Codigo_Glosa',
             'Observacion','Estado_actual','Valor_Glosa','Fecha_Radicacion','Fecha_Ultimo_estado']

print('\n Inicio de carga de archivos SISCO Salud \n')
for Archivo in glob.glob(path_entrada1 + '\*csv'):
    print('\n Cargando archivo ', Archivo[-20:-4])
    df = pd.read_csv(Archivo, sep = '|', header = 0, usecols = columnas)
    df['Origen'] = 'Salud'
    print('Archivo ', Archivo[-20:-4], ' cargado \n')
    dic[Archivo] = df
    
for Archivo in glob.glob(path_entrada2 + '\*csv'):
    print('\n Cargando archivo ', Archivo[-18:-4])
    df = pd.read_csv(Archivo, sep = '|', header = 0, usecols = columnas)
    df['Origen'] = 'ARL'
    print('Archivo ', Archivo[-18:-4], ' cargado \n')
    dic[Archivo] = df
    
#%%
   

sisco = pd.concat(dic).reset_index(drop = True)

print('\n SISCO Salud cargado completamente')

print('\n Genrando SISCO Salud con pagos \n')
print('SISCO Salud con pagos generado \n')
print("\n Tiempo del proceso de carga: " , datetime.now()-Tiempo_Total)

sisco['Numero_OP'] = sisco['Numero_OP'].astype(str)
sisco['Numero_OP'] = np.where(sisco['Numero_OP'].str[-2::] == '.0', sisco['Numero_OP'].str[0:-2], sisco['Numero_OP'])

lista_valores = ['Valor_Pagado_Egresos','Valor_Bruto','Valor_OP','IVA','Rete_Fuente','Rete_ICA',
               'Rete_IVA','Valor_Glosa']

for i in lista_valores:
    sisco[i] = sisco[i].astype(str)
    sisco[i] = sisco[i].str.replace(',','.')
    sisco[i] = np.where(sisco[i].str[-2::] == '.0', sisco[i].str[0:-2], sisco[i])
    sisco[i] = sisco[i].astype(float)

print('\n Cargando archivo de devoluciones')
b = glob.glob(path_entrada4 + '/*Gestion documental de salida (AXA Colpatria-Administradora riesgos laborales ARL)*.xlsx')
devoluciones = pd.read_excel(b[0], header = 0, usecols = ['NIT_IPS','Factura','Ultimo_Estado','Observaciones_Devolucion','Fecha_Cierre_Proceso'], dtype = {'NIT_IPS':str})
print('\n Archivo de devoluciones cargado \n')

sisco['NIT'] = sisco['NIT'].astype(str)

sisco['Fecha_Ultimo_estado'] = pd.to_datetime(sisco['Fecha_Ultimo_estado'], format = '%d/%m/%Y')
sisco['Fecha_OP'] = pd.to_datetime(sisco['Fecha_OP'], format = '%d/%m/%Y')
sisco['Fecha_OP'] = sisco['Fecha_OP'].dt.strftime('%d/%m/%Y')

sisco = sisco.sort_values('Fecha_Ultimo_estado', ascending = False)

#%%

#glosas = sisco[['Observacion','Valor_Glosa','Numero_Interno','Codigo_Glosa']]
#glosas = glosas.drop_duplicates('Numero_Interno')
#glosas['Numero_Interno'] = glosas['Numero_Interno'].astype(str)

#devoluciones['Numero_Interno'] = devoluciones['Numero_Interno'].astype(str)

#devoluciones = devoluciones.merge(glosas, how = 'left', on = 'Numero_Interno')

devoluciones['NIT_IPS'] = devoluciones['NIT_IPS'].astype(str)
devoluciones = devoluciones.rename(columns = {'Observaciones_Devolucion':'Observacion','Ultimo_Estado':'Estado_actual','Fecha_Cierre_Proceso':'Fecha_Ultimo_estado'})

#%%

entrada = 'si'
while entrada.title() == 'Si':
    Numero_NIT = input('\n Ingrese el número de NIT que desea consultar: ')
    a = glob.glob(path_entrada4 + '/EC-' + str(Numero_NIT) + '*.xlsx')
    base_consulta = pd.read_excel(a[0], sheet_name = 'EC', header = 8, usecols = ['NUMERO FACTURA','NIT','FECHA FACTURA','FECHA RADICACION','VALOR FACTURA','VALOR SALDO'])
    base_consulta = base_consulta.drop_duplicates('NUMERO FACTURA')
    
    if (Numero_NIT in list(sisco['NIT'])) | (Numero_NIT in list(devoluciones['NIT_IPS'])):
        print('\n El NIT ', str(Numero_NIT),' se encuentra en la base \n')
        
        sisco_NIT = sisco[(sisco['NIT'] == Numero_NIT)] 
        sisco_NIT2 = sisco_NIT.copy()        
        sisco_NIT2['Numero_Factura_Original'] = sisco_NIT2['Numero_Factura_Original'].astype(str)
        sisco_NIT2['Numero_Factura_Original'] = sisco_NIT2['Numero_Factura_Original'].str.strip('#')
        sisco_NIT2['Numero_Factura_Original'] = sisco_NIT2['Numero_Factura_Original'].str.strip()

        sisco_NIT2 = sisco_NIT2.groupby(['Numero_Factura_Original'], as_index = False).agg({'Fecha_Radicacion':'first',
        'Valor_Bruto':'first','Valor_Glosa':'first', 'Numero_OP':'first', 'Fecha_OP':'first','Valor_Pagado_Egresos':'sum','Valor_OP':'sum', 'IVA':'sum', 'Rete_Fuente':'sum', 'Rete_ICA':'sum',
       'Rete_IVA':'sum','Regimen':'first', 'Estado_actual':'first', 'Fecha_Ultimo_estado':'first',
       'Codigo_Glosa':'first','Observacion':'first', 'Origen':'first'})
        
        base_consulta['NUMERO FACTURA_t'] = base_consulta['NUMERO FACTURA'].copy().astype(str)
        base_consulta['NUMERO FACTURA_t'] = base_consulta['NUMERO FACTURA_t'].str.strip('#')
        base_consulta['NUMERO FACTURA_t'] = base_consulta['NUMERO FACTURA_t'].str.strip()
        
        base_consulta2 = base_consulta.copy()
        base_consulta2 = base_consulta2.merge(sisco_NIT2, how = 'left', left_on = 'NUMERO FACTURA_t', right_on = 'Numero_Factura_Original')
        
        base_consulta2_a = base_consulta2.copy()
        base_consulta2_a = base_consulta2_a[base_consulta2_a['Numero_Factura_Original'].isnull() == False]
        
        base_consulta2_b = base_consulta2.copy()
        base_consulta2_b = base_consulta2_b[base_consulta2_b['Numero_Factura_Original'].isnull() == True]
        
        def OnlyNumbers(df, a = 'a'):
            df[a + '_2'] = df[a]
            df[a + '_2'] = df[a + '_2'].astype(str).str.upper()
            rep = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','Ñ','O','P','Q','R','S',
                       'T','U','V','W','X','Y','Z','.','-',',']
            for i in rep:                
                df[a + '_2'] = df[a + '_2'].str.replace(i,'#')
                
            df[a + '_2'] = df[a + '_2'].str.strip('#')
            df[a + '_2'] = df[a + '_2'].str.strip()
            return df
            
        sisco_NIT2 = OnlyNumbers(sisco_NIT2, a = 'Numero_Factura_Original') 
        
        base_consulta2_b = base_consulta2_b[['NUMERO FACTURA','NIT','FECHA FACTURA','FECHA RADICACION','VALOR FACTURA','VALOR SALDO','NUMERO FACTURA_t']]
        base_consulta2_b = OnlyNumbers(base_consulta2_b, a = 'NUMERO FACTURA_t')
        base_consulta2_b = base_consulta2_b.merge(sisco_NIT2, how = 'left', left_on = 'NUMERO FACTURA_t_2', right_on = 'Numero_Factura_Original_2')
        base_consulta2_b = base_consulta2_b.sort_values('Fecha_Ultimo_estado', ascending = False)
        base_consulta2_b = base_consulta2_b.drop_duplicates('NUMERO FACTURA', keep = 'first')
        
        devoluciones_NIT = devoluciones[(devoluciones['NIT_IPS'] == Numero_NIT)]
        devoluciones_NIT = devoluciones_NIT.drop(columns = {'NIT_IPS'})
        devoluciones_NIT['Fecha_Ultimo_estado'] = pd.to_datetime(devoluciones_NIT['Fecha_Ultimo_estado'], format = '%Y-%m-%d')
        devoluciones_NIT = devoluciones_NIT.sort_values('Fecha_Ultimo_estado', ascending = False)
        devoluciones_NIT = devoluciones_NIT.drop_duplicates('Factura', keep = 'first')
        devoluciones_NIT = OnlyNumbers(devoluciones_NIT, a = 'Factura')
        
        base_consulta2_b_a = base_consulta2_b.copy()
        base_consulta2_b_a = base_consulta2_b_a[base_consulta2_b_a['Numero_Factura_Original'].isnull() == False]
        
        base_consulta2_b_b = base_consulta2_b.copy()
        base_consulta2_b_b = base_consulta2_b_b[base_consulta2_b_b['Numero_Factura_Original'].isnull() == True]
        base_consulta2_b_b = base_consulta2_b_b[['NUMERO FACTURA','NIT','FECHA FACTURA','FECHA RADICACION','VALOR FACTURA','VALOR SALDO','NUMERO FACTURA_t']]        
        base_consulta2_b_b = base_consulta2_b_b.merge(devoluciones_NIT, how = 'left', left_on = 'NUMERO FACTURA_t', right_on = 'Factura')
        
        base_consulta2_b_b_a = base_consulta2_b_b.copy()
        base_consulta2_b_b_a = base_consulta2_b_b_a[base_consulta2_b_b_a['Factura'].isnull() == False]
        
        base_consulta2_b_b_b = base_consulta2_b_b.copy()
        base_consulta2_b_b_b = base_consulta2_b_b_b[base_consulta2_b_b_b['Factura'].isnull() == True]
        base_consulta2_b_b_b = base_consulta2_b_b_b[['NUMERO FACTURA','NIT','FECHA FACTURA','FECHA RADICACION','VALOR FACTURA','VALOR SALDO','NUMERO FACTURA_t']]        
        base_consulta2_b_b_b = OnlyNumbers(base_consulta2_b_b_b, a = 'NUMERO FACTURA_t')
        base_consulta2_b_b_b = base_consulta2_b_b_b.merge(devoluciones_NIT, how = 'left', left_on = 'NUMERO FACTURA_t', right_on = 'Factura_2')
        base_consulta2_b_b_b = base_consulta2_b_b_b.sort_values('Fecha_Ultimo_estado', ascending = False)
        base_consulta2_b_b_b = base_consulta2_b_b_b.drop_duplicates('NUMERO FACTURA', keep = 'first')
        
        base_consulta_final = pd.concat([base_consulta2_a,base_consulta2_b_a, base_consulta2_b_b_a, base_consulta2_b_b_b]).reset_index(drop = True)
        base_consulta_final = base_consulta_final[['NUMERO FACTURA','NIT','FECHA FACTURA','FECHA RADICACION',
                                'VALOR FACTURA','VALOR SALDO','Numero_OP','Fecha_OP','Valor_Bruto',
                                'Valor_Pagado_Egresos','IVA','Rete_Fuente','Rete_ICA','Rete_IVA',
                                'Valor_Glosa','Codigo_Glosa','Observacion','Estado_actual','Fecha_Ultimo_estado',
                                'Origen']]
        print(sisco_NIT)
        print(devoluciones_NIT)
        guardar_archivo = input('\n Desea guardar el archivo generado para el NIT ' + str(Numero_NIT) + ' ? ')
        
        if guardar_archivo.title() == 'Si':
            print('\n guardando la consulta en la ruta ', path_salida)
            base_consulta_final.to_excel(path_salida + '/' + str(Numero_NIT) + '.xlsx', index = False)
            #devoluciones_NIT.to_excel(path_salida + '/' + str(Numero_NIT) + '_d.xlsx', index = False)
            print('\n Consulta guardada exitosamente!')
            
            entrada = input('\n Desea generar una nueva consulta? ')
        else:        
            entrada = input('\n Desea generar una nueva consulta? ')
        
    else:
        print('\n El NIT ', str(Numero_NIT),' NO se encuentra en la base \n')
        entrada = input('Desea generar una nueva consulta? ')
    
        
print('\n Consulta finalizada!')    


            
            
        
    
