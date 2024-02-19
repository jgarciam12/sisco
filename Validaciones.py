# -*- coding: utf-8 -*-
"""
Created on Thu Jun  1 15:34:44 2023

@author: jcgarciam
"""

import pandas as pd
import zipfile
from datetime import datetime
import numpy as np
import csv
import pyttsx3
import sys

def ValidacionMaestro(Maestro, last_date, path_entrada1):
    
    print('Empezando a validar base de datos Maestro \n')

    def CambioFormato(df, a = 'a'):
        df[a] = df[a].astype(str).str.strip().str.strip('\x02').str.strip('')
        df[a] = np.where(df[a].str[-2::] == '.0', df[a].str[0:-2], df[a])
        df[a] = np.where(df[a] == 'nan', np.nan, df[a])
        return df[a]

    dic = {}
    
    print('Extrayendo Maestro',last_date)
    columnas = ['Numero_Factura','NIT','Razon_Social','Fecha_Radicacion','Valor_Bruto','Valor_Neto',
                'Valor_Glosa','Regimen','Cod_Barra','Fecha_Ultimo_estado','Valor_Descuento','Fecha_Auditoria',
                'Valor_Copago','Valor_Cuota_Moderadora','Valor_Iva','Fac_Norint','Estado_actual',
                'Tipo_Doc_Prestador','Fecha_Ingreso','Fecha_Factura','TipoCuentaMedica']
    
    with zipfile.ZipFile(path_entrada1 + '/' + last_date + '.zip', mode = 'r') as z:
        lista = z.namelist()
        for i in lista:
            with z.open(i) as f:
                print(i)
                df = pd.read_csv(f, sep=';', header=0, encoding='ANSI', engine='python', quoting=csv.QUOTE_NONE, index_col =  False, usecols = columnas)
                df['Archivo_Origen'] = i
                dic[i] = df
            
    Maestro_last_date = pd.concat(dic).reset_index(drop = True)
    print('Maestro',last_date,' extraído \n')
    
    print('------------------------------------------------------------------ \n')
    
    Maestro['Cod_Barra'] = CambioFormato(Maestro, a = 'Cod_Barra')
    Maestro = Maestro[Maestro['Cod_Barra'].isnull() == False]
    
    Maestro = Maestro.sort_values('Archivo_Origen', ascending = True)
    Maestro = Maestro.drop_duplicates('Cod_Barra', keep = 'last')
    print('En el Maestro actual hay ', '{:,.0f}'.format(Maestro.shape[0]), ' registros únicos')
    
    Maestro_last_date['Cod_Barra'] = CambioFormato(Maestro_last_date, a = 'Cod_Barra')
    Maestro_last_date = Maestro_last_date[Maestro_last_date['Cod_Barra'].isnull() == False]
    
    Maestro_last_date = Maestro_last_date.sort_values('Archivo_Origen', ascending = True)
    Maestro_last_date = Maestro_last_date.drop_duplicates('Cod_Barra', keep = 'last')
    print('En el Maestro anterior hay ', '{:,.0f}'.format(Maestro_last_date.shape[0]), ' registros únicos \n')
    
    print('Para una diferencia de: ','{:,.0f}'.format(Maestro.shape[0] - Maestro_last_date.shape[0]), ' registros \n')

    if Maestro.shape[0] - Maestro_last_date.shape[0] <= 0:
        engine = pyttsx3.init()
        engine.say('La cantidad de datos del actual Maestro NO es mayor a la anterior')
        engine.runAndWait()
        print('La cantidad de datos del actual Maestro NO es mayor a la anterior')
        print('Revisa la data del Maestro')

    print('------------------------------------------------------------------')
    
    a = Maestro_last_date[Maestro_last_date['Cod_Barra'].isin(Maestro['Cod_Barra']) ==  False]
    
    print('La cantidad de datos que habían en el anterior Maestro')
    print('y que hoy NO estan son: ',a.shape[0], '\n')
    print('Los regsitros se dividen en los estados: \n')
    print(a['Estado_actual'].value_counts())
    print('\n Los regsitros se dividen en los regimenes: \n')
    print(a['Regimen'].value_counts())
    
    print('\n Guardando registros faltantes')
    a.to_excel(r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas/Maestro_Registros_faltantes.xlsx', index = False)
    print('Registros faltantes guardados')
    
    
    Maestro['llave'] = Maestro['Cod_Barra'].astype(str) + Maestro['Valor_Neto'].astype(int).astype(str)
    Maestro_last_date['llave'] = Maestro_last_date['Cod_Barra'].astype(str) + Maestro_last_date['Valor_Neto'].astype(int).astype(str)
    
    b = Maestro_last_date[Maestro_last_date['llave'].isin(Maestro['llave']) ==  False]
    
    c = b[b['Cod_Barra'].isin(a['Cod_Barra']) == False]
    
    d = Maestro[['Cod_Barra','Valor_Neto']]
    d = d.rename(columns = {'Valor_Neto':'Valor_Neto_Anterior'})
    #%%
    c = c.merge(d, how = 'left', on = 'Cod_Barra')
    
    print('Cantidad de datos a los que le cambió el Valor Neto: ', c.shape[0], '\n')
    print('Las fechas de radicación son: \n')
    print(c['Fecha_Radicacion'].value_counts())
    
    print('Los regsitros se dividen en los estados: \n')
    print(c['Estado_actual'].value_counts())
    print('\n Los regsitros se dividen en los regimenes: \n')
    print(c['Regimen'].value_counts())
    
    print('\n Guardando registros faltantes de Maestro')
    c.to_excel(r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas/Maestro_Registros_Cond_Valor_Neto_Diferente.xlsx', index = False)
    print('Registros faltantes de Maestro guardados')
    
    return Maestro

def ValidacionPagosParciales(Pagos_Parciales, last_date, path_entrada1):
    
    print('Empezando a validar base de datos Pagos_Parciales \n')

    def CambioFormato(df, a = 'a'):
        df[a] = df[a].astype(str).str.strip().str.strip('\x02').str.strip('')
        df[a] = np.where(df[a].str[-2::] == '.0', df[a].str[0:-2], df[a])
        df[a] = np.where(df[a] == 'nan', np.nan, df[a])
        return df[a]

    dic = {}
    
    print('Extrayendo Pagos_Parciales',last_date)
    columnas = ['Numero_Factura_Original','NIT','Razon_Social','Fecha_Radicacion','Valor_Bruto','Valor_Neto',
                'Valor_Glosa','Regimen','Numero_Factura_Virtual','Cod_Barra','Fecha_Auditoria',
                'Cod_Barra_Pago_Parcial','Valor_Copago','Fac_Nroint','Tipo_Doc_Prestador',
                'Fecha_Ingreso','Fecha_Factura']
    
    with zipfile.ZipFile(path_entrada1 + '\Pagos_parciales_' + last_date + '.zip', mode = 'r') as z:
        lista = z.namelist()
        for i in lista:
            with z.open(i) as f:
                print(i)
                df = pd.read_csv(f, sep = ',', header = 0, usecols = columnas, encoding='latin-1', quoting=csv.QUOTE_NONE)
                df['Archivo_Origen'] = i
                dic[i] = df
    
    
    Pagos_Parciales_last_date = pd.concat(dic).reset_index(drop = True)
    
    print('Pagos_Parciales',last_date,' extraído \n')
    
    print('------------------------------------------------------------------ \n')
    
    Pagos_Parciales['Cod_Barra'] = CambioFormato(Pagos_Parciales, a = 'Cod_Barra')
    Pagos_Parciales['Cod_Barra_Pago_Parcial'] = CambioFormato(Pagos_Parciales, a = 'Cod_Barra_Pago_Parcial')

    Pagos_Parciales = Pagos_Parciales[Pagos_Parciales['Cod_Barra'].isnull() == False]
    Pagos_Parciales = Pagos_Parciales[Pagos_Parciales['Cod_Barra_Pago_Parcial'].isnull() == False]  
    
    Pagos_Parciales['Valor_Glosa_tem'] = Pagos_Parciales['Valor_Glosa'].copy()
    Pagos_Parciales['Valor_Glosa_tem'] = -Pagos_Parciales['Valor_Glosa_tem']
    
    Pagos_Parciales = Pagos_Parciales.sort_values(['Archivo_Origen','Valor_Glosa_tem'], ascending = True)
    Pagos_Parciales['Llave'] = Pagos_Parciales['Cod_Barra'].astype(str) + '|' + Pagos_Parciales['Cod_Barra_Pago_Parcial'].astype(str)
    Pagos_Parciales = Pagos_Parciales.drop_duplicates('Llave', keep = 'last')
    print('En Pagos_Parciales actual hay ', '{:,.0f}'.format(Pagos_Parciales.shape[0]), ' registros únicos')
    
    Pagos_Parciales_last_date['Cod_Barra'] = CambioFormato(Pagos_Parciales_last_date, a = 'Cod_Barra')
    Pagos_Parciales_last_date['Cod_Barra_Pago_Parcial'] = CambioFormato(Pagos_Parciales_last_date, a = 'Cod_Barra_Pago_Parcial')

    Pagos_Parciales_last_date = Pagos_Parciales_last_date[Pagos_Parciales_last_date['Cod_Barra'].isnull() == False]
    Pagos_Parciales_last_date = Pagos_Parciales_last_date[Pagos_Parciales_last_date['Cod_Barra_Pago_Parcial'].isnull() == False]    
    
    Pagos_Parciales_last_date['Valor_Glosa_tem'] = Pagos_Parciales_last_date['Valor_Glosa'].copy()
    Pagos_Parciales_last_date['Valor_Glosa_tem'] = -Pagos_Parciales_last_date['Valor_Glosa_tem']
    
    Pagos_Parciales_last_date = Pagos_Parciales_last_date.sort_values(['Archivo_Origen','Valor_Glosa_tem'], ascending = True)    
    Pagos_Parciales_last_date['Llave'] = Pagos_Parciales_last_date['Cod_Barra'].astype(str) + '|' + Pagos_Parciales_last_date['Cod_Barra_Pago_Parcial'].astype(str)
    Pagos_Parciales_last_date = Pagos_Parciales_last_date.drop_duplicates('Llave', keep = 'last')
    print('En Pagos_Parciales anterior hay ', '{:,.0f}'.format(Pagos_Parciales_last_date.shape[0]), ' registros únicos \n')
    
    print('Para una diferencia de: ','{:,.0f}'.format(Pagos_Parciales.shape[0] - Pagos_Parciales_last_date.shape[0]), ' registros \n')

    if Pagos_Parciales.shape[0] - Pagos_Parciales_last_date.shape[0] <= 0:
        engine = pyttsx3.init()
        engine.say('La cantidad de datos del actual Pagos Parciales NO es mayor a la anterior')
        engine.runAndWait()
        print('La cantidad de datos del actual Pagos_Parciales NO es mayor a la anterior')
        print('Revisa la data de Pagos_Parciales')

    print('------------------------------------------------------------------')
    
    a = Pagos_Parciales_last_date[Pagos_Parciales_last_date['Llave'].isin(Pagos_Parciales['Llave']) ==  False]
    
    print('La cantidad de datos que habían en el anterior Pagos_Parciales')
    print('y que hoy NO estan son: ',a.shape[0], '\n')
    print('\n Los regsitros se dividen en los regimenes: \n')
    print(a['Regimen'].value_counts())
    
    print('\n Guardando registros faltantesde Pagos_Parciales')
    a.to_excel(r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas/Pagos_Parciales_Registros_faltantes.xlsx', index = False)
    print('Registros faltantes de Pagos_Parciales guardados')
    
    return Pagos_Parciales





