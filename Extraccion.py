# -*- coding: utf-8 -*-
"""
Created on Tue Jul 26 11:47:28 2022

@author: jcgarciam
"""

import pandas as pd
import zipfile
import os
import glob
import csv

# Funcion para extraer los archivos de Pagos Parciales que se encuentran en un archivo zip y se leen todos
# y se concatenan en una sola tabla.
def ExtraccionPagosParciales(path1, Current_Date):
    #Extraccion de la base actual de pagos parciales
    dic = {}
    
    columnas = ['Numero_Factura_Original','NIT','Razon_Social','Fecha_Radicacion','Valor_Bruto','Valor_Neto',
                'Valor_Glosa','Regimen','Numero_Factura_Virtual','Cod_Barra','Valor_Descuento',
                'Fecha_Auditoria','Cod_Barra_Pago_Parcial','Departamento','Municipio','Zona',
                'Usuario_Inicia_Radicacion','Usuario_Finaliza_Radicacion','Valor_Copago','Valor_Cuota_Moderadora',
                'Valor_Iva','Fac_Nroint','Tipo_Doc_Prestador','Fecha_Ingreso','Fecha_Factura',
                'Descuento_Pronto_Pago']
    
    with zipfile.ZipFile(path1 + '\Pagos_parciales_'+Current_Date+'.zip', mode = 'r') as z:
        lista = z.namelist()
        for i in lista:
            with z.open(i) as f:
                print(i)
                df = pd.read_csv(f, sep = ',', usecols = columnas, header = 0,
                                 encoding = 'latin-1', quoting = csv.QUOTE_NONE,
                                 dtype = {'Cod_Barra_Pago_Parcial':str,'Cod_Barra':str,'NIT':str})
                df['Archivo_Origen'] = i
                dic[i] = df
    
    
    Pagos_Parciales = pd.concat(dic).reset_index(drop = True)
    Pagos_Parciales = Pagos_Parciales.drop_duplicates('Cod_Barra_Pago_Parcial', keep = 'last')
    
    return Pagos_Parciales

# Funcion para extaer los archivo de Maestro que se encuentran en un archivo zip
def ExtraccionMaestro(path1, Current_Date):
    #Extraccion de la base actual de Maestro
    dic = {}
    
    columnas = ['Numero_Factura','NIT','Razon_Social','Fecha_Radicacion','Valor_Bruto','Valor_Neto',
                'Valor_Glosa','Regimen','Cod_Barra','Fecha_Ultimo_estado','Valor_Descuento','Fecha_Auditoria',
                'Departamento','Municipio','Zona','Usuario_Inicia_Radicacion','Usuario_Finaliza_Radicacion',
                'Valor_Copago','Valor_Cuota_Moderadora','Valor_Iva','Fac_Norint','Estado_actual',
                'Tipo_Doc_Prestador','Fecha_Ingreso','Fecha_Factura','TipoCuentaMedica','Descuento_Pronto_Pago',
                'Fecha_Comentario','Auditor']
    
    with zipfile.ZipFile(path1 + '/'+Current_Date+'.zip', mode = 'r') as z:
        lista = z.namelist()
        for i in lista:
            with z.open(i) as f:
                print(i)
                df = pd.read_csv(f, sep=';',header=0, usecols = columnas,
                                 encoding='ANSI', engine='python',
                                 quoting=csv.QUOTE_NONE,index_col =  False,
                                 dtype = {'NIT':str})
                df['Archivo_Origen'] = i
                dic[i] = df
            
    Maestro = pd.concat(dic).reset_index(drop = True)
    Maestro = Maestro.drop_duplicates('Cod_Barra', keep = 'last')
    return Maestro

    
def ExtraccionPagos(path2):
    #Extraccion de la base Pagos
    dic = {}
    
    files = os.listdir(path2)
    
    for file in files:
        if file != 'ASISTENCIALES.txt':
            print(file)
            df = pd.read_csv(path2 + "/" + file, sep = '\t', header=0,
                             error_bad_lines = False)
            df['Origen pagos'] = file
            dic[file] = df  
    
    Pagos = pd.concat(dic).reset_index(drop = True)
    
    return Pagos

    
def ExtraccionAistenciales(path2):
    #Extraccion de la base Asistenciales
    Asistenciales = pd.read_csv(path2 + "\ASISTENCIALES.txt", sep = '\t',
                                header=0, error_bad_lines = False,
                                dtype = {'RADICADO':str,'EGRESOSDC':str,
                                         'RAAEST':str,'F_LOTE':str,
                                         'DELNRE':str,'DELSRE':str})
    return Asistenciales
    
    
def ExtraccionGlosas(path3, Current_Date):
    #Extraccion de la base glosas
    dic = {}
    columnas = ['Numero_Interno','Auditor_conciliación','Fecha_Envio','Nro_Guia','Tipo_Cuenta_Medica','Codigo_Glosa','Observacion','Nota_crédito','Nombre_Estado_Actual','Fecha_Ultimo_estado','Valor_Glosa_Detalle']
    
    for file in glob.glob(path3 + '/' + Current_Date + '*Control Calidad Informe Mensual Glosas (AXA Colpatria *'):
        print('Leyendo el archivo: ',file[len(path3) + 1::])
        df = pd.read_excel(file, header=0, usecols = columnas)   
        
        if 'ARL' in file.upper():
            df['Origen_Glosa'] = 'ARL'
        else:
            df['Origen_Glosa'] = 'Salud'
            
        dic[file] = df
        print('El archivo: ',file[len(path3) + 1::], ' leido\n')
    Glosas = pd.concat(dic).reset_index(drop = True)
    
    return Glosas


def ExtraccionCuentasMedicas(path4):
    #Extraccion de la base RR_Cuentas_medicas
    
    #RR_Cuentas_Medicas = pd.read_csv(path4 + "\ReporteRadicacionRapida.txt", sep=',', header=0, usecols = ['FECHA INGRESO', 'ESTADO','FOLIO'], encoding='ANSI')
    datos = []
    with open(path4 + '/ReporteRadicacionRapida.txt', 'r') as archivo:
        for linea in archivo:
            datos_linea = linea.strip().split(',')
            datos.append(datos_linea)
         
    RR_Cuentas_Medicas  = pd.DataFrame(datos)
    
    return RR_Cuentas_Medicas


def ExtraccionRadicadosARL(path5):
    #Extraccion de la base  RPT_RADICADOS_ARL
    
    dic = {}

    with zipfile.ZipFile(path5 + '\RPT_RADICADOS_ARL2017-2020.zip', mode = 'r') as z:
        lista = z.namelist()
        for i in lista:
            with z.open(i) as f:
                print(i)
                df = pd.read_csv(f, sep = ',', header = 0,
                                 usecols = ['CODIGO_BARRAS','RADICADO'],
                                 quoting=csv.QUOTE_NONE, index_col = False,
                                 dtype = {'CODIGO_BARRAS':str,
                                          'RADICADO':str})
                dic[i] = df

        for ArchivoZip in glob.glob(path5 + '\*zip'):
            if ArchivoZip != path5 + '\RPT_RADICADOS_ARL2017-2020.zip':
                with zipfile.ZipFile(ArchivoZip, mode = 'r') as z:
                    lista = z.namelist()
                    for i in lista:
                        with z.open(i) as f:
                            print(i)
                            df = pd.read_csv(f, sep = '|', header = 0,
                                             usecols = ['CODIGO_BARRAS','RADICADO'],
                                             quoting=csv.QUOTE_NONE, index_col = False,
                                             dtype = {'CODIGO_BARRAS':str,
                                                      'RADICADO':str})
                            dic[i] = df
    
    RPT_RADICADOS_ARL = pd.concat(dic).reset_index(drop = True)
    return RPT_RADICADOS_ARL

def devoluciones_entrada(path6):
    i = glob.glob(path6 + '\Devoluciones Gestión Documental*.xlsx')[0]
    print('El archivo a extraer de Gestion Documental es: ',i[len(path6) + 1::])
    xls = pd.ExcelFile(i)
    sheets = xls.sheet_names

    dic = {}
    
    columnas2 = ['REGIMEN','NIT_IPS','RAZON_SOCIAL','NUMERO_FACTURA','CODIGO_BARRA','FECHA_LOTE_RADICADO',
                 'Fecha de envio','RUBRO','Numero de guia','ESTADO_ANTERIOR']

    for i in sheets:
        if (('2019' in i) | ('HIstorico dev hasta 2020' in i) | ('Devoluciones 2021 antiguo' in i)):
            print(i)
            df = pd.read_excel(glob.glob(path6 + '\Devoluciones Gestión Documental*.xlsx')[0], header = 0, sheet_name =  i, usecols  = columnas2)
            df = df.rename(columns = {'NIT_IPS':'NIT','RAZON_SOCIAL':'RAZON SOCIAL','ESTADO_ANTERIOR':'ESTADO',
                                'NUMERO_FACTURA':'NUMERO DE FACTURA','FECHA_LOTE_RADICADO':'FECHA DE RADICADO',
                                'Fecha de envio':'FECHA DE DEVOLUCION','RUBRO':'RUBRO-CAUSAL',
                                'Numero de guia':'NUMERO DE ACTA DEVOLUCION'})
            dic[i] = df
    
        elif ('DEv. entada 2020' not in i):
            print(i)
            df = pd.read_excel(glob.glob(path6 + '\Devoluciones Gestión Documental*.xlsx')[0], header = 0, sheet_name =  i)
            df = df.rename(columns = {'NIT_IPS':'NIT','IPS':'RAZON SOCIAL','Unidad_Negocio':'REGIMEN','Regimen':'REGIMEN',
                            'Factura':'NUMERO DE FACTURA','Codigo_barra':'CODIGO_BARRA','Fecha_Radicado':'FECHA DE RADICADO',
                            'Fecha de envio':'FECHA DE DEVOLUCION','RUBRO':'RUBRO-CAUSAL',
                            'Generacion_Numero_Acta':'NUMERO DE ACTA DEVOLUCION','Ultimo_Estado':'ESTADO'})
            df = df[['NIT','RAZON SOCIAL','REGIMEN','NUMERO DE FACTURA','CODIGO_BARRA','FECHA DE RADICADO',
                     'FECHA DE DEVOLUCION','RUBRO-CAUSAL','NUMERO DE ACTA DEVOLUCION','ESTADO','Zona']]
            
            dic[i] = df
    
    devoluciones_entrada = pd.concat(dic).reset_index(drop = True)
    
    return devoluciones_entrada

def ExtraccionConciliaciones(path3, Current_Date):
    #Extraccion de la base glosas
    dic = {}
    columnas = ['Codigo_barras','Numero_conciliacion']
    
    for i in glob.glob(path3 + '/' + Current_Date + '*Control pendiente proceso respuesta glosa (AXA Colpatria*'):
        print('Leyendo el archivo: ', i[len(path3) + 1::])
        df = pd.read_excel(i, usecols = columnas)
        df['Origen'] = i[len(path3) + 1::]
        dic[i] = df
        print('Archivo: ', i[len(path3) + 1::], ' leído\n')        
        
    conciliaciones = pd.concat(dic).reset_index(drop = True)
    
    return conciliaciones